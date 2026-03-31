"""NSI download: USACE API streaming or HuggingFace pre-processed Parquet."""

from __future__ import annotations

import io
import json
import re
import time
import warnings
from http.client import IncompleteRead
from pathlib import Path
from urllib import request as urllib_request

import pandas as pd

try:
    from tqdm.auto import tqdm
except ImportError:  # pragma: no cover
    import subprocess
    import sys

    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "tqdm"])
    from tqdm.auto import tqdm

try:
    from .us_states import API_BASE as _API_BASE
    from .us_states import STATE_BY_NAME as _STATE_BY_NAME
    from .us_states import STATE_FIPS as _STATE_FIPS
except ImportError:  # pragma: no cover
    from us_states import API_BASE as _API_BASE
    from us_states import STATE_BY_NAME as _STATE_BY_NAME
    from us_states import STATE_FIPS as _STATE_FIPS


class NSIDownloader:
    """NSI download client supporting USACE API and HuggingFace backends."""

    API_BASE = _API_BASE
    LARGE_STATE_FIPS = frozenset({"06", "12", "48"})  # CA, FL, TX — too large for single API call
    STATE_BY_NAME = _STATE_BY_NAME
    STATE_FIPS = _STATE_FIPS

    KEEP_COLS = [
        "bid",
        "occtype",
        "val_struct",
        "sqft",
        "num_story",
        "found_type",
        "found_ht",
        "val_cont",
        "cbfips",
        "pop2pmu65",
        "pop2pmo65",
    ]

    class _CountingHTTPReader:
        """Wrap http.client response to count raw bytes read (for MB/s throughput)."""

        __slots__ = ("_raw", "bytes_read")

        def __init__(self, raw):
            self._raw = raw
            self.bytes_read = 0

        def read(self, n=-1):
            b = self._raw.read(n)
            if b:
                self.bytes_read += len(b)
            return b

        def read1(self, n=-1):
            # TextIOWrapper wraps us in BufferedReader, which fills its buffer via read1()
            # on the raw stream — __getattr__ would forward read1 to HTTPResponse and skip counting.
            b = self._raw.read1(n)
            if b:
                self.bytes_read += len(b)
            return b

        def __getattr__(self, name):
            return getattr(self._raw, name)

    def __init__(self, work_dir: Path | str):
        self.work_dir = Path(work_dir)

    @classmethod
    def _normalize_state_name(cls, state_name: str) -> str | None:
        """Return canonical state name (title-case) or None if not found."""
        key = re.sub(r"\s+", " ", state_name.replace("-", " ").strip()).lower()
        spec = cls.STATE_BY_NAME.get(key)
        if spec:
            return spec.name
        warnings.warn(f"No FIPS for {state_name!r}, skipping")
        return None

    @staticmethod
    def _normalize_cbfips(df: pd.DataFrame) -> pd.DataFrame:
        """Standardize cbfips to zero-padded 15-digit strings, preserving missing values."""
        if "cbfips" not in df.columns:
            return df

        cb = df["cbfips"].astype("string")
        cb = cb.str.replace(r"[^0-9]", "", regex=True)
        cb = cb.fillna("")
        cb = cb.str.zfill(15)
        cb = cb.mask(cb.str.strip() == "")
        df = df.copy()
        df["cbfips"] = cb
        return df

    def stream_features(self, url: str, timeout: int = 600, retries: int = 3) -> tuple[list[dict], int]:
        """Download NSI features using fmt=fs (feature stream) with line-by-line JSON parsing."""
        rows = []
        for attempt in range(retries):
            try:
                req = urllib_request.Request(url, headers={"Accept": "application/json"})
                with urllib_request.urlopen(req, timeout=timeout) as resp:
                    counter = self._CountingHTTPReader(resp)
                    stream = io.TextIOWrapper(counter, encoding="utf-8")
                    for line_number, raw_line in enumerate(stream, start=1):
                        line = raw_line.strip()
                        if not line:
                            continue
                        try:
                            feat = json.loads(line)
                        except json.JSONDecodeError as exc:
                            raise json.JSONDecodeError(
                                f"Malformed NSI feature stream line {line_number}", line, exc.pos
                            ) from exc
                        if not isinstance(feat, dict) or feat.get("type") != "Feature":
                            continue
                        props = feat.get("properties", {})
                        geom = feat.get("geometry", {})
                        row = {k: props.get(k) for k in self.KEEP_COLS if k in props}
                        if geom.get("type") == "Point":
                            coords = geom["coordinates"]
                            row["longitude"] = coords[0]
                            row["latitude"] = coords[1]
                        else:
                            row["longitude"] = None
                            row["latitude"] = None
                        rows.append(row)
                return rows, counter.bytes_read
            except (OSError, IncompleteRead, json.JSONDecodeError) as e:
                print(f"    Attempt {attempt + 1} failed ({type(e).__name__}): {e}")
                if attempt < retries - 1:
                    time.sleep(5 * (attempt + 1))
                    rows.clear()
                else:
                    raise

    def county_fips(self, state_fips: str) -> tuple:
        """Get 5-digit county FIPS codes for a state using pygris."""
        import pygris

        counties_gdf = pygris.counties(state=state_fips, year=2022)
        return counties_gdf["GEOID"].tolist(), counties_gdf

    def download_state(self, state_name: str, raster_bbox_polygon=None) -> tuple[pd.DataFrame, int]:
        """Download NSI for one state. Large states use county-by-county download.

        Returns (dataframe, bytes_downloaded) where bytes_downloaded is HTTP payload bytes (0 if cached).
        """
        normalized_state = self._normalize_state_name(state_name)
        if not normalized_state:
            return pd.DataFrame(), 0

        fips = self.STATE_FIPS.get(normalized_state)
        if not fips:
            warnings.warn(f"No FIPS for {state_name!r} (normalized={normalized_state!r}), skipping")
            return pd.DataFrame(), 0

        cache_path = self.work_dir / f"nsi_{normalized_state.replace(' ', '_').lower()}.parquet"
        cache_meta_path = cache_path.with_suffix(".meta.json")
        use_cache = cache_path.exists()
        if use_cache and fips in self.LARGE_STATE_FIPS:
            if not cache_meta_path.exists():
                print(
                    f"  {normalized_state}: ignoring legacy large-state cache without completeness metadata ({cache_path.name})"
                )
                use_cache = False
            else:
                try:
                    cache_meta = json.loads(cache_meta_path.read_text())
                except (OSError, json.JSONDecodeError) as e:
                    print(f"  {normalized_state}: ignoring unreadable cache metadata {cache_meta_path.name}: {e}")
                    use_cache = False
                else:
                    if not cache_meta.get("complete", False):
                        print(f"  {normalized_state}: ignoring incomplete large-state cache {cache_path.name}")
                        use_cache = False
        if use_cache:
            print(f"  {normalized_state}: loading cached {cache_path.name}")
            return pd.read_parquet(cache_path), 0

        should_cache = True
        failed_counties = []
        bytes_downloaded = 0
        if fips in self.LARGE_STATE_FIPS:
            county_fips_list, counties_gdf = self.county_fips(fips)
            print(f"  {normalized_state} (FIPS={fips}): downloading {len(county_fips_list)} counties...")
            all_rows = []
            t0 = time.time()
            total_bytes = 0

            with tqdm(total=len(county_fips_list), desc=f"{normalized_state} counties", unit="county") as pbar:
                for cfips in county_fips_list:
                    url = f"{self.API_BASE}?fips={cfips}&fmt=fs"
                    try:
                        county_rows, nbytes = self.stream_features(url)
                        all_rows.extend(county_rows)
                        total_bytes += nbytes
                    except Exception as e:
                        failed_counties.append((cfips, str(e)))
                        pbar.write(f"    County {cfips} FAILED: {type(e).__name__}: {e}")
                    finally:
                        elapsed = max(time.time() - t0, 1e-9)
                        mb_s = total_bytes / elapsed / 1024 / 1024
                        pbar.set_postfix_str(f"{mb_s:.2f} MB/s")
                        pbar.update(1)

            if failed_counties and raster_bbox_polygon is not None:
                failed_geoids = {c[0] for c in failed_counties}
                failed_gdf = counties_gdf[counties_gdf["GEOID"].isin(failed_geoids)]
                intersecting = failed_gdf[failed_gdf.geometry.intersects(raster_bbox_polygon)]
                if not intersecting.empty:
                    names = intersecting["GEOID"].tolist()
                    raise RuntimeError(
                        f"{normalized_state}: {len(intersecting)} failed counties intersect raster footprint: {names}. "
                        f"Cannot produce reliable results."
                    )
                else:
                    warnings.warn(
                        f"{normalized_state}: {len(failed_counties)} counties failed but none intersect raster — continuing"
                    )
            elif failed_counties:
                warnings.warn(
                    f"{normalized_state}: {len(failed_counties)} counties failed (no raster bbox to check): "
                    f"{[c[0] for c in failed_counties]}"
                )
            if failed_counties:
                should_cache = False

            if not all_rows:
                raise RuntimeError(f"{normalized_state}: all county downloads failed, 0 buildings retrieved")

            df = pd.DataFrame(all_rows)
            bytes_downloaded = total_bytes
        else:
            url = f"{self.API_BASE}?fips={fips}&fmt=fs"
            print(f"  {normalized_state} (FIPS={fips}): downloading from NSI API (streaming)...")
            t0 = time.time()
            with tqdm(total=1, desc=f"{normalized_state} NSI", unit="state") as pbar:
                rows, nbytes = self.stream_features(url)
                elapsed = max(time.time() - t0, 1e-9)
                pbar.update(1)
                mb_s = nbytes / elapsed / 1024 / 1024
                pbar.set_postfix_str(f"{mb_s:.2f} MB/s")
            df = pd.DataFrame(rows)
            bytes_downloaded = nbytes

        df = self._normalize_cbfips(df)
        print(f"    Downloaded {len(df):,} buildings for {normalized_state}")
        if should_cache:
            df.to_parquet(cache_path, index=False)
            if fips in self.LARGE_STATE_FIPS:
                cache_meta_path.write_text(json.dumps({"complete": True, "state_fips": fips}))
        else:
            for stale_path in (cache_path, cache_meta_path):
                if stale_path.exists():
                    stale_path.unlink()
            print(f"    Skipping cache for {state_name} because {len(failed_counties)} counties failed during download")
        return df, bytes_downloaded

    def download_states(self, state_names: list, raster_bbox_polygon=None) -> pd.DataFrame:
        """Download NSI for multiple states, show tqdm on states, return concatenated buildings."""
        normalized_states = []
        seen_states = set()
        for s in state_names:
            normalized = self._normalize_state_name(s)
            if normalized and normalized not in seen_states:
                seen_states.add(normalized)
                normalized_states.append(normalized)

        if not normalized_states:
            raise RuntimeError("No valid states to download after normalization.")

        print(f"Downloading NSI for {len(normalized_states)} states...")
        nsi_dfs = []
        failed_states = []
        t_dl0 = time.time()
        total_dl_bytes = 0

        with tqdm(total=len(normalized_states), desc="Downloading NSI states", unit="state") as pbar:
            for state in normalized_states:
                try:
                    df, nbytes = self.download_state(state, raster_bbox_polygon=raster_bbox_polygon)
                    total_dl_bytes += nbytes
                    if not df.empty:
                        nsi_dfs.append(df)
                except Exception as e:
                    failed_states.append((state, str(e)))
                    pbar.write(f"  FAILED: {state} — {type(e).__name__}: {e}")
                finally:
                    elapsed = max(time.time() - t_dl0, 1e-9)
                    mb_s = total_dl_bytes / elapsed / 1024 / 1024
                    pbar.set_postfix_str(f"{mb_s:.2f} MB/s")
                    pbar.update(1)

        if failed_states:
            warnings.warn(f"NSI download failed for {len(failed_states)} states: {[s for s, _ in failed_states]}")

        if not nsi_dfs:
            raise RuntimeError("No NSI data downloaded. Cannot proceed.")

        return pd.concat(nsi_dfs, ignore_index=True)

    REQUIRED_HF_COLS = frozenset(
        {"cbfips", "bid", "longitude", "latitude"}
    )

    @staticmethod
    def _hf_partition_key(state_name: str) -> str:
        """Convert state name to HF partition directory key.

        HF dataset uses ``state=Florida/``, ``state=New_York/``,
        ``state=District_Of_Columbia/`` (spaces → ``_``, title-case
        with each word capitalised).
        """
        return state_name.strip().replace(" ", "_").title()

    def download_states_hf(
        self,
        state_names: list,
        repo_id: str = "Alexq847182/NSI_Parquet",
        token: str | None = None,
    ) -> pd.DataFrame:
        """Download NSI from a HuggingFace dataset repo.

        The HF dataset is partitioned by state
        (``state=Florida/part-00000.snappy.parquet``).  Only files
        for the requested ``state_names`` are downloaded — no full
        dataset download is needed.
        """
        from huggingface_hub import HfApi, hf_hub_download

        keep_cols = self.KEEP_COLS + ["longitude", "latitude"]

        normalized_states = []
        seen_states = set()
        for s in state_names:
            normalized = self._normalize_state_name(s)
            if normalized and normalized not in seen_states:
                seen_states.add(normalized)
                normalized_states.append(normalized)

        if not normalized_states:
            raise RuntimeError("No valid states to download after normalization.")

        # Build mapping: HF partition key → state name
        api = HfApi()
        repo_files = api.list_repo_files(
            repo_id, repo_type="dataset", token=token
        )
        parquet_files = [
            f for f in repo_files if f.endswith(".parquet")
        ]
        if not parquet_files:
            raise FileNotFoundError(
                f"No parquet files in HF dataset {repo_id}"
            )

        # Index available partitions (lower-case key → filename)
        available: dict[str, list[str]] = {}
        for pf in parquet_files:
            # e.g. "state=Florida/part-00000.snappy.parquet"
            parts = pf.split("/")
            if parts[0].startswith("state="):
                key = parts[0].split("=", 1)[1].lower()
                available.setdefault(key, []).append(pf)

        # Match requested states to partitions
        files_to_download: list[str] = []
        matched_states: list[str] = []
        for s in normalized_states:
            key = self._hf_partition_key(s).lower()
            if key in available:
                files_to_download.extend(available[key])
                matched_states.append(s)
            else:
                warnings.warn(
                    f"No HF partition for {s!r} "
                    f"(tried key={key!r}), skipping"
                )

        if not files_to_download:
            raise FileNotFoundError(
                f"No HF partitions matched for states "
                f"{state_names}. Available partitions: "
                f"{sorted(available.keys())}"
            )

        print(
            f"Downloading {len(files_to_download)} file(s) for "
            f"{len(matched_states)} state(s): {matched_states}"
        )

        hf_cache_dir = self.work_dir / "hf_nsi_cache"
        hf_cache_dir.mkdir(parents=True, exist_ok=True)

        nsi_dfs: list[pd.DataFrame] = []
        t0 = time.time()
        total_bytes = 0

        with tqdm(
            total=len(files_to_download),
            desc="Downloading NSI from HuggingFace",
            unit="file",
        ) as pbar:
            for pf in files_to_download:
                local_path = hf_hub_download(
                    repo_id=repo_id,
                    filename=pf,
                    repo_type="dataset",
                    token=token,
                    cache_dir=str(hf_cache_dir),
                )
                file_bytes = Path(local_path).stat().st_size
                total_bytes += file_bytes

                df_part = pd.read_parquet(local_path)

                # Fail fast if required columns are missing
                missing = self.REQUIRED_HF_COLS - set(
                    df_part.columns
                )
                if missing:
                    raise ValueError(
                        f"HF parquet {pf!r} missing required "
                        f"columns: {sorted(missing)}. "
                        f"Available: {sorted(df_part.columns)}"
                    )

                available_cols = [
                    c for c in keep_cols if c in df_part.columns
                ]
                df_part = df_part[available_cols]

                if not df_part.empty:
                    nsi_dfs.append(df_part)

                elapsed = max(time.time() - t0, 1e-9)
                mb_s = total_bytes / elapsed / 1024 / 1024
                pbar.set_postfix_str(f"{mb_s:.2f} MB/s")
                pbar.update(1)

        if not nsi_dfs:
            raise RuntimeError(
                f"No NSI buildings found for states "
                f"{normalized_states} in HF dataset {repo_id}"
            )

        result = pd.concat(nsi_dfs, ignore_index=True)
        result = self._normalize_cbfips(result)
        print(f"Total: {len(result):,} buildings")
        return result
