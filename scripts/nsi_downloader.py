"""NSI download: USACE API streaming or HuggingFace pre-processed Parquet."""

from __future__ import annotations

import io
import json
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
    from .us_states import STATE_FIPS as _STATE_FIPS
except ImportError:  # pragma: no cover
    from us_states import API_BASE as _API_BASE
    from us_states import STATE_FIPS as _STATE_FIPS


class NSIDownloader:
    """NSI download client supporting USACE API and HuggingFace backends."""

    API_BASE = _API_BASE
    LARGE_STATE_FIPS = frozenset({"06", "12", "48"})  # CA, FL, TX — too large for single API call
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
        fips = self.STATE_FIPS.get(state_name)
        if not fips:
            warnings.warn(f"No FIPS for {state_name!r}, skipping")
            return pd.DataFrame(), 0

        cache_path = self.work_dir / f"nsi_{state_name.replace(' ', '_').lower()}.parquet"
        cache_meta_path = cache_path.with_suffix(".meta.json")
        use_cache = cache_path.exists()
        if use_cache and fips in self.LARGE_STATE_FIPS:
            if not cache_meta_path.exists():
                print(
                    f"  {state_name}: ignoring legacy large-state cache without completeness metadata ({cache_path.name})"
                )
                use_cache = False
            else:
                try:
                    cache_meta = json.loads(cache_meta_path.read_text())
                except (OSError, json.JSONDecodeError) as e:
                    print(f"  {state_name}: ignoring unreadable cache metadata {cache_meta_path.name}: {e}")
                    use_cache = False
                else:
                    if not cache_meta.get("complete", False):
                        print(f"  {state_name}: ignoring incomplete large-state cache {cache_path.name}")
                        use_cache = False
        if use_cache:
            print(f"  {state_name}: loading cached {cache_path.name}")
            return pd.read_parquet(cache_path), 0

        should_cache = True
        failed_counties = []
        bytes_downloaded = 0
        if fips in self.LARGE_STATE_FIPS:
            county_fips_list, counties_gdf = self.county_fips(fips)
            print(f"  {state_name} (FIPS={fips}): downloading {len(county_fips_list)} counties...")
            all_rows = []
            t0 = time.time()
            total_bytes = 0

            with tqdm(total=len(county_fips_list), desc=f"{state_name} counties", unit="county") as pbar:
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
                        f"{state_name}: {len(intersecting)} failed counties intersect raster footprint: {names}. "
                        f"Cannot produce reliable results."
                    )
                else:
                    warnings.warn(
                        f"{state_name}: {len(failed_counties)} counties failed but none intersect raster — continuing"
                    )
            elif failed_counties:
                warnings.warn(
                    f"{state_name}: {len(failed_counties)} counties failed (no raster bbox to check): "
                    f"{[c[0] for c in failed_counties]}"
                )
            if failed_counties:
                should_cache = False

            if not all_rows:
                raise RuntimeError(f"{state_name}: all county downloads failed, 0 buildings retrieved")

            df = pd.DataFrame(all_rows)
            bytes_downloaded = total_bytes
        else:
            url = f"{self.API_BASE}?fips={fips}&fmt=fs"
            print(f"  {state_name} (FIPS={fips}): downloading from NSI API (streaming)...")
            t0 = time.time()
            with tqdm(total=1, desc=f"{state_name} NSI", unit="state") as pbar:
                rows, nbytes = self.stream_features(url)
                elapsed = max(time.time() - t0, 1e-9)
                pbar.update(1)
                mb_s = nbytes / elapsed / 1024 / 1024
                pbar.set_postfix_str(f"{mb_s:.2f} MB/s")
            df = pd.DataFrame(rows)
            bytes_downloaded = nbytes

        print(f"    Downloaded {len(df):,} buildings for {state_name}")
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
        print(f"Downloading NSI for {len(state_names)} states...")
        nsi_dfs = []
        failed_states = []
        t_dl0 = time.time()
        total_dl_bytes = 0

        with tqdm(total=len(state_names), desc="Downloading NSI states", unit="state") as pbar:
            for state in state_names:
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

    def download_states_hf(
        self,
        state_names: list,
        repo_id: str = "Alexq847182/NSI_Parquet",
        token: str | None = None,
    ) -> pd.DataFrame:
        """Download NSI from a HuggingFace dataset repo, filtering per-file to affected states.

        Each parquet file is filtered to the requested states immediately after reading,
        keeping peak memory low.
        """
        from huggingface_hub import HfApi, hf_hub_download

        keep_cols = self.KEEP_COLS + ["longitude", "latitude"]
        affected_fips = {self.STATE_FIPS[s] for s in state_names if s in self.STATE_FIPS}
        if not affected_fips:
            raise ValueError(f"No valid state FIPS found for: {state_names}")

        api = HfApi()
        repo_files = api.list_repo_files(repo_id, repo_type="dataset", token=token)
        parquet_files = [f for f in repo_files if f.endswith(".parquet")]
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in HF dataset {repo_id}")
        print(f"Found {len(parquet_files)} parquet file(s) in {repo_id}")

        hf_cache_dir = self.work_dir / "hf_nsi_cache"
        hf_cache_dir.mkdir(parents=True, exist_ok=True)

        nsi_dfs: list[pd.DataFrame] = []
        t0 = time.time()
        total_bytes = 0

        with tqdm(total=len(parquet_files), desc="Downloading NSI from HuggingFace", unit="file") as pbar:
            for pf in parquet_files:
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
                available_cols = [c for c in keep_cols if c in df_part.columns]
                df_part = df_part[available_cols]

                # Filter to affected states immediately to reduce memory
                if "cbfips" in df_part.columns:
                    df_part = df_part[
                        df_part["cbfips"].astype(str).str[:2].isin(affected_fips)
                    ]

                if not df_part.empty:
                    nsi_dfs.append(df_part)

                elapsed = max(time.time() - t0, 1e-9)
                mb_s = total_bytes / elapsed / 1024 / 1024
                pbar.set_postfix_str(f"{mb_s:.2f} MB/s")
                pbar.update(1)

        if not nsi_dfs:
            raise RuntimeError(
                f"No NSI buildings found for states {state_names} in HF dataset {repo_id}"
            )

        result = pd.concat(nsi_dfs, ignore_index=True)
        print(f"Filtered to {len(affected_fips)} state(s): {affected_fips} — {len(result):,} buildings")
        return result
