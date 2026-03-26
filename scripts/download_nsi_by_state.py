#!/usr/bin/env python3
"""Download NSI inventory by state from the USACE API and convert it to parquet."""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, TextIO
from urllib import error, request

try:  # Supports both `python -m scripts.download_nsi_by_state` and `python scripts/download_nsi_by_state.py`.
    from .nsi_raw_to_parquet import convert_raw_nsi_to_parquet, validate_schema
except ImportError:  # pragma: no cover - exercised when the script is run directly.
    from nsi_raw_to_parquet import convert_raw_nsi_to_parquet, validate_schema


API_ROOT = "https://nsi.sec.usace.army.mil/nsiapi/structures"
DEFAULT_TIMEOUT = 600.0
DEFAULT_RETRIES = 3
FEATURE_COLLECTION_PREFIX = '{"type":"FeatureCollection","features":['
FEATURE_COLLECTION_SUFFIX = "]}"
GEOPANDAS_MEMORY_WARNING_FIPS = {"06", "12", "13", "22", "37", "48"}


@dataclass(frozen=True)
class StateSpec:
    name: str
    abbr: str
    fips: str

    @property
    def path_name(self) -> str:
        return self.name.replace(" ", "_")

    @property
    def api_url(self) -> str:
        return f"{API_ROOT}?fips={self.fips}&fmt=fs"


STATE_SPECS = [
    StateSpec("Alabama", "AL", "01"),
    StateSpec("Alaska", "AK", "02"),
    StateSpec("Arizona", "AZ", "04"),
    StateSpec("Arkansas", "AR", "05"),
    StateSpec("California", "CA", "06"),
    StateSpec("Colorado", "CO", "08"),
    StateSpec("Connecticut", "CT", "09"),
    StateSpec("Delaware", "DE", "10"),
    StateSpec("District Of Columbia", "DC", "11"),
    StateSpec("Florida", "FL", "12"),
    StateSpec("Georgia", "GA", "13"),
    StateSpec("Hawaii", "HI", "15"),
    StateSpec("Idaho", "ID", "16"),
    StateSpec("Illinois", "IL", "17"),
    StateSpec("Indiana", "IN", "18"),
    StateSpec("Iowa", "IA", "19"),
    StateSpec("Kansas", "KS", "20"),
    StateSpec("Kentucky", "KY", "21"),
    StateSpec("Louisiana", "LA", "22"),
    StateSpec("Maine", "ME", "23"),
    StateSpec("Maryland", "MD", "24"),
    StateSpec("Massachusetts", "MA", "25"),
    StateSpec("Michigan", "MI", "26"),
    StateSpec("Minnesota", "MN", "27"),
    StateSpec("Mississippi", "MS", "28"),
    StateSpec("Missouri", "MO", "29"),
    StateSpec("Montana", "MT", "30"),
    StateSpec("Nebraska", "NE", "31"),
    StateSpec("Nevada", "NV", "32"),
    StateSpec("New Hampshire", "NH", "33"),
    StateSpec("New Jersey", "NJ", "34"),
    StateSpec("New Mexico", "NM", "35"),
    StateSpec("New York", "NY", "36"),
    StateSpec("North Carolina", "NC", "37"),
    StateSpec("North Dakota", "ND", "38"),
    StateSpec("Ohio", "OH", "39"),
    StateSpec("Oklahoma", "OK", "40"),
    StateSpec("Oregon", "OR", "41"),
    StateSpec("Pennsylvania", "PA", "42"),
    StateSpec("Rhode Island", "RI", "44"),
    StateSpec("South Carolina", "SC", "45"),
    StateSpec("South Dakota", "SD", "46"),
    StateSpec("Tennessee", "TN", "47"),
    StateSpec("Texas", "TX", "48"),
    StateSpec("Utah", "UT", "49"),
    StateSpec("Vermont", "VT", "50"),
    StateSpec("Virginia", "VA", "51"),
    StateSpec("Washington", "WA", "53"),
    StateSpec("West Virginia", "WV", "54"),
    StateSpec("Wisconsin", "WI", "55"),
    StateSpec("Wyoming", "WY", "56"),
]

STATE_BY_FIPS = {state.fips: state for state in STATE_SPECS}
STATE_BY_ABBR = {state.abbr: state for state in STATE_SPECS}
STATE_BY_NAME = {
    re.sub(r"\s+", " ", state.name.replace("-", " ").strip()).lower(): state
    for state in STATE_SPECS
}


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_state_identifier(value: str) -> StateSpec:
    token = value.strip()
    if not token:
        raise ValueError("state identifier cannot be empty")

    if re.fullmatch(r"\d{2}", token):
        try:
            return STATE_BY_FIPS[token]
        except KeyError as exc:
            raise ValueError(f"unsupported state FIPS code: {token}") from exc

    if re.fullmatch(r"[A-Za-z]{2}", token):
        upper = token.upper()
        try:
            return STATE_BY_ABBR[upper]
        except KeyError as exc:
            raise ValueError(f"unsupported state abbreviation: {upper}") from exc

    normalized_name = re.sub(r"\s+", " ", token.replace("_", " ").replace("-", " ").strip()).lower()
    try:
        return STATE_BY_NAME[normalized_name]
    except KeyError as exc:
        raise ValueError(f"unsupported state name: {value}") from exc


def resolve_states(values: list[str]) -> list[StateSpec]:
    resolved: list[StateSpec] = []
    seen_fips: set[str] = set()
    for value in values:
        state = normalize_state_identifier(value)
        if state.fips in seen_fips:
            continue
        seen_fips.add(state.fips)
        resolved.append(state)
    return resolved


def ensure_output_paths(paths: Iterable[Path], overwrite: bool) -> None:
    conflicts = [str(path) for path in paths if path.exists()]
    if conflicts and not overwrite:
        joined = ", ".join(conflicts)
        raise FileExistsError(f"refusing to overwrite existing file(s): {joined}")


def make_temp_path(final_path: Path) -> Path:
    final_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = final_path.with_name(f"{final_path.name}.part")
    if temp_path.exists():
        temp_path.unlink()
    return temp_path


def finalize_temp_path(temp_path: Path, final_path: Path) -> None:
    os.replace(temp_path, final_path)


def cleanup_temp_path(temp_path: Path) -> None:
    if temp_path.exists():
        temp_path.unlink()


def open_url_with_retries(url: str, timeout: float, retries: int):
    if retries < 0:
        raise ValueError(f"retries must be >= 0, got {retries}")
    last_error: Exception | None = None
    for attempt in range(1, retries + 2):
        try:
            return request.urlopen(url, timeout=timeout)
        except (error.URLError, TimeoutError, OSError) as exc:
            last_error = exc
            if attempt > retries:
                break
            sleep_seconds = min(5, attempt)
            log(
                f"Request failed for {url} (attempt {attempt}/{retries + 1}): {exc}. Retrying in {sleep_seconds}s..."
            )
            time.sleep(sleep_seconds)
    raise RuntimeError(f"failed to download NSI API response from {url}") from last_error


def write_feature_collection(feature_lines: Iterable[str], destination: TextIO) -> int:
    destination.write(FEATURE_COLLECTION_PREFIX)
    count = 0

    for raw_line in feature_lines:
        line = raw_line.strip()
        if not line:
            continue

        try:
            feature = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError("NSI feature stream contained invalid JSON") from exc

        if not isinstance(feature, dict) or feature.get("type") != "Feature":
            raise ValueError("NSI feature stream entry is not a GeoJSON Feature")

        if count:
            destination.write(",")
        json.dump(feature, destination, separators=(",", ":"))
        count += 1

    destination.write(FEATURE_COLLECTION_SUFFIX)

    if count == 0:
        raise ValueError("NSI API returned an empty feature stream")
    return count


def download_state_geojson(
    *,
    state: StateSpec,
    destination: Path,
    timeout: float,
    retries: int,
    overwrite: bool,
) -> tuple[int, int]:
    ensure_output_paths([destination], overwrite=overwrite)
    temp_path = make_temp_path(destination)
    try:
        with open_url_with_retries(state.api_url, timeout=timeout, retries=retries) as response:
            with (
                io.TextIOWrapper(response, encoding="utf-8") as stream,
                temp_path.open("w", encoding="utf-8") as handle,
            ):
                feature_count = write_feature_collection(stream, handle)
                handle.flush()
        bytes_written = temp_path.stat().st_size
        finalize_temp_path(temp_path, destination)
        return feature_count, bytes_written
    except Exception:
        cleanup_temp_path(temp_path)
        raise


def convert_geojson_to_parquet(
    *,
    raw_geojson_path: Path,
    parquet_path: Path,
    engine: str,
    overwrite: bool,
) -> tuple[int, int]:
    ensure_output_paths([parquet_path], overwrite=overwrite)
    temp_path = make_temp_path(parquet_path)
    try:
        row_count = convert_raw_nsi_to_parquet(str(raw_geojson_path), str(temp_path), engine=engine)
        if not validate_schema(str(temp_path)):
            raise RuntimeError(f"schema validation failed for {temp_path}")
        bytes_written = temp_path.stat().st_size
        finalize_temp_path(temp_path, parquet_path)
        return row_count, bytes_written
    except Exception:
        cleanup_temp_path(temp_path)
        raise


def build_output_dir(custom_output_dir: str | None) -> Path:
    if custom_output_dir:
        return Path(custom_output_dir).expanduser().resolve()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    return (Path("exports") / "nsi_downloads" / run_id).resolve()


def warn_about_runtime_risks(
    states: list[StateSpec], engine: str, custom_output_dir: str | None
) -> None:
    if engine == "geopandas":
        large_states = [
            state.name for state in states if state.fips in GEOPANDAS_MEMORY_WARNING_FIPS
        ]
        if large_states:
            joined = ", ".join(large_states)
            log(
                "WARNING: geopandas fallback loads the full GeoJSON into memory. "
                f"Large-state runs may be memory-heavy for: {joined}. Prefer --engine duckdb when available."
            )

    if custom_output_dir is None:
        log(
            "INFO: using a timestamped output directory. "
            "For a stable downstream parquet glob, consider --output-dir data."
        )


def download_state_inventory(
    state: StateSpec,
    output_dir: Path,
    engine: str,
    overwrite: bool,
    timeout: float,
    retries: int,
) -> dict[str, Any]:
    raw_geojson_path = output_dir / "raw" / f"nsi_2022_{state.fips}_{state.path_name}.geojson"
    parquet_path = (
        output_dir / "processed" / "nsi" / f"state={state.path_name}" / "part-00000.snappy.parquet"
    )

    ensure_output_paths([raw_geojson_path, parquet_path], overwrite=overwrite)

    log(f"Downloading state={state.name} ({state.abbr}, FIPS={state.fips}) from NSI API...")
    feature_count, geojson_bytes = download_state_geojson(
        state=state,
        destination=raw_geojson_path,
        timeout=timeout,
        retries=retries,
        overwrite=overwrite,
    )
    log(f"Downloaded {feature_count:,} features to {raw_geojson_path}")

    log(f"Converting {raw_geojson_path.name} to parquet with engine={engine}...")
    parquet_rows, parquet_bytes = convert_geojson_to_parquet(
        raw_geojson_path=raw_geojson_path,
        parquet_path=parquet_path,
        engine=engine,
        overwrite=overwrite,
    )
    log(f"Wrote {parquet_rows:,} parquet rows to {parquet_path}")

    return {
        "state": state.name,
        "state_path": state.path_name,
        "abbr": state.abbr,
        "fips": state.fips,
        "api_url": state.api_url,
        "feature_count": feature_count,
        "raw_geojson": str(raw_geojson_path),
        "raw_geojson_bytes": geojson_bytes,
        "processed_parquet": str(parquet_path),
        "processed_parquet_rows": parquet_rows,
        "processed_parquet_bytes": parquet_bytes,
    }


def write_manifest(
    output_dir: Path,
    state_results: list[dict[str, Any]],
    engine: str,
    timeout: float,
    retries: int,
    overwrite: bool,
) -> Path:
    manifest_path = output_dir / "manifest.json"
    ensure_output_paths([manifest_path], overwrite=overwrite)
    temp_path = make_temp_path(manifest_path)
    manifest = {
        "generated_at": now_utc_iso(),
        "output_dir": str(output_dir),
        "engine": engine,
        "timeout": timeout,
        "retries": retries,
        "states": state_results,
    }
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=False)
            handle.write("\n")
        finalize_temp_path(temp_path, manifest_path)
        return manifest_path
    except Exception:
        cleanup_temp_path(temp_path)
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download NSI by state from the USACE API")
    parser.add_argument(
        "--state",
        action="append",
        required=True,
        help="State name, 2-letter abbreviation, or 2-digit FIPS code. Repeat for multiple states.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for downloaded GeoJSON and parquet outputs (default: exports/nsi_downloads/<timestamp>)",
    )
    parser.add_argument(
        "--engine",
        default="duckdb",
        choices=["duckdb", "geopandas"],
        help="Conversion engine for raw GeoJSON -> parquet (default: duckdb)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing raw GeoJSON, parquet, and manifest files.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help=f"Per-request timeout in seconds (default: {DEFAULT_TIMEOUT})",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help=f"Number of retry attempts for API calls (default: {DEFAULT_RETRIES})",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        states = resolve_states(args.state)
    except ValueError as exc:
        parser.error(str(exc))

    output_dir = build_output_dir(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    warn_about_runtime_risks(states, args.engine, args.output_dir)

    results: list[dict[str, Any]] = []
    try:
        for state in states:
            results.append(
                download_state_inventory(
                    state=state,
                    output_dir=output_dir,
                    engine=args.engine,
                    overwrite=args.overwrite,
                    timeout=args.timeout,
                    retries=args.retries,
                )
            )
        manifest_path = write_manifest(
            output_dir=output_dir,
            state_results=results,
            engine=args.engine,
            timeout=args.timeout,
            retries=args.retries,
            overwrite=args.overwrite,
        )
    except Exception as exc:
        log(f"ERROR: {exc}")
        return 1

    log(f"Completed {len(results)} state download(s). Manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
