#!/usr/bin/env python3
"""Upload NSI Parquet files to Hugging Face Hub as a dataset.

Usage:
    # Step 1: Download all states (run once, takes hours)
    python scripts/download_nsi_by_state.py \
        --state AL --state AK --state AZ ... --state WY \
        --output-dir data --engine duckdb --overwrite

    # Step 2: Upload to HF
    python scripts/upload_nsi_to_hf.py \
        --parquet-dir data/processed/nsi \
        --repo-id <your-username>/nsi-2022 \
        --token hf_xxxxx

    # Or download + upload in one go:
    python scripts/upload_nsi_to_hf.py \
        --download-all --output-dir data \
        --repo-id <your-username>/nsi-2022 \
        --token hf_xxxxx
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

DATASET_CARD = """\
---
license: other
license_name: public-domain
tags:
  - geospatial
  - buildings
  - infrastructure
  - flood
  - FEMA
  - USACE
  - NSI
size_categories:
  - 10M<n<100M
---

# USACE National Structure Inventory (NSI) 2022

Building-level inventory of ~30M structures across all US states/territories,
sourced from the [USACE NSI API](https://nsi.sec.usace.army.mil/).

Partitioned by state for efficient per-state loading.

## Quick Start

```python
from datasets import load_dataset

# Load one state (fast)
ds = load_dataset("{repo_id}", data_files="state=Florida/*.parquet")

# Load multiple states
ds = load_dataset("{repo_id}", data_files=[
    "state=Florida/*.parquet",
    "state=Louisiana/*.parquet",
])

# Load all (large, ~10-20 GB)
ds = load_dataset("{repo_id}")
```

## Key Columns

| Column | Type | Description |
|--------|------|-------------|
| bid | string | Building ID (unique per structure) |
| occtype | string | Occupancy type (RES1, COM1, etc.) |
| val_struct | float | Structure replacement cost ($) |
| sqft | float | Floor area (sqft) |
| num_story | int | Stories above ground |
| found_type | string | Foundation type |
| found_ht | float | First floor height (ft above grade) |
| cbfips | string | Census block FIPS |
| latitude | float | WGS84 latitude |
| longitude | float | WGS84 longitude |
| val_cont | float | Content value ($) |
| pop2pmu65 | int | Population under 65 (PM estimate) |
| pop2pmo65 | int | Population over 65 (PM estimate) |

## Source

USACE National Structure Inventory: <https://nsi.sec.usace.army.mil/>

## License

Public domain (US Government work).
"""


def download_all_states(output_dir: str, engine: str = "duckdb") -> None:
    """Run download_nsi_by_state.py for all states defined in STATE_BY_ABBR.

    Raises SystemExit if the download phase fails.
    """
    try:
        from scripts.download_nsi_by_state import main as download_main
        from scripts.us_states import STATE_BY_ABBR
    except ImportError:
        from download_nsi_by_state import main as download_main  # type: ignore[import-not-found]
        from us_states import STATE_BY_ABBR  # type: ignore[import-not-found]

    all_abbrs = sorted(STATE_BY_ABBR.keys())
    argv: list[str] = []
    for abbr in all_abbrs:
        argv.extend(["--state", abbr])
    argv.extend(["--output-dir", output_dir, "--engine", engine, "--overwrite"])

    print(f"Downloading {len(all_abbrs)} states to {output_dir} ...")
    rc = download_main(argv)
    if rc != 0:
        print("ERROR: download phase failed; aborting before upload to prevent publishing incomplete data.")
        sys.exit(1)


def upload_to_hf(parquet_dir: str, repo_id: str, token: str | None, private: bool) -> None:
    """Upload Hive-partitioned Parquet directory to HF Hub."""
    try:
        from huggingface_hub import HfApi
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "huggingface_hub"])
        from huggingface_hub import HfApi

    pdir = Path(parquet_dir)
    parquets = sorted(pdir.glob("state=*/*.parquet"))
    if not parquets:
        parquets = sorted(pdir.glob("**/*.parquet"))
    if not parquets:
        print(f"ERROR: no .parquet files found under {pdir}")
        sys.exit(1)

    print(f"Found {len(parquets)} Parquet files to upload")
    for p in parquets[:5]:
        print(f"  {p.relative_to(pdir)}")
    if len(parquets) > 5:
        print(f"  ... and {len(parquets) - 5} more")

    api = HfApi(token=token)

    # Create dataset repo
    api.create_repo(repo_id=repo_id, repo_type="dataset", private=private, exist_ok=True)
    print(f"Repo ready: https://huggingface.co/datasets/{repo_id}")

    # Upload dataset card
    card = DATASET_CARD.replace("{repo_id}", repo_id)
    api.upload_file(
        path_or_fileobj=card.encode("utf-8"),
        path_in_repo="README.md",
        repo_id=repo_id,
        repo_type="dataset",
    )
    print("Uploaded README.md (dataset card)")

    # Upload all Parquet files (chunked, resumable)
    print("Uploading Parquet files (this may take a while for large datasets)...")
    api.upload_folder(
        folder_path=str(pdir),
        repo_id=repo_id,
        repo_type="dataset",
        allow_patterns=["**/*.parquet"],
    )

    print(f"\nDone! Dataset: https://huggingface.co/datasets/{repo_id}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Download NSI to Parquet and/or upload to Hugging Face Hub",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--download-all",
        action="store_true",
        help="Download all 51 states before uploading",
    )
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Base output directory for downloads (default: data)",
    )
    parser.add_argument(
        "--parquet-dir",
        default=None,
        help="Directory containing state=*/*.parquet (default: <output-dir>/processed/nsi)",
    )
    parser.add_argument(
        "--repo-id",
        required=True,
        help="HF dataset repository ID (e.g. your-username/nsi-2022)",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="HF write token (or set HF_TOKEN env var)",
    )
    parser.add_argument(
        "--private",
        action="store_true",
        help="Make dataset repo private",
    )
    parser.add_argument(
        "--engine",
        default="duckdb",
        choices=["duckdb", "geopandas"],
        help="Conversion engine for GeoJSON -> Parquet (default: duckdb)",
    )

    args = parser.parse_args(argv)

    if args.download_all:
        download_all_states(args.output_dir, engine=args.engine)

    # Step 2: Upload
    parquet_dir = args.parquet_dir or str(Path(args.output_dir) / "processed" / "nsi")
    upload_to_hf(parquet_dir, args.repo_id, args.token, args.private)

    return 0


if __name__ == "__main__":
    sys.exit(main())
