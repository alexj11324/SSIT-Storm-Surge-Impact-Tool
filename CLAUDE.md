# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CMU Heinz MSPPM 2026 Capstone for American Red Cross. Property-level storm surge/tsunami impact modeling using FEMA's FAST (Flood Assessment Structure Tool) with NSI building inventory (30M+ structures) and SLOSH surge rasters. Goal: estimate building damage, displaced population, and high-need populations for Red Cross shelter/casework planning.

## Architecture

```
NSI Parquet --> DuckDB SQL (clean/filter/dedup/map) --> FAST CSV
NHC P-Surge GeoTIFF (FAST-main/rasters/)                  |
                                                    FAST engine --> damage predictions
```

- Primary pipeline: `scripts/duckdb_fast_pipeline.py`
- FAST headless engine: `FAST-main/Python_env/run_fast.py`
- Agent execution rules: @AGENTS.md
- Pipeline architecture: @docs/shelter_demand_pipeline.md

## Critical Gotchas

- `hazus_notinuse.py` is NOT obsolete -- it is the active FAST execution engine called by `run_fast.py`
- `manage.py` uses `ctypes.windll` -- do NOT import on macOS/Linux
- Do NOT use FIRM zones as spatial filter for event impact (FIRM = long-term risk; raster = event footprint)
- Spatial filtering must use raster bbox (`_raster_bbox_wgs84`) -- all buildings outside bbox are dropped
- `FltyId` must be deduplicated (DuckDB pipeline handles via `ROW_NUMBER() OVER (PARTITION BY bid)`)
- Partial FAST output: `run_fast_job` checks returncode + file existence but not row count -- partial writes on crash pass the success check

## Commands

```bash
# Primary pipeline
python scripts/duckdb_fast_pipeline.py \
  --parquet-glob "data/nsi/state=FL/*.parquet" \
  --raster FAST-main/rasters/IAN_2022_adv33_e10_ResultMaskRaster.tif \
  --output outputs/fast_input.csv \
  --flc CoastalA

# SLOSH -> raster
python scripts/slosh_to_raster.py --basin ny3mom --category 3 --tide high

# H3 spatial index
python scripts/h3_spatial_index.py --raster path/to/raster.tif --resolution 7

# Validate output
python scripts/validate_pipeline.py --predictions path/to/output.csv
```

## Data Contracts

### NSI -> FAST CSV Mapping

| NSI Field | FAST Column | Notes |
|-----------|-------------|-------|
| `bid` | `FltyId` | Deduplicate before writing |
| `occtype` | `Occ` | e.g. RES1, COM1 |
| `val_struct` | `Cost` | Replacement cost ($) |
| `sqft` | `Area` | Floor area (sqft) |
| `num_story` | `NumStories` | Stories above ground |
| `found_type` | `FoundationType` | Numeric: Pier=2, Basement=4, Crawl=5, Slab=7 |
| `found_ht` | `FirstFloorHt` | Feet above grade |
| `latitude`/`longitude` | `Latitude`/`Longitude` | WGS84 |

Full contract with optional columns and runtime params: @AGENTS.md

### Flood Depth Raster

Pipeline uses NHC P-Surge GeoTIFF rasters directly (inundation depth in feet). See `docs/shelter_demand_pipeline.md` for the full data flow.

## FAST Runtime Parameters

- `flC`: `CoastalA` (default) | `CoastalV` (high-risk) | `Riverine` (inland)
- `raster`: path to `.tif` flood depth raster

## Configuration

- `configs/event_state_map.yaml` -- hurricane -> affected states + raster patterns

## Dependencies

Python 3.10+. Key packages: `duckdb`, `rasterio`, `geopandas`, `pyarrow`, `pyyaml`, `h3`.
Conda env spec: `FAST-main/src/environment.yaml` (FAST-specific, Windows-focused).

## Testing

Use `pytest`. Pipeline validation: `scripts/validate_pipeline.py`.
Test data parity: `FAST-main/tests/test_csv_parquet_parity.py`.
Test scaffold: `tests/conftest.py` (shared fixtures).
