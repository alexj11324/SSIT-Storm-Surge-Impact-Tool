# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CMU Heinz MSPPM 2026 Capstone for American Red Cross. Property-level storm surge/tsunami impact modeling using FEMA's FAST (Flood Assessment Structure Tool) with NSI building inventory (30M+ structures) and NHC P-Surge rasters. Goal: estimate building damage, displaced population, and high-need populations for Red Cross shelter/casework planning.

**Architecture Direction**: The project pivoted from ML-based shelter prediction (R²=-0.308, worse than mean) to deterministic L/M/H intensity classification. See @docs/DIRECTION.md for full context. `scripts/ml_damage_model.py` has been deleted.

## Architecture

Five-stage pipeline (see `docs/e2e_pipeline.md` for the full Mermaid diagram):

```
Stage 1: Data Acquisition
  NHC P-Surge ZIP → GeoTIFF + identify affected states
  USACE NSI API → GeoJSON → Parquet (per state, partitioned)

Stage 2: FAST Damage Engine
  NSI Parquet --(DuckDB: bbox filter, dedup, column map)--> FAST CSV
  FAST CSV + GeoTIFF --(hazus_notinuse.py: DDF lookup)--> predictions.csv

Stage 3: Damage Classification (Census Tract)
  predictions JOIN NSI (by FltyId=bid) → tract GEOID (cbfips[:11])
  BldgDmgPct thresholds → damage state → tract severity (HIGH/MEDIUM/LOW)

Stage 4: Shelter Demand  (runs in Colab: notebooks/shelter_demand.ipynb)
  BHI factor × Census population × SVI mapping → shelter-seeking estimate

Stage 5: Validation
  Compare vs ground truth (9 hurricanes 2018-2024) → RMSE, MAE, R²
```

Key modules and their relationships:

- `scripts/duckdb_fast_pipeline.py` — Primary DuckDB pipeline: NSI Parquet → FAST CSV. Owns `FOUND_TYPE_MAP` (canonical foundation type mapping) and `_raster_bbox_wgs84()` for spatial filtering.
- `FAST-main/Python_env/run_fast.py` → imports `hazus_notinuse.py:local_with_options()` → reads `FAST-main/Lookuptables/` DDFs. This is the FAST execution chain.
- `scripts/import_nhc_by_storm.py` — Downloads NHC P-Surge raster ZIPs, extracts TIF, identifies overlapping states via `pygris`.
- `scripts/download_nsi_by_state.py` — Downloads NSI from USACE API → GeoJSON → Parquet. Uses `scripts/nsi_raw_to_parquet.py` for conversion.
- `scripts/us_states.py` — Single source of truth for state FIPS, abbreviations, and API URLs (`STATE_BY_ABBR`, `STATE_SPECS`, etc.). Import from here, do not duplicate state lists.
- `notebooks/shelter_demand.ipynb` — BHI + SVI shelter demand E2E pipeline, designed for Google Colab. **This is the primary ARC deliverable.**

Agent execution rules and data contracts: @AGENTS.md — defines hard defaults (CoastalA, headless execution), what agents should/should not ask, and learned workspace facts.

### L/M/H Intensity Classification

Damage classification thresholds (from ARC's Figure 9 methodology):

| Level | BldgDmgPct | Surge Depth | ARC Planning Factor |
|-------|-----------|-------------|---------------------|
| High | >35% destroyed | >12 ft | 5% of affected pop |
| Medium | 11-34% destroyed | 9-12 ft | 3% of affected pop |
| Low | 0-10% destroyed | 4-8 ft | 1% of affected pop |

These thresholds drive Stage 3 (damage classification) and Stage 4 (shelter demand). Surge depth thresholds (4/9/12 ft) are ARC "gut feeling" baselines — refining them with data is a project goal.

## Shelter Demand Pipeline (Stage 4)

Runs in Google Colab: `notebooks/shelter_demand.ipynb`. This is the primary ARC deliverable.

Pipeline: FAST predictions → L/M/H classification → census tract aggregation → BHI (Building Habitability Index) factor → join Census ACS population + CDC SVI → shelter demand estimate.

Colab-specific gotchas:
- First cell must add project root to `sys.path` for `from scripts.*` imports.
- Use `tqdm` with rate display (postfix MB/s or it/s) on all download/streaming paths.
- Pass cross-cell data via small artifacts under `WORK_DIR` (e.g., join CSV), not in-memory variables only defined in earlier cells.

## Critical Gotchas

- `hazus_notinuse.py` is NOT obsolete — it is the active FAST execution engine called by `run_fast.py`. The "obsolete" header comment in the file is misleading; `local_with_options()` is the production entrypoint.
- Do NOT use FIRM zones as spatial filter for event impact (FIRM = long-term risk; raster = event footprint). Spatial filtering must use raster bbox (`_raster_bbox_wgs84`) — all buildings outside bbox are dropped.
- `FltyId` must be deduplicated (DuckDB pipeline handles via `ROW_NUMBER() OVER (PARTITION BY bid)`).
- Partial FAST output: `run_fast_job` checks returncode + file existence but not row count — partial writes on crash pass the success check.
- `numpy<2` is pinned in `pyproject.toml` (compatibility constraint with rasterio/geopandas).

## Commands

```bash
# --- Data Acquisition ---
# Download NSI inventory for specific states
python scripts/download_nsi_by_state.py --state FL --state GA --output-dir data
# Download NHC P-Surge raster (example inline usage in import_nhc_by_storm.py __main__)

# --- FAST CSV Generation ---
python scripts/duckdb_fast_pipeline.py \
  --parquet-glob "data/nsi/state=FL/*.parquet" \
  --raster FAST-main/rasters/IAN_2022_adv33_e10_ResultMaskRaster.tif \
  --output outputs/fast_input.csv

# --- Run FAST Engine (headless, from FAST-main/Python_env/) ---
python FAST-main/Python_env/run_fast.py \
  --inventory outputs/fast_input.csv \
  --mapping-json '{"UserDefinedFltyId":"FltyId","OCC":"Occ","Cost":"Cost","Area":"Area","NumStories":"NumStories","FoundationType":"FoundationType","FirstFloorHt":"FirstFloorHt","ContentCost":"ContentCost","Latitude":"Latitude","Longitude":"Longitude"}' \
  --flc CoastalA \
  --rasters FAST-main/rasters/IAN_2022_adv33_e10_ResultMaskRaster.tif \
  --pretty

# --- Validation ---
python scripts/validate_pipeline.py path/to/predictions.csv

# --- Linting ---
ruff check scripts/ tests/          # lint (rules: E, F, W, I; line-length 100)
ruff format scripts/ tests/          # auto-format
# Note: FAST-main/** is excluded from ruff; notebooks/ is excluded from ruff and pyright

# --- Testing ---
pytest                               # run all tests (tests/ + FAST-main/tests/)
pytest tests/test_duckdb_fast_pipeline.py -v   # single test file
pytest tests/ -k "test_cli"          # run by keyword match

# --- H3 Spatial Index (optional pre-filter) ---
python scripts/h3_spatial_index.py --raster path/to/raster.tif --parquet data/nsi/*.parquet --resolution 7
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
| `val_cont` | `ContentCost` | Optional, defaults to 0 via COALESCE |

Foundation type mapping is defined in `scripts/duckdb_fast_pipeline.py:FOUND_TYPE_MAP` — this is the canonical source for both the DuckDB pipeline and notebook.

Full contract with optional columns and runtime params: @AGENTS.md

### Flood Depth Raster

NHC P-Surge GeoTIFF (inundation depth in feet). Rasters stored in `FAST-main/rasters/`. Naming convention: `{STORM}_{YEAR}_adv{N}_e10_ResultMaskRaster.tif`.

## FAST Runtime Parameters

- `flC`: `CoastalA` (default) | `CoastalV` (high-risk) | `Riverine` (inland)
- `raster`: path to `.tif` flood depth raster

## Configuration

- `configs/event_state_map.yaml` — hurricane -> affected states + raster patterns (11 events)
- `pyproject.toml` — ruff config (line-length 100, py310, select E/F/W/I), pytest testpaths, dependencies
- `pyrightconfig.json` — excludes `notebooks/`
- **Ruff discrepancy**: `pyproject.toml` sets line-length=100, but `.claude/settings.json` PostToolUse hook overrides with `--config 'line-length=120'`. The hook wins on Claude edits; manual/CI runs use 100.

## Testing

Use `pytest`. Test paths: `tests/` + `FAST-main/tests/` (configured in `pyproject.toml`).
Shared fixtures in `tests/conftest.py` (provides `project_root` fixture).
Pipeline validation: `scripts/validate_pipeline.py` (schema checks + aggregate stats).
