# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CMU Heinz MSPPM 2026 Capstone for American Red Cross. Property-level storm surge/tsunami impact modeling using FAST + NSI + NHC P-Surge. Deterministic L/M/H intensity classification (not ML — see `docs/DIRECTION.md` for pivot rationale).

## Architecture

Five-stage pipeline: NHC raster + NSI inventory → FAST damage engine → damage classification → BHI shelter demand → validation. See `docs/e2e_pipeline.md` for full Mermaid diagram.

Key module relationships:

- `scripts/duckdb_fast_pipeline.py` — Primary DuckDB pipeline. Owns `FOUND_TYPE_MAP` (canonical foundation type mapping) and `_raster_bbox_wgs84()` for spatial filtering.
- `FAST-main/Python_env/run_fast.py` → imports `hazus_notinuse.py:local_with_options()` → reads `FAST-main/Lookuptables/` DDFs. This is the FAST execution chain.
- `scripts/us_states.py` — Single source of truth for state FIPS, abbreviations, API URLs. Import from here, do not duplicate.
- `scripts/read_excel_config.py` — Reads config from `data/ARC Storm Surge Shelter Demand.xlsx` Interface sheet. Provides `load_config_from_excel()`.
- `scripts/nsi_downloader.py` — NSI download client supporting USACE API and HuggingFace backends.
- `notebooks/shelter_demand.ipynb` — BHI + SVI shelter demand E2E pipeline (Colab). **Primary ARC deliverable.**

Agent execution rules and data contracts: @AGENTS.md

## Critical Gotchas

- **hazus_notinuse.py**: File name and header "OBSOLETE" comment are inherited from FEMA's original code. Our fork still uses it (`run_fast.py:70` imports `local_with_options`). Raster loading was migrated to `rasterio` (commit `9128ea0`).
- **FIRM zones ≠ event footprint**: Do NOT use FIRM zones as spatial filter for event impact. Spatial filtering must use raster bbox (`_raster_bbox_wgs84`).
- **FltyId deduplication**: Must deduplicate via `ROW_NUMBER() OVER (PARTITION BY bid)` before FAST input.
- **Partial FAST output**: `run_fast_job` checks returncode + file existence but not row count — partial writes on crash pass the success check.
- **numpy<2**: Pinned in `pyproject.toml` (rasterio/geopandas compatibility).

## Data Contracts

- NSI→FAST column mapping: defined in @AGENTS.md Section 4 (do not duplicate here).
- Foundation type mapping canonical source: `scripts/duckdb_fast_pipeline.py:FOUND_TYPE_MAP`.
- Flood depth raster: NHC P-Surge GeoTIFF (depth in feet), stored in `FAST-main/rasters/`. Naming: `{STORM}_{YEAR}_adv{N}_e10_ResultMaskRaster.tif`.

## L/M/H Classification & BHI

Intensity thresholds and ARC planning factors are in README.md (canonical location). BHI (Building Habitability Index) combines per-building damage state fractions with utility disruption factors to estimate the fraction of residents who will seek shelter.

## Colab Notebook Gotchas

- First cell must add project root to `sys.path` for `from scripts.*` imports.
- Use `tqdm` with rate display (postfix MB/s or it/s) on all download/streaming paths.
- Pass cross-cell data via small artifacts under `WORK_DIR` (e.g., join CSV), not in-memory variables only defined in earlier cells.

## Configuration

- `configs/event_state_map.yaml` — hurricane → affected states + raster patterns (11 events).
- `data/ARC Storm Surge Shelter Demand.xlsx` — Excel config interface (storm params, thresholds, BHI ratios).
- `pyproject.toml` — ruff (line-length 100, py310, select E/F/W/I), pytest testpaths, dependencies.
- **Ruff discrepancy**: `pyproject.toml` sets line-length=100, but `.claude/settings.json` PostToolUse hook overrides with `--config 'line-length=120'`. The hook wins on Claude edits; manual/CI runs use 100.

## Testing

Use `pytest`. Test paths: `tests/` + `FAST-main/tests/` (configured in `pyproject.toml`).
Shared fixtures in `tests/conftest.py` (provides `project_root` fixture).
Pipeline validation: `scripts/validate_pipeline.py` (schema checks + aggregate stats).
