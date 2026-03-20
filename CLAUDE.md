# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CMU Heinz MSPPM 2026 Capstone for American Red Cross. Property-level storm surge impact modeling using FEMA's FAST (Flood Assessment Structure Tool) with NSI building inventory (30M+ structures) and NHC P-Surge rasters. Goal: estimate building damage, displaced population, and high-need populations for Red Cross shelter/casework planning.

## Architecture

There are two major pipelines:

### Pipeline 1: FAST Damage Engine (Building-Level)

```
NSI API / Parquet  → DuckDB: clean/filter/dedup/map → FAST CSV
                                                          ↓
NHC P-Surge GeoTIFF (FAST-main/rasters/) ────────→ FAST engine → damage predictions
                                                          ↓
                                            BldgDmgPct, Depth_Grid, BldgLossUSD per building
```

**Primary pipeline**: `scripts/duckdb_fast_pipeline.py` — single SQL pass handles spatial filtering (bbox), deduplication (`ROW_NUMBER() OVER (PARTITION BY bid)`), and column mapping. Preferred for performance.

**Legacy pipeline**: `scripts/fast_e2e_from_oracle.py` — row-by-row Python with `ThreadPoolExecutor`. Name is historical; no longer connects to Oracle.

Both produce identical FAST CSV format and invoke the same FAST engine.

### Pipeline 2: L/M/H Population Impact (County-Level)

```
FAST predictions (Athena)
    → Dedup across advisories (MAX damage per building)
    → Classify intensity zone: HIGH/MEDIUM/LOW by surge depth + damage % fallback
    → Spatial join to county (ST_CONTAINS)
    → County aggregation (GROUP BY event, county, zone)
    → Census population join + ARC conversion rates
    → Planning Assumptions Spreadsheet (columns J-R)
```

Scripts live in `research/population_impact/scripts/` (01–06, executed sequentially). Full flowchart: `docs/pipeline_flowchart.md`. Implementation plan: `research/population_impact/IMPLEMENTATION_PLAN.md`.

**Intensity zone classification logic** (surge-primary, damage-fallback):
- `depth_grid > 12ft` → HIGH | `>= 9ft` → MEDIUM | `>= 4ft` → LOW
- Fallback: `bldgdmgpct > 35%` → HIGH | `> 15%` → MEDIUM | `> 0%` → LOW

**ARC conversion rates** (shelter: H=5%, M=3%, L=1% | feeding: H=12%, M=7%, L=3%).

### Data Sources

| Dataset | Format | Location |
|---------|--------|----------|
| **NSI** (National Structure Inventory) | Parquet, partitioned by state | Local filesystem or via `download_nsi_by_state.py` |
| **NHC P-Surge rasters** | GeoTIFF (.tif), flood depth in feet | `FAST-main/rasters/` — 27 rasters (9 events x 3 advisories) |
| **Ground Truth** | Excel | `Ground Truth Data.xlsx` — 9 hurricanes 2018-2024 |
| **FAST Depth-Damage Functions** | CSV/Excel lookup tables | `FAST-main/Lookuptables/` |
| **Census ACS 5-year** | API | County population for L/M/H pipeline |

### FAST Engine Internals

- Headless entrypoint: `FAST-main/Python_env/run_fast.py` — accepts `--inventory`, `--mapping-json`, `--flc`, `--raster` args
- `hazus_notinuse.py` is **NOT obsolete** despite its name — it is the active FAST execution engine called by `run_fast.py`
- `manage.py` is Windows-only (`ctypes.windll`); do not import on macOS/Linux
- FAST reads a field mapping JSON that maps its 15 internal keys (e.g. `UserDefinedFltyId`, `OCC`) to CSV column names
- Per building: raster depth at lat/lon → subtract `FirstFloorHt` → DDF lookup by occupancy type → damage %

### Scripts

| Script | Purpose |
|--------|---------|
| `scripts/duckdb_fast_pipeline.py` | **Primary pipeline**: NSI Parquet → FAST CSV via DuckDB SQL |
| `scripts/fast_e2e_from_oracle.py` | Legacy E2E pipeline (row-by-row Python) |
| `research/population_impact/scripts/01_county_damage_agg.py` | Event-level county aggregation of FAST predictions (pre-L/M/H) |
| `research/population_impact/scripts/02_fetch_census_svi.py` | Pull county ACS + SVI metrics for population impact pipeline |
| `research/population_impact/scripts/03_build_and_train.py` | Train ML damage model variant on FAST output |
| `scripts/download_nsi_by_state.py` | Download NSI from USACE API → GeoJSON → Parquet (state-by-state) |
| `scripts/nsi_raw_to_parquet.py` | Raw NSI GPKG/GeoJSON → Parquet conversion (DuckDB or geopandas engine) |
| `scripts/slosh_to_raster.py` | SLOSH Parquet → GeoTIFF (inundation = surge - topography) |
| `scripts/h3_spatial_index.py` | H3 hex pre-filtering: raster valid pixels → H3 cells → filter NSI buildings |
| `scripts/ml_damage_model.py` | ML alternative to FAST DDFs (LightGBM/XGBoost on FAST output) |
| `scripts/validate_pipeline.py` | Post-run validation: schema checks + aggregate stats on predictions CSV |
| `scripts/match_county_coverage_cloud.py` | County-level coverage analysis against ground truth |
| `scripts/deploy_to_instances.py` | Remote AWS node bootstrapping |
| `scripts/gen_cloudinit.py` | Generate cloud-init config for EC2 instances |
| `research/population_impact/scripts/04_classify_lmh.py` | Athena query: dedup + L/M/H zone classification + county agg |
| `research/population_impact/scripts/05_format_for_spreadsheet.py` | Census join + ARC conversion rates → planning output CSV/XLSX |
| `research/population_impact/scripts/06_validate_lmh.py` | Validation against ground truth (RMSE, MAE, R²) |

## Environment Setup

```bash
conda create -n arc-pipeline python=3.10 -y
conda activate arc-pipeline
pip install duckdb pyarrow pandas geopandas rasterio pypdf pyyaml h3
# For ML model: pip install lightgbm xgboost scikit-learn
```

Geospatial deps (rasterio/GDAL): always use `conda install conda-forge::rasterio` on Windows. Native pip works on macOS/Linux.

## Common Commands

```bash
# Download NSI data by state (API → GeoJSON → Parquet)
python scripts/download_nsi_by_state.py --state Florida --state Texas --engine duckdb --output-dir data

# Convert raw NSI file to processed parquet
python scripts/nsi_raw_to_parquet.py --input path/to/nsi.geojson --output path/to/out.parquet

# DuckDB pipeline (preferred)
python scripts/duckdb_fast_pipeline.py --state Florida

# Legacy E2E pipeline
python scripts/fast_e2e_from_oracle.py \
  --state-scope Florida --raster-name auto --config configs/fast_e2e.yaml

# SLOSH Parquet → GeoTIFF raster
python scripts/slosh_to_raster.py --basin ny3mom --category 3 --tide high

# H3 spatial pre-indexing
python scripts/h3_spatial_index.py --raster path/to/raster.tif --resolution 7

# Validate pipeline output
python scripts/validate_pipeline.py --predictions path/to/output.csv
```

### Testing

```bash
# Run all tests
python -m pytest tests/ -v

# Run a single test file
python -m pytest tests/test_download_nsi_by_state.py -v

# Run a single test function
python -m pytest tests/test_download_nsi_by_state.py::test_normalize_state_identifier_variants -v

# FAST CSV/Parquet parity test (requires GDAL + sample data in FAST-main/UDF/)
python -m pytest FAST-main/tests/test_csv_parquet_parity.py -v
```

Tests must be run from project root (`python -m pytest`) because `scripts/` has no `__init__.py` — the test files use `from scripts import ...` which relies on the project root being in `sys.path`.

### CI/CD

- **GitHub Actions**: `.github/workflows/download-nsi.yml` — manual dispatch workflow to download NSI by state, converts to parquet, uploads as artifact (90-day retention). Trigger via GitHub UI with comma-separated state list.

## Data Contracts

### NSI → FAST CSV Column Mapping

| NSI Field | FAST Column | Notes |
|-----------|-------------|-------|
| `bid` | `FltyId` | **Must deduplicate** across parquet files |
| `occtype` | `Occ` | e.g. RES1, COM1 |
| `val_struct` | `Cost` | Replacement cost ($) |
| `sqft` | `Area` | Floor area (sqft) |
| `num_story` | `NumStories` | Stories above ground |
| `found_type` | `FoundationType` | Numeric via `found_type_map` in config: Pier=2, Basement=4, Crawl=5, Slab=7 |
| `found_ht` | `FirstFloorHt` | Feet above grade |
| `latitude` / `longitude` | `Latitude` / `Longitude` | WGS84 |
| `val_cont` | `ContentCost` | Optional |

Full mapping also defined in AGENTS.md §3-4.

### NSI Parquet Target Schema

Defined in `scripts/nsi_raw_to_parquet.py:TARGET_SCHEMA` — 31 columns including population fields (`pop2pmu65`, `pop2pmo65`), census block FIPS (`cbfips`), and geometry (`x`, `y`, `longitude`, `latitude`). The `download_nsi_by_state.py` pipeline produces parquet partitioned as `processed/nsi/state={State_Name}/part-00000.snappy.parquet`.

### P-Surge Rasters

- Naming: `{EVENT}_{YEAR}_adv{N}_e10_ResultMaskRaster.tif`
- `e10` = 10% exceedance probability (upper-end planning level)
- 9 events: BERYL, DEBBY, FLORENCE, HELENE, IAN, IDALIA, IDA, MICHAEL, MILTON
- 3 advisories each, 27 rasters total, ~3.9M building-level predictions

### SLOSH → Raster (when rasterizing from source)

- Geometry: `geometry_wkt` | Surge: `cN_mean`/`cN_high` (N=0..5) | Terrain: `topography`
- Inundation depth = surge elevation - topography; output GeoTIFF in feet, NODATA=-9999

### FAST Runtime Parameters

- `flC`: `CoastalA` (default for storm surge) | `CoastalV` (high-risk) | `Riverine` (inland only)
- `raster`: path to `.tif` flood depth raster

## Configuration

- `configs/fast_e2e.yaml` — batch_size (65536), firmzone codes, foundation type mapping
- `configs/event_state_map.yaml` — hurricane → affected state routing (11 events configured)

## Known Issues

### 99.7% Zero-Loss Spatial Mismatch (CRITICAL)

Buildings with valid FIRM zones bypass bbox filter in `fast_e2e_from_oracle.py`, landing outside raster coverage. FAST returns depth=0 for out-of-bounds coords → inflated zero-loss. **Fix**: raster-aware spatial pre-filtering (H3 or bbox clip) on ALL buildings before FAST, regardless of firmzone.

### FltyId Deduplication (HIGH)

No dedup on `bid` across parquet files — duplicate FltyIds inflate damage totals. The DuckDB pipeline handles this via `ROW_NUMBER()`. The legacy E2E pipeline needs explicit `seen_bids: set` tracking.

### Partial FAST Output (MEDIUM)

`run_fast_job` checks returncode + file existence but not row count. Partial writes on crash pass the success check.

## Spatial Filtering Rules

1. `impact-only` mode: drop ALL buildings outside `raster_bbox_wgs84(raster_path)`
2. BBox is coarse; for irregular footprints use H3 hex or raster valid-pixel convex hull
3. Do NOT use FIRM zones as proxy for event footprint (FIRM = long-term risk; raster = event-specific)

## Conventions

- **Commit messages**: Conventional Commits — `feat:`, `fix:`, `docs:`, `chore:` etc.
- **Code style**: Python 3.10+, strict type hints, `black`/`ruff` formatting (line limit 120), `isort` for imports. Details in `conductor/code_styleguides/python_data.md` (mirrored at `docs/governance/code_styleguides/python_data.md`).
- **TDD**: Required for data transformation functions. Mock parquet payloads locally.
- **Execution contract**: AGENTS.md defines hard rules for agent behavior — follow it by default.
- **Governance docs**: `docs/governance/` tracks workflow, tech stack, product definition.

## Key Documentation

| Document | Path |
|----------|------|
| Agent execution contract | `AGENTS.md` |
| Pipeline flowchart (end-to-end) | `docs/pipeline_flowchart.md` |
| C4 architecture diagrams | `C4-Documentation/c4-*.md` |
| L/M/H implementation plan | `research/population_impact/IMPLEMENTATION_PLAN.md` |
| System manual | `docs/manual/system_manual.md` |
| Onboarding guide | `docs/wiki/zero_to_hero.md` |
| NSI data dictionary | `docs/data_dictionary/NSI_DATA_DICTIONARY_EN.md` |

## Next Steps (Active Roadmap)

### 1. Latest-Advisory Raster Selection (Timeliness over Maximum)

Current approach uses all 3 advisories per event. **New policy**: use only the **most recent (latest) advisory raster** for predictions, not the maximum or all advisories. Rationale:
- Latest advisory reflects the most up-to-date NHC forecast track and intensity
- Older advisories may predict surge in areas the storm no longer threatens
- Improves both timeliness and spatial accuracy of damage estimates

### 2. Census Tract Severity Classification via Building Damage

Replace the current storm-surge-depth-based intensity metric with a **building-damage-based severity classification** at the census tract level. Approach:
- Aggregate FAST `BldgDmgPct` per census tract (using NSI `cbfips` → tract FIPS)
- Classify tract severity: e.g. **High** if mean/median `BldgDmgPct` > 35% (threshold is configurable, derived from FAST output)
- This is more meaningful than raw surge depth because it accounts for building characteristics (foundation type, first floor height, occupancy)
- Output: census-tract-level severity map (Low / Medium / High) for Red Cross planning

## What NOT to Do

- Do not run GUI mode for production
- Do not import `manage.py` on macOS/Linux
- Do not use FIRM zone as spatial filter in `impact-only` mode
- Do not skip FltyId deduplication
- Do not expand scope beyond what is requested
- Do not ask questions answered by AGENTS.md or this file
