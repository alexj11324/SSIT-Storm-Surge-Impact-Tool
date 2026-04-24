# Immediate Tsunami and Storm Surge Population Impact Modeling

Property-level storm surge/tsunami impact modeling using FEMA's FAST tool, USACE National Structure Inventory (30M+ buildings), and NOAA storm surge models. Estimates building damage, displaced population, and high-need populations to inform Red Cross shelter and casework planning.

## Architecture

```
Stage 1-2: Damage Prediction
  NSI Parquet --> DuckDB: clean/filter/dedup/map --> FAST CSV -+
  NHC P-Surge GeoTIFF (.tif) --------------------------------+-> FAST engine -> predictions.csv

Stage 3-5: Shelter Demand + Export (Colab: notebooks/shelter_demand.ipynb)
  predictions.csv --> L/M/H classification (tract level) --> BHI × Census × SVI --> shelter demand
  shelter demand --> CSV/XLSX deliverables
```

See `docs/e2e_pipeline.md` for the full Mermaid diagram.

## Prerequisites

- Python 3.10+
- FAST engine (`FAST-main/Python_env/run_fast.py`)

```bash
pip install -e '.[dev]'
```

## Quick Start

```bash
# Shelter demand pipeline (Google Colab)
# Open notebooks/shelter_demand.ipynb in Colab — the primary ARC deliverable.
# The notebook reads the Excel interface, downloads the NHC raster, infers
# affected states, loads NSI, runs FAST, and exports CSV/XLSX deliverables.

# Optional local FAST-input build only (DuckDB SQL → FAST CSV)
python scripts/duckdb_fast_pipeline.py \
  --parquet-glob "nsi/state=FL/*.parquet" \
  --raster FAST-main/rasters/IAN_2022_adv33_e10_ResultMaskRaster.tif \
  --output outputs/fast_input.csv
```

## Command Reference

| Script | Description | Key Flags |
|--------|-------------|-----------|
| `duckdb_fast_pipeline.py` | NSI Parquet → FAST CSV (primary pipeline) | `--parquet-glob`, `--raster`, `--output` (all required) |
| `run_fast.py` | Run FAST engine headless | `--inventory`, `--mapping-json`, `--flc`, `--rasters` (all required), `--pretty` |
| `download_nsi_by_state.py` | Download NSI from USACE API | `--state` (repeatable), `--output-dir`, `--engine {duckdb,geopandas}` |
| `nsi_downloader.py` | NSI download client (USACE API + HuggingFace) | Used as library by notebook and other scripts |
| `nsi_raw_to_parquet.py` | Convert raw NSI to Parquet | `--input`, `--output`, `--engine {duckdb,geopandas}` |
| `import_nhc_by_storm.py` | Download NHC P-Surge rasters and infer affected states | Used by notebook: `download_surge_raster(storm_id, storm_name, advisory, year)` |
| `upload_nsi_to_hf.py` | Upload NSI to Hugging Face | `--repo-id`, `--parquet-dir`, `--download-all` |
| `read_excel_config.py` | Load config from Excel interface | Used as library: `load_config_from_excel(path)` |
| `us_states.py` | State FIPS, abbreviations, API URLs | Used as library: `from scripts.us_states import STATE_BY_ABBR` |

## Project Structure

```
scripts/
  duckdb_fast_pipeline.py       # Primary pipeline: NSI Parquet -> FAST CSV
  download_nsi_by_state.py      # Download NSI from USACE API -> Parquet
  nsi_downloader.py             # NSI download client (USACE API + HuggingFace)
  nsi_raw_to_parquet.py         # Raw NSI GPKG/GeoJSON -> Parquet
  import_nhc_by_storm.py        # Download NHC P-Surge rasters
  upload_nsi_to_hf.py           # Upload NSI Parquet to Hugging Face Hub
  read_excel_config.py          # Excel config loader (Interface sheet)
  us_states.py                  # State metadata: FIPS, abbreviations, API URLs
tests/
  conftest.py                   # Shared pytest fixtures
  test_download_nsi_by_state.py # NSI download tests
  test_import_nhc_by_storm.py   # NHC import tests
  test_duckdb_fast_pipeline.py  # DuckDB pipeline tests
  test_read_excel_config.py     # Excel config parsing tests
  test_shelter_demand_notebook.py # Notebook validation tests
notebooks/
  shelter_demand.ipynb          # BHI shelter demand E2E pipeline (Colab) — primary ARC deliverable
data/
  ARC Storm Surge Shelter Demand.xlsx  # Excel config interface (storm params, thresholds, BHI)
  Ground Truth Data.xlsx               # Historical ARC records used for reference/calibration
docs/
  e2e_pipeline.md               # End-to-end pipeline architecture (Mermaid)
  DIRECTION.md                  # ML→L/M/H pivot rationale and ARC methodology
  nsi_data_dictionary.md        # NSI field definitions (EN/ZH)
  wiki/                         # Onboarding guides (zero_to_hero, principal_guide)
  manual/                       # System manual
  governance/                   # Product definition
  archive/                      # Archived docs (reflection, slides_data_sources)
FAST-main/
  Python_env/run_fast.py        # FAST headless engine (production)
  Lookuptables/                 # FEMA depth-damage functions
  rasters/                      # Pre-downloaded flood rasters
```

## Data Sources

| Source | Description | Format |
|--------|-------------|--------|
| NSI | USACE National Structure Inventory 2022 (30M+ buildings); notebook defaults to preprocessed HuggingFace Parquet with USACE API fallback | Parquet, partitioned by state |
| NHC P-Surge | NOAA storm surge inundation depth rasters | GeoTIFF (depth in feet) |
| Census ACS | Population by census tract | API (data.census.gov) |
| SVI | CDC Social Vulnerability Index | Census tract level |
| Ground Truth | ARC operational records (9 hurricanes 2018-2024) | Excel |

## L/M/H Intensity Classification

Building damage is classified into intensity zones per ARC's Mass Care Planning framework (see `docs/DIRECTION.md`):

| Level | BldgDmgPct | Surge Depth | ARC Planning Factor |
|-------|-----------|-------------|---------------------|
| **High** | >35% destroyed | >12 ft | 5% of affected pop |
| **Medium** | 11-34% destroyed | 9-12 ft | 3% of affected pop |
| **Low** | 0-10% destroyed | 4-8 ft | 1% of affected pop |

The **Building Habitability Index (BHI)** combines damage state fractions with utility disruption factors to estimate the fraction of residents who will seek shelter. BHI is computed per census tract in `notebooks/shelter_demand.ipynb`.

## Prediction Results

Results for 9 hurricane events × 3 advisories (27 runs, ~3.9M building predictions):

| Event | Advisories | Buildings | Notes |
|-------|-----------|-----------|-------|
| BERYL_2024 | 39, 40, 41 | ~107K each | TX/LA Gulf Coast |
| DEBBY_2024 | 18, 19, 20 | ~103K each | FL/GA/NC/SC/VA |
| FLORENCE_2018 | 63, 64, 65 | 17K-32K | NC/SC/VA Atlantic |
| HELENE_2024 | 14, 15, 16 | 240K-475K | FL/GA/NC/SC |
| IAN_2022 | 31, 32, 33 | ~119K-122K | FL/NC/SC |
| IDALIA_2023 | 18, 19, 20 | 62K-124K | FL/GA/SC |
| IDA_2021 | 16, 17, 18 | ~412K each | AL/LA/MS |
| MICHAEL_2018 | 20, 21, 22 | ~900 each | Coastal GA (small raster footprint) |
| MILTON_2024 | 20, 21, 22 | 70K-208K | FL |

### Output Column Reference

**Building Attributes**

| Column | Description |
|--------|-------------|
| `FltyId` | NSI unique building ID |
| `Occ` | Occupancy type (RES1=single-family, RES3=multi-family, COM1=commercial) |
| `Cost` | Replacement cost ($) |
| `Area` | Floor area (sqft) |
| `NumStories` | Stories above ground |
| `FoundationType` | 2=Pier, 4=Basement, 5=Crawlspace, 7=Slab |
| `FirstFloorHt` | First floor height above grade (ft) |
| `Latitude` / `Longitude` | WGS84 coordinates |
| `state` | State name |

**Flood Depth**

| Column | Description |
|--------|-------------|
| `Depth_Grid` | Surge depth from P-Surge raster at building location (ft) |
| `Depth_in_Struc` | Effective depth inside structure = Depth_Grid - FirstFloorHt (ft) |

**Damage & Loss**

| Column | Description |
|--------|-------------|
| `BldgDmgPct` | Structural damage percentage (%) |
| `BldgLossUSD` | Structural loss ($) |
| `ContentCost` | Contents replacement value ($) |
| `ContDmgPct` | Contents damage percentage (%) |
| `ContentLossUSD` | Contents loss ($) |
| `InventoryLossUSD` | Inventory loss ($ — commercial buildings) |

**Debris & Recovery**

| Column | Description |
|--------|-------------|
| `Debris_Fin` | Finish debris (tons) |
| `Debris_Struc` | Structural debris (tons) |
| `Debris_Found` | Foundation debris (tons) |
| `Debris_Tot` | Total debris (tons) |
| `Restor_Days_Min` / `Restor_Days_Max` | Estimated restoration days (range) |

**Partition & Provenance**

| Column | Description |
|--------|-------------|
| `event` | Hurricane event slug |
| `adv` | Advisory number |
| `raster_name` | Source raster filename |
| `run_id` | Pipeline run ID (timestamp-based) |
| `flc` | Flood class: CoastalA / CoastalV / Riverine |

## Linting

```bash
ruff check scripts/ tests/          # lint
ruff format scripts/ tests/         # auto-format
```

Config in `pyproject.toml` (E/F/W/I rules, line-length 100, Python 3.10+).

## Key Documentation

| File | Purpose |
|------|---------|
| `CLAUDE.md` | AI agent instructions, critical gotchas, module relationships |
| `AGENTS.md` | Execution contract, data contracts, guardrails |
| `docs/e2e_pipeline.md` | End-to-end pipeline architecture with BHI model (Mermaid) |
| `docs/DIRECTION.md` | ML→L/M/H pivot rationale and ARC methodology alignment |
| `docs/nsi_data_dictionary.md` | NSI field definitions (English + Chinese) |
| `docs/wiki/` | Onboarding guides (zero_to_hero, principal_guide) |

---

## Team

CMU Heinz College — Master of Science in Public Policy and Management, 2026
