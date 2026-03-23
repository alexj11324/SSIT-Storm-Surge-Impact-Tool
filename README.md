# Immediate Tsunami and Storm Surge Population Impact Modeling

A rapid, programmatic geospatial data pipeline bridging predictive hurricane water models with FEMA's deep structural damage index. Designed to equip the American Red Cross and mass-care planners with data-driven shelter placement estimates within hours of severe weather advisories, without the latency of relational database bottlenecks.

## Key Features

- **No-DB "Storage-as-Compute" Engine**: Exclusively pivots around `DuckDB` pulling S3 `.parquet` objects, entirely side-stepping SQL Database IOPS bottlenecks.
- **Headless FEMA Assessment**: Hooks into FEMA's native FAST python core to probabilistically assess the physical damage percentage on millions of granular structures. 
- **Geospatial Parallelism**: Sub-divides NOAA's SLOSH `.tif` warning grids and maps them identically onto USACE's National Structure Inventory (NSI) to produce real-time county-by-county impact analytics.

---

## Tech Stack

- **Language**: Python 3.10+
- **Core Architecture Framework**: `duckdb` for in-memory analytics; `pyarrow` for columnar format streaming.
- **Geodata Tooling**: `rasterio` (for TIFF grids), `geopandas` (for vector boundaries), `h3` (for hex grids).
- **Physical Model**: FEMA Flood Assessment Structure Tool (FAST) headless engine.
- **Infrastructure**: AWS S3 (Blobs), AWS EC2 Spot Instances (Batch Worker Nodes), AWS Athena.

---

## Prerequisites

- **Python**: Version 3.10+ (via pyenv or native)
- **Conda/Mamba**: For clean distribution of GIS wheels (especially GDAL/Rasterio)
- **AWS CLI**: Pre-configured environment containing valid IAM roles (`aws configure`).

---

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/alexj11324/ARC_Capstone.git
cd ARC_Capstone
```

### 2. Setup the Python Environment

Due to native C-bindings used by geospatial tooling, installing pure headers via Conda is recommended over native Pip.

```bash
conda create -n arc-pipeline python=3.10 -y
conda activate arc-pipeline
pip install duckdb pyarrow pandas geopandas rasterio pypdf pyyaml h3
```

### 3. Execution Setup Matrix

The base execution revolves around NOAA models. Ensure you configure your states inside the YAML system.

```bash
# Modify routing behaviors here
nano configs/event_state_map.yaml
```

### 4. Running Pipeline Modules Locally

*Run Core Damage Engine (preferred — DuckDB pipeline):*
```bash
python scripts/duckdb_fast_pipeline.py --state Florida
```

*Run Core Damage Engine (legacy — row-by-row):*
```bash
python scripts/fast_e2e_from_oracle.py --state-scope Florida --raster-name auto --config configs/fast_e2e.yaml
```

> **Note**: `slosh_to_raster.py` is legacy. The active pipeline uses NHC P-Surge GeoTIFF rasters downloaded directly from NHC — no conversion needed.

---

## Architecture Overview

This section is for engineers inheriting the pipeline. Instead of relying on a multi-hour RDBMS spatial query, we rely on decoupled Bash nodes interacting seamlessly via DuckDB.

### Directory Structure

```text
ARC_Capstone/
├── C4-Documentation/        # Granular Architectural Diagrams (Component, Context)
├── conductor/               # Internal tracking status of tracks, configurations, and strict guidelines
├── configs/                 # YAML Event Router and end-to-end execution constants
├── docs/                    
│   ├── manual/              # Long-form system manual 
│   └── wiki/                # Deep Principals guide & Onboarding tutorials
├── FAST-main/               # Embedded FEMA FAST Assessment core module
└── scripts/                 # Core Pipeline operations 
    ├── duckdb_fast_pipeline.py    # Spatial intersections and logic orchestration
    ├── fast_e2e_from_oracle.py    # Legacy-named main trigger route
    ├── slosh_to_raster.py         # Sub-process building GeoTIFF from points
    ├── deploy_to_instances.py     # Remote AWS node bootstrapping
    └── launch_cloud_parallel.sh   # Ephemeral execution trigger
```

### Data Flow Execution

1. `SLOSH warning` is caught and converted to a bounding box raster map by `slosh_to_raster`.
2. Python uses `PyArrow` to sweep the `NSI_Baseline.parquet` (AWS S3 source).
3. `duckdb` memory engines are initialized; a query executes `ST_MakeValid` geometric bounding matching on both components inside memory.
4. Results (water heights at coordinates) are flattened and dumped as `fast_input.csv` down to the local file system.
5. The `FEMA FAST` sub-shell is invoked, reading the structures, comparing it with static building `Cost` tables, and producing a metric called `BldgDmgPct`.
6. Result mapped back to S3 for Red Cross operators to query in Athena.

---

## Environment Variables

For security bounds, do NOT commit actual AWS keys into the version log or YAML definitions!

### Required System Environments

| Variable | Description |
| --- | --- |
| `AWS_ACCESS_KEY_ID` | User IAM to read NSI baseline objects |
| `AWS_SECRET_ACCESS_KEY` | User IAM password key |
| `AWS_DEFAULT_REGION` | Usually `us-east-1` or `us-west-2` |

---

## Available Scripts

| Tool/Command | Responsibility |
| --- | --- |
| `python scripts/fast_e2e_from_oracle.py` | Runs End-to-End local batch node. |
| `bash scripts/launch_cloud_parallel.sh` | Orchestrates remote spinning via Boto3/CLI. |
| `bash scripts/monitor_parallel.sh` | Hooks into EC2/Batch lifecycle stream and surfaces active outputs to host. |
| `bash scripts/terminate_parallel.sh` | Safety parachute destroying active run queues. |

---

## Pipeline 2: L/M/H Population Impact

After Pipeline 1 (FAST Damage Engine) produces building-level damage predictions, Pipeline 2 classifies them into **Low / Medium / High intensity zones** and aggregates to county-level population estimates for Red Cross mass care planning.

### Data Flow

```
FAST Building Predictions (Athena)
  → Dedup across advisories (MAX damage per building)
  → Classify intensity zone per building (surge depth + damage %)
  → Spatial join to county (ST_CONTAINS)
  → County × zone aggregation
  → Census population join
  → SVI join + conditional bump (HIGH zone only)
  → ARC conversion rates → shelter / feeding estimates
```

### Intensity Zone Classification

Each building is classified based on surge depth (primary) with damage % fallback:

| Zone | Surge Depth | Damage % (fallback) |
|------|-------------|---------------------|
| **HIGH** | > 12 ft | > 35% |
| **MEDIUM** | 9–12 ft | 15–35% |
| **LOW** | 4–8 ft | > 0% |

Source: ARC Mass Care Planning Assumptions Job Tool V.6.0, Figures 9–10.

### ARC Conversion Rates

| Impact Zone | Shelter % | Feeding % |
|-------------|-----------|-----------|
| HIGH | 5.0% | 12.0% |
| MEDIUM | 3.0% | 7.0% |
| LOW | 1.0% | 3.0% |

### Scripts

| Script | Purpose |
|--------|---------|
| `research/population_impact/scripts/04_classify_lmh.py` | Athena query: dedup → L/M/H classification → county aggregation |
| `research/population_impact/scripts/05_format_for_spreadsheet.py` | Census join + SVI join + SVI bump + ARC rates → CSV/Excel |
| `research/population_impact/scripts/06_validate_lmh.py` | Validation against ground truth (RMSE, MAE, R²) |

### Running Pipeline 2

```bash
cd research/population_impact/scripts

# Step 1: Classify and aggregate (requires AWS credentials for Athena)
python 04_classify_lmh.py --output-dir ../data

# Step 2: Format for ARC spreadsheet (SVI bump enabled by default)
python 05_format_for_spreadsheet.py \
  --input ../data/county_lmh_features.csv \
  --census ../data/census_county_population.csv \
  --output-dir ../outputs

# Step 3: Validate against ground truth
python 06_validate_lmh.py
```

### Output

- `planning_assumptions_output.csv` — county-level L/M/H population estimates
- `arc_planning_template_lmh.xlsx` — Excel with Estimates + Parameters sheets
- `lmh_validation_report.md` — accuracy metrics vs historical events

Full architecture diagram: [`docs/architecture/e2e_pipeline.md`](docs/architecture/e2e_pipeline.md)

---

## SVI (Social Vulnerability Index) Adjustment

The population impact pipeline applies a **conditional SVI bump** to HIGH intensity zones. Counties with higher social vulnerability (elderly, low-income, minority, housing-insecure populations) generate disproportionately higher demand for Red Cross services when severely impacted.

### How it works

```
pop_impacted_high_adjusted = pop_impacted_high × (1 + SVI_BUMP_WEIGHT × svi_score)
```

- **`svi_score`**: CDC SVI 2022 county-level overall percentile (`RPL_THEMES`, range 0–1). Downloaded automatically from CDC on first run.
- **`SVI_BUMP_WEIGHT`**: Default **0.20** — a county with SVI=1.0 (most vulnerable) gets a 20% uplift on HIGH zone estimates; SVI=0 gets no bump.
- The bump only applies to **HIGH** intensity zones. Medium and Low zones are unchanged.

### Tuning the default

> **The default weight of 0.20 is a starting point and should be calibrated against ground truth data.** Run `06_validate_lmh.py` with different `--svi-bump-weight` values and compare RMSE/MAE against historical events to find the optimal weight for your planning region.

```bash
# Default (20% max bump)
python 05_format_for_spreadsheet.py

# More aggressive bump (30%)
python 05_format_for_spreadsheet.py --svi-bump-weight 0.30

# Disable SVI adjustment entirely
python 05_format_for_spreadsheet.py --no-svi
```

| SVI Score | Bump (weight=0.20) | Bump (weight=0.30) |
|-----------|-------------------|-------------------|
| 0.0 | +0% | +0% |
| 0.25 | +5% | +7.5% |
| 0.50 | +10% | +15% |
| 0.75 | +15% | +22.5% |
| 1.00 | +20% | +30% |

Data source: [CDC/ATSDR SVI 2022](https://www.atsdr.cdc.gov/placeandhealth/svi/index.html)

---

## Testing

*(Governed by Strict TDD under Conductor Rules)*

Execute all local unit tests (if configured via `pytest`) prior to PR generation, specifically mocking the `aws_s3_read()` logic utilizing mock parquet stubs locally to verify matrix offsets independent of cloud uptime.

```bash
# E.g.
pytest tests/
```

---

## Deployment

The system is deployed purely as ephemeral functions without long-standing web servers.

### AWS Spot Execution (Production Pattern)

To operate over 30 states, do not trigger entirely locally:

```bash
# Deploys Python environment onto nodes, triggers intersections concurrently 
bash scripts/launch_cloud_parallel.sh --regions us-east-1 --max-nodes 10
```

### AWS Athena (Final Serving)

Instead of a DB connection string, Red Cross analysts configure Excel using AWS ODBC Athena Plugin targeting the output S3 bucket defined in `configs/fast_e2e.yaml`.

---

## Troubleshooting

### Spatial Dependency Errors

**Error**: `ModuleNotFoundError: No module named 'rasterio._base'` or GDAL conflict.
**Solution**: Never try to pip install rasterio on Windows cleanly. Use conda exclusively: `conda install conda-forge::rasterio`. 

### FAST Engine Fails to Evaluate

**Error**: `ValueError: Missing required FAST column 'FirstFloorHt'`
**Solution**: Occurs if the DuckDB extraction query drops or renames NSI columns. Review `duckdb_fast_pipeline.py` schema output map and cross reference against expected headers inside `FAST-main/run_fast.py`.

### S3 Permission Denied
**Error**: `ArrowIOError: AWS Error ACCESS_DENIED`
**Solution**: Verify your AWS CLI is authenticated (`aws sts get-caller-identity`). Re-authenticate your SSO/MFA payload if expired.
