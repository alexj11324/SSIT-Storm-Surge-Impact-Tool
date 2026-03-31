# ARC Capstone Agent Execution Contract

This file defines hard execution rules for agents working in this repository.
Follow these rules by default unless the user explicitly overrides them.

## 1. Project Goal (Do Not Drift)

Primary production path:

1. Use NSI processed data to build FAST-ready building inventory CSV.
2. Use NHC P-Surge rasters as flood depth input (`.tif`).
3. Run FAST headless (no GUI) and generate FAST output CSV artifacts.

Do not introduce unrelated architecture changes unless requested.

## 2. Default Runtime Path (Authoritative)

Use headless entrypoint:

- `FAST-main/Python_env/run_fast.py`

Do not require GUI for production runs.

## 3. FAST Input Contract (Production)

### 3.1 Required building columns in FAST CSV

Keep and populate these columns:

1. `FltyId` (or mapped to `UserDefinedFltyId`)
2. `Occ`
3. `Cost`
4. `Area`
5. `NumStories`
6. `FoundationType`
7. `FirstFloorHt`
8. `Latitude`
9. `Longitude`

### 3.2 Optional columns (can be omitted)

1. `ContentCost`
2. `BDDF_ID`
3. `CDDF_ID`
4. `IDDF_ID`
5. `InvCost`
6. `SOID`

### 3.3 Non-column runtime parameters (still required at run time)

1. `flC` (`Riverine` / `CoastalA` / `CoastalV`)
2. `raster` (`.tif` path/name)

## 4. NSI Source Mapping Rules

### 4.1 NSI -> FAST CSV (canonical mapping)

1. `bid` -> `FltyId`
2. `occtype` -> `Occ`
3. `val_struct` -> `Cost`
4. `sqft` -> `Area`
5. `num_story` -> `NumStories`
6. `found_type` -> `FoundationType` (must be normalized to FAST-expected numeric code)
7. `found_ht` -> `FirstFloorHt`
8. `latitude` -> `Latitude`
9. `longitude` -> `Longitude`
10. `val_cont` -> `ContentCost` (optional)

### 4.2 Flood Depth Raster

NHC P-Surge inundation rasters are downloaded via `scripts/import_nhc_by_storm.py` and stored in `FAST-main/rasters/`. Format: GeoTIFF (`.tif`) in feet.

## 5. Default Hazard Choice Policy

For coastal storm-surge workflows in this repo:

1. Default baseline: `CoastalA`
2. Sensitivity/high-risk run: `CoastalV`
3. Use `Riverine` only for inland/riverine tasks

If user does not specify and the task is storm-surge-driven, use `CoastalA`.

## 6. Execution Behavior Rules (No Low-Value Questions)

Do not ask obvious or repetitive questions when the repository context already answers them.
Instead, proceed using defaults and document assumptions briefly.

### 6.1 Questions that should NOT be asked

1. Asking whether FAST needs raster (it does).
2. Asking whether `flC` is required at run time (it is).
3. Asking for fields already defined in this contract.
4. Asking whether to use GUI for production runs (do not).

### 6.2 When questions ARE allowed

Ask only when blocked by missing irrecoverable inputs, such as:

1. Missing target region or time/event scope needed to pick source partitions.
2. Missing raster scenario selection when multiple outputs are explicitly required.
3. Missing credentials/access needed to read data sources.

When asking, provide exactly what is missing and a recommended default.

## 7. Output Standards

1. Prefer deterministic, reproducible scripts and commands.
2. Keep production data schemas explicit.
3. Avoid introducing optional complexity unless requested.
4. Summarize results with concrete artifacts and paths.

## 8. Guardrails

1. Do not silently alter business assumptions.
2. Do not switch data model without explicit request.
3. Do not expand scope to infrastructure refactors unless user asks.
4. Keep changes focused on NSI -> FAST CSV, raster acquisition, and FAST execution.

## 9. Data Processing Policy

1. Primary execution environment: local Python or Google Colab.
2. NSI data is downloaded via the project's NSI download workflow and stored as local Parquet files.
3. NHC P-Surge rasters are downloaded directly from NHC and stored in `FAST-main/rasters/`.
4. Pipeline execution uses `scripts/duckdb_fast_pipeline.py` (DuckDB SQL) for all data transformation.
5. Shelter demand analysis runs in Google Colab via `notebooks/shelter_demand.ipynb`.

## Learned User Preferences

- Prefer cell-level notebook editing (for example `EditNotebook`) over rewriting `.ipynb` via shell; if the user forbids bash for a notebook change, do not use shell to edit the notebook.
- For `notebooks/shelter_demand.ipynb`, use `tqdm` with speed or rate display (for example postfix MB/s or it/s) on every download and streaming-fetch code path, not only a subset of cells.
- For `notebooks/shelter_demand.ipynb`, when static checkers flag cross-cell variables, prefer passing data via small artifacts under `WORK_DIR` (for example a join CSV written after the upstream cell) instead of referencing in-memory variables only defined in earlier cells.

## Learned Workspace Facts

- Team-processed NSI/SLOSH may live in S3 as partitioned Parquet; the Athena/Glue catalog can expose more than one SLOSH table with different schemas—confirm the authoritative table and prefix before building joins (see `docs/` and slide materials for bucket paths).
- US state abbreviations and related metadata live in `scripts/us_states.py` (`STATE_BY_ABBR`, `STATE_SPECS`, and related maps); scripts should import from there instead of duplicating state lists.
- In `notebooks/shelter_demand.ipynb`, `from scripts.*` requires the repository root on `sys.path` early (for example in the first setup cell). If the layout has no `repo/` clone or the kernel cwd is under `notebooks/`, resolve the project root so `scripts/` is discoverable (for example via `Path.cwd()` or parent directories), not only `notebooks/repo`.
