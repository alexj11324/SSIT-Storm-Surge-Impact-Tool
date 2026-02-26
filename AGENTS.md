# ARC Capstone Agent Execution Contract

This file defines hard execution rules for agents working in this repository.
Follow these rules by default unless the user explicitly overrides them.

## 1. Project Goal (Do Not Drift)

Primary production path:

1. Use NSI processed data to build FAST-ready building inventory CSV.
2. Use SLOSH processed data to produce flood depth raster (`.tif`).
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

## 4. NSI/SLOSH Source Mapping Rules

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

### 4.2 SLOSH -> Raster

Use SLOSH fields to build raster; FAST does not consume SLOSH parquet directly:

1. Geometry: `geometry_wkt`
2. Surge scenario value: one of `cN_mean` / `cN_high` (N=0..5)
3. Terrain adjustment as needed: `topography`

Output must be GeoTIFF (`.tif`) in feet.

## 5. Default Hazard Choice Policy

For coastal storm-surge workflows in this repo:

1. Default baseline: `CoastalA`
2. Sensitivity/high-risk run: `CoastalV`
3. Use `Riverine` only for inland/riverine tasks

If user does not specify and the task is SLOSH-driven, use `CoastalA`.

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
3. Do not expand scope to OCI/DB refactors unless user asks.
4. Keep changes focused on NSI -> FAST CSV, SLOSH -> raster, and FAST execution.

## 9. Cloud Data Processing Policy

1. All cloud-source data (for example S3, Athena, Oracle Object Storage) must be processed in cloud services.
2. Do not download cloud data to local environments for core cleaning, joins, spatial analysis, or aggregations.
3. Local environments may only be used to submit cloud jobs, inspect logs, and read small final summaries.
4. Intermediate and final artifacts derived from cloud data must be written back to cloud storage and remain queryable.
5. Any exception requires explicit user approval in the current task.
