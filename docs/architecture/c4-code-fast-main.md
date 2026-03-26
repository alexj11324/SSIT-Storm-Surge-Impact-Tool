# C4 Code-Level Documentation: FAST-main/

## 1. Overview Section
- **Name**: FEMA FAST Engine Wrapper
- **Description**: Headless execution wrapper around FEMA's proprietary Flood Assessment Structure Tool (FAST).
- **Location**: [`FAST-main/`](./FAST-main/)
- **Language**: Python
- **Purpose**: Accepts correctly formatted building footprints with water depths and churns through engineering damage curves to yield `% Damage` and `$ Loss`.

## 2. Code Elements Section

### Core Execution Moduler (`Python_env/run_fast.py`)
- **Signature**: `run_fast(input_csv: str, output_csv: str, parameters: dict) -> bool`
- **Description**: CLI accessible wrapper. Initializes FAST configurations, applies the built-in vulnerability curves against provided structures (`Cost`, `FirstFloorHt`), and spits out structural damage.
- **Dependencies**: Native FAST libraries.

## 3. Dependencies Section
- **Internal Dependencies**: Inherently relies on the precise input format dictated by extraction from `scripts/fast_e2e_from_oracle.py`.
- **External Dependencies**: FEMA damage tables, internal python geometry bindings, `pandas`.

## 4. Relationships Section
```mermaid
graph LR
    A[duckdb_fast_pipeline.py] -->|Writes fast_input.csv| B(FAST: run_fast.py)
    B -->|Outputs predictions.csv| C[AWS S3 Export]
```
