# Scripts Directory

Place project-specific automation and data-processing scripts here.

## Guidelines

- Keep executable source files (for example, `*.py`, `*.sh`) in this folder.
- Do not commit bytecode or cache outputs (`__pycache__`, `*.pyc`).
- Write scripts so output paths are configurable and default to `exports/`.
- Prefer explicit, descriptive names such as `fast_e2e_from_oracle.py`.

## NSI API Download

Use [`download_nsi_by_state.py`](/Users/alexjiang/Library/Mobile%20Documents/com~apple~CloudDocs/CMU/course/Capstone/ARC_Capstone/scripts/download_nsi_by_state.py) to download NSI building inventory by state through the official USACE API and convert the result to the project's processed parquet layout.

Examples:

```bash
python scripts/download_nsi_by_state.py --state Florida
python scripts/download_nsi_by_state.py --state FL --state Georgia
python scripts/download_nsi_by_state.py --state 12 --engine geopandas
python scripts/download_nsi_by_state.py --state FL --engine duckdb --timeout 600
python scripts/download_nsi_by_state.py --state FL --output-dir data
```

Default output layout:

```text
exports/nsi_downloads/<timestamp>/
├── manifest.json
├── raw/
│   └── nsi_2022_<fips>_<State>.geojson
└── processed/
    └── nsi/
        └── state=<State>/
            └── part-00000.snappy.parquet
```

Notes:

- Large states are safer with `--engine duckdb`; the `geopandas` fallback loads the full GeoJSON into memory.
- For a stable downstream parquet glob such as `data/processed/nsi/state=Florida/*.parquet`, use `--output-dir data` instead of the default timestamped run directory.
