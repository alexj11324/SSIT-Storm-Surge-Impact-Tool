# Python / Data Pipeline Style Guide

1. **Typing & Signatures**: Use strict Python type hints for all function signatures. Example: `def load_slosh_raster(path: str) -> np.ndarray:`
2. **Docstrings**: Use NumPy or Google style docstrings for all core pipeline functions to document behavior, especially geometry expectations (e.g. projecting to EPSG 4326 vs 3857).
3. **DataFrames**: When mutating large pandas/geopandas DataFrames or DuckDB cursors, prefer immutable transformations or explicitly state side-effects. Use descriptive column names matching the Data Dictionary.
4. **Environment Variables**: Stricly isolate AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, etc.) in `.env` files. Ensure they are loaded dynamically and NEVER hardcoded in `configs/` or python code.
5. **Linting & Formatting**: Use `ruff format` (line-length 120) and `ruff check --fix` for linting. Ruff replaces Black + isort + flake8. Auto-enforced via PostToolUse hook in `.claude/settings.json`.
