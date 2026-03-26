# Tech Stack

## Programming Languages
- **Python 3.10+**: Core logic, pipeline orchestration, data science libraries, and LLM API integrations.

## Core Computing & Data Science Stack
- **DuckDB**: For fast, in-process analytical querying over large Parquet files.
- **PyArrow**: For high-performance read/write of partitioning Parquet structures.
- **Rasterio & GeoPandas**: For manipulating raster data (SLOSH GeoTIFFs) and geometric joining.
- **FEMA FAST Engine**: Headless mode processing via `run_fast.py`.

## Database & Cloud Infrastructure
- **Storage**: AWS S3 (for resulting pipelines/models and processed inputs).
- **Compute/Query**: AWS Athena for ad-hoc distributed querying over processed S3 datasets.
- **Execution Layer**: AWS Cloud and Python concurrent pipelines orchestrated via shell (e.g., `launch_cloud_parallel.sh`).

## Architectural Constraints
- Maintain data consistency within the `parquet` format for large table storage.
- Avoid introducing heavy ORMs or DB servers unless strictly necessary; prefer stateless containerized scripts wrapping DuckDB + S3 inputs.
