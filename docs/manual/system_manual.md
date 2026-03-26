# ARC Capstone: Comprehensive System Manual

## 1. Executive Summary
The Immediate Tsunami and Storm Surge Population Impact Modeling system is a Python and Cloud-based data pipeline built for the American Red Cross. It ingests National Structure Inventory (NSI) footprint data and NOAA Sea, Lake, and Overland Surges from Hurricanes (SLOSH) warnings to deduce probabilistic storm damages on residential and commercial structures. With results processed through FEMA's FAST engine, emergency coordinators can immediately assess and allocate mass care shelter resources prior to or directly after landfall.

## 2. Core Architecture Philosophy
To achieve rapid iteration and avoid the classic GIS latency traps of large SQL JOINs, the architecture relies on **storage-as-compute**:
- **DuckDB**: Replaces traditional database environments to load partitioned `.parquet` payloads directly from disk/blob memory. 
- **Decoupled Engine**: The FEMA FAST utility operates as a black-box local shell execution, receiving structural damage states natively formatted via our pipeline.

*For rigorous architectural diagrams and interface specs, refer to our [C4 Context & Container documentation](../architecture/).*

## 3. High-Level Data Flow
1. **Trigger Phase**: The pipeline initiates via configuration mapping storms to geographical basins.
2. **Raster Acquisition**: P-Surge GeoTIFF rasters are downloaded directly from NHC (via `import_nhc_by_storm.py` or manually). The legacy `slosh_to_raster.py` is no longer used.
3. **Intersection**: `duckdb_fast_pipeline.py` leverages DuckDB to merge P-Surge flooding grids with NSI structure locations.
4. **Scoring**: Valid matching footprints (structures with >0 water depth) are sent through `FAST-main/Python_env/run_fast.py` to yield `% Damage`.
5. **Analytics**: The results are exported to S3 Parquet, acting as tables for an encompassing AWS Athena engine query interface.

## 4. Engineering Guidelines
- **Language**: Strictly Python 3.10+
- **Version Control**: Conventional Commits are aggressively enforced. Use `feat:`, `fix:`, `docs:`.
- **Testing**: Test-Driven Development (TDD) is mandated for algorithmic units, notably boundary conditions when joining structures near the coast.
- **Formatting**: Run `ruff format` (line-length 120) and `ruff check --fix` prior to PR submissions. Detailed code conventions exist in `../governance/code_styleguides/python_data.md`.

## 5. Deployment and Operation
As an ephemeral cloud pipeline:
- No REST APIs or persistent Postgres nodes are launched.
- Boto3 / OCI CLI scripts spin up Spot/Preemptible VMs.
- Nodes process their assigned spatial grids and auto-terminate after uploading the partition to S3.

## 6. Onboarding Next Steps
For detailed procedural tutorials, developers should traverse to:
- [Zero-to-Hero Execution Guide](../wiki/zero_to_hero.md)
- [Principal Engineering Context](../wiki/principal_guide.md)
