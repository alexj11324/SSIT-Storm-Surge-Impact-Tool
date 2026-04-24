# ARC Capstone: System Manual

## 1. Executive Summary
The Immediate Tsunami and Storm Surge Population Impact Modeling system is a Python-based data pipeline built for the American Red Cross. It ingests National Structure Inventory (NSI) building data and NHC P-Surge inundation rasters to estimate building damage via FEMA's FAST engine, then derives shelter demand at census-tract level using the BHI (Building Habitability Index) model.

## 2. Core Architecture Philosophy
To achieve rapid iteration and avoid classic GIS latency traps of large SQL JOINs, the architecture relies on **storage-as-compute**:
- **DuckDB**: Replaces traditional database environments to load partitioned `.parquet` payloads directly from disk.
- **Decoupled Engine**: The FEMA FAST utility operates as a black-box local shell execution, receiving structural damage states natively formatted via our pipeline.

*For the pipeline architecture diagram, refer to [e2e_pipeline.md](../e2e_pipeline.md).*

## 3. High-Level Data Flow
1. **Raster Acquisition**: NHC P-Surge GeoTIFF rasters are downloaded via `scripts/import_nhc_by_storm.py`.
2. **NSI Loading**: Building inventory is loaded in the notebook via `scripts/nsi_downloader.py`, using HuggingFace preprocessed Parquet when available and the USACE API fallback when needed.
3. **Intersection**: `scripts/duckdb_fast_pipeline.py` leverages DuckDB to spatially filter NSI buildings within the raster bbox and map columns to FAST schema.
4. **Damage Scoring**: Matched buildings are sent through `FAST-main/Python_env/run_fast.py` to yield damage percentages.
5. **Shelter Demand**: `notebooks/shelter_demand.ipynb` classifies damage states, computes BHI factors, joins Census/SVI data, and estimates shelter-seeking population.

## 4. Engineering Guidelines
- **Language**: Python 3.10+
- **Version Control**: Conventional Commits (`feat:`, `fix:`, `docs:`)
- **Formatting**: `ruff format` (line-length 100) and `ruff check --fix`. Config in `pyproject.toml`.

## 5. Onboarding
- [Zero-to-Hero Guide](../wiki/zero_to_hero.md)
- [Principal Architecture Guide](../wiki/principal_guide.md)
