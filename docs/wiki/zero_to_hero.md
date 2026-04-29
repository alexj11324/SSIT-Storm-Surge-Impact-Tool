# Zero-to-Hero Learning Path

Welcome! If you are new to geospatial data or disaster prediction systems, this guide will take you step-by-step from zero knowledge to running massive Red Cross disaster predictions.

## Part I: Foundations (The Tools)

Our project uses specific technologies to process millions of building points quickly:
- **DuckDB**: Think of it as SQLite but designed for massive Data Analytics rather than small apps. It’s what reads gigabytes of CSV/Parquet instantly.
- **Parquet**: A columnar storage format. If CSV reads row-by-row (slow for 30 million lines), Parquet reads vertically by columns, which is lightning fast for data science.
- **GeoPandas/Rasterio**: Python libraries. Rasterio reads `.tif` images (like satellite images, where pixels contain flood depth data). GeoPandas handles polygon/vector shapes (like county lines).

## Part II: This Codebase's Architecture

The goal: **Take a projected hurricane, figure out the flooding at every single house, and tell the Red Cross how many people are displaced.**

We achieve this via:
1. **Input Data**: 
   - `NSI` (National Structure Inventory): Where is every building in the US?
   - `NHC P-Surge`: NOAA's probabilistic hurricane surge rasters.
2. **Intersection (`duckdb_fast_pipeline.py`)**: Merges the building data with the flood depths.
3. **Execution (`FAST-main/Python_env/run_fast.py`)**: FEMA's FAST engine takes the flood depth per building, calculates building value (`Cost`), and spits out structural damage percentage (`BldgDmgPct`).
4. **Shelter Demand**: Building damage is classified into damage states, aggregated to census tracts, and combined with Census population + SVI data to estimate shelter-seeking population (see `notebooks/shelter_demand.ipynb`).

## Part III: Setup, Execute, and Contribute

1. **Prerequisites**
   Install the project dependencies:
   ```bash
   pip install -e '.[dev]'
   ```
2. **Run the shelter demand notebook**
   Open `notebooks/shelter_demand.ipynb` in Google Colab. The notebook reads the Excel interface, applies any notebook-only storm overrides, downloads the NHC P-Surge raster, infers affected states from the raster footprint, loads NSI, runs FAST, and exports CSV/XLSX deliverables.
3. **Optional local FAST-input build**
   Use this only when you want to build a FAST-ready CSV outside the notebook:
   ```bash
   python scripts/duckdb_fast_pipeline.py \
     --parquet-glob "data/nsi/state=FL/*.parquet" \
     --raster FAST-main/rasters/IAN_2022_adv33_e10_ResultMaskRaster.tif \
     --output outputs/fast_input.csv --flc CoastalA
   ```

## Appendix: Glossary of Key Terms

- **NHC P-Surge**: NOAA National Hurricane Center probabilistic surge product; pre-computed inundation GeoTIFF rasters used as flood depth input.
- **NSI**: National Structure Inventory.
- **FAST**: FEMA's predictive simulation tool.
- **TIF (GeoTIFF)**: Image format containing geographic projection metadata.
- **Advisory (Adv)**: NOAA's public forecast updates (often numbered Adv 14, 15...).
