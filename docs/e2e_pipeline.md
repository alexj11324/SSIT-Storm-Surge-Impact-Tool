# End-to-End Pipeline Architecture

```mermaid
graph TD
    subgraph "External Data Sources"
        NSI_API["USACE NSI API<br/>(30M+ buildings)"]
        NHC["NHC P-Surge GeoTIFF<br/>(flood depth, feet)"]
        CENSUS["Census ACS 5-year<br/>(county population)"]
        SVI["CDC SVI 2022<br/>(RPL_THEMES 0–1)"]
        GT["Ground Truth Data.xlsx<br/>(9 hurricanes 2018–2024)"]
    end

    subgraph "Stage 1: Data Ingestion"
        INGEST["Download & Convert<br/>API → GeoJSON → Parquet"]
    end

    subgraph "Stage 2: Building-Level Damage (FAST Engine)"
        PREP["Spatial Filter & Column Mapping<br/>DuckDB: bbox clip, dedup by bid,<br/>NSI → FAST schema"]
        FAST["FAST Depth-Damage Lookup<br/>raster depth at lat/lon<br/>− FirstFloorHt → DDF by occupancy"]
    end

    subgraph "Stage 3: S3 / Athena"
        S3_ATHENA["arc_storm_surge.predictions_csv<br/>~3.9M rows on S3"]
    end

    subgraph "Stage 4: Intensity Zone Classification"
        CLASSIFY["Dedup across advisories<br/>(MAX damage per building)<br/>─────────────────────<br/>Surge depth primary:<br/>  >12 ft → HIGH | ≥9 → MED | ≥4 → LOW<br/>Damage % fallback:<br/>  >35% → HIGH | >15% → MED | >0% → LOW<br/>─────────────────────<br/>Spatial join → county FIPS<br/>Aggregate per county × zone"]
    end

    subgraph "Stage 5: Planning Estimates"
        PLAN["Census population join<br/>+ SVI bump (1 + 0.20 × svi_score) on HIGH<br/>─────────────────────<br/>ARC conversion rates:<br/>  Shelter: H=5% M=3% L=1%<br/>  Feeding: H=12% M=7% L=3%"]
    end

    subgraph "Stage 6: Validation"
        VALIDATE["Compare vs Ground Truth<br/>RMSE, MAE, R²<br/>threshold sensitivity"]
    end

    %% Data flow with output artifacts on edges
    NSI_API --> INGEST
    INGEST -->|"nsi/state=XX/part-00000.snappy.parquet"| PREP
    NHC -->|"EVENT_YEAR_advN_e10_ResultMaskRaster.tif"| PREP
    NHC -.->|raster| FAST
    PREP -->|"fast_input.csv<br/>(FltyId, Occ, Cost, Lat, Lon, ...)"| FAST
    FAST -->|"predictions.csv<br/>(BldgDmgPct, Depth_Grid, BldgLossUSD)"| S3_ATHENA

    S3_ATHENA --> CLASSIFY
    CLASSIFY -->|"county_lmh_long.csv<br/>(event × county × zone)"| PLAN
    CLASSIFY -->|"county_lmh_features.csv<br/>(event × county, wide)"| PLAN
    CENSUS --> PLAN
    SVI --> PLAN
    PLAN -->|"planning_assumptions_output.csv<br/>arc_planning_template_lmh.xlsx"| VALIDATE
    GT --> VALIDATE
    VALIDATE -->|"lmh_validation_report.md"| DONE["Deliverable"]

    %% Styling
    style PREP fill:#2d6a4f,color:#fff
    style FAST fill:#d62828,color:#fff
    style S3_ATHENA fill:#ff9f1c,color:#000
    style CLASSIFY fill:#457b9d,color:#fff
    style PLAN fill:#457b9d,color:#fff
    style VALIDATE fill:#457b9d,color:#fff
    style SVI fill:#6a4c93,color:#fff
    style DONE fill:#e76f51,color:#fff
```

## Legend

| Color | Meaning |
|-------|---------|
| Green | Data preparation (DuckDB) |
| Red | FAST damage engine |
| Orange | Data storage |
| Blue | Population impact pipeline |
| Purple | SVI data source |
| Coral | Final deliverable |

## Notes

- **Edges show output file names** — each arrow is labeled with the artifact produced by that step.
- **Stages 1-2 run locally**; Stages 4-6 run in Google Colab (`notebooks/shelter_demand.ipynb`).
- All data processing uses local Parquet + DuckDB. No cloud dependencies required.
