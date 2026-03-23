# End-to-End Pipeline Architecture

```mermaid
graph TD
    subgraph Data Sources
        NSI_API["USACE NSI API<br/>(30M+ buildings)"]
        NHC["NHC P-Surge GeoTIFF<br/>(downloaded from NHC)"]
        NSI_PARQUET["NSI Parquet<br/>(partitioned by state)"]
        CENSUS["Census ACS 5-year API<br/>(county population)"]
        SVI["CDC SVI 2022<br/>(county-level, RPL_THEMES 0-1)"]
        GT["Ground Truth Data.xlsx<br/>(9 hurricanes 2018-2024)"]
    end

    subgraph "Data Ingestion (local)"
        DL["download_nsi_by_state.py<br/>API → GeoJSON"]
        CONVERT["nsi_raw_to_parquet.py<br/>GeoJSON → Parquet"]
    end

    subgraph "Pipeline 1: FAST Damage Engine (local, no AWS)"
        DUCKDB["duckdb_fast_pipeline.py<br/>DuckDB SQL: bbox filter,<br/>dedup, column mapping"]
        LEGACY["fast_e2e_from_oracle.py<br/>(legacy, row-by-row)"]
        FAST_CSV["FAST Input CSV<br/>(FltyId, Occ, Cost, etc.)"]
        FAST["FAST Engine<br/>run_fast.py → hazus_notinuse.py<br/>depth-damage function lookup"]
        PREDICTIONS["Building-Level Predictions<br/>~3.9M rows<br/>BldgDmgPct, Depth_Grid,<br/>BldgLossUSD"]
    end

    subgraph "AWS (S3 + Athena)"
        S3["S3: red-cross-capstone-<br/>project-data/"]
        ATHENA["Athena SQL queries"]
    end

    subgraph "Pipeline 2: L/M/H Population Impact"
        LMH_04["04_classify_lmh.py<br/>Athena: dedup → L/M/H<br/>zone classification<br/>→ county aggregation"]
        SVI_BUMP["SVI Conditional Bump<br/>HIGH zone: pop_impacted<br/>× (1 + 0.2 × svi_score)"]
        FMT_05["05_format_for_spreadsheet.py<br/>Census join + SVI join<br/>+ SVI bump + ARC rates<br/>shelter: H=5% M=3% L=1%<br/>feeding: H=12% M=7% L=3%"]
        VAL_06["06_validate_lmh.py<br/>RMSE/MAE/R² vs GT<br/>threshold sensitivity"]
        LMH_LONG["county_lmh_long.csv<br/>(836 rows: event×county×zone)"]
        LMH_WIDE["county_lmh_features.csv<br/>(383 rows: event×county)"]
        PLANNING["planning_assumptions_output.csv<br/>+ arc_planning_template_lmh.xlsx"]
        REPORT["lmh_validation_report.md"]
    end

    subgraph "Validation & Analysis (local)"
        VALIDATE["validate_pipeline.py<br/>schema + stats checks"]
        MATCH["match_county_coverage_cloud.py<br/>county coverage vs GT (Athena)"]
    end

    %% Data Ingestion
    NSI_API --> DL
    DL --> CONVERT
    CONVERT --> NSI_PARQUET

    %% Pipeline 1
    NSI_PARQUET --> DUCKDB
    NSI_PARQUET --> LEGACY
    NHC --> DUCKDB
    NHC --> LEGACY
    DUCKDB --> FAST_CSV
    LEGACY --> FAST_CSV
    FAST_CSV --> FAST
    NHC -.->|raster| FAST
    FAST --> PREDICTIONS

    %% Upload to S3
    PREDICTIONS -->|upload| S3
    S3 --> ATHENA

    %% Pipeline 2 (L/M/H)
    ATHENA --> LMH_04
    LMH_04 --> LMH_LONG
    LMH_04 --> LMH_WIDE
    LMH_WIDE --> FMT_05
    CENSUS --> FMT_05
    SVI --> FMT_05
    FMT_05 --> SVI_BUMP
    SVI_BUMP --> PLANNING
    PLANNING --> VAL_06
    GT --> VAL_06
    VAL_06 --> REPORT

    %% Validation
    PREDICTIONS --> VALIDATE
    ATHENA --> MATCH
    GT --> MATCH

    %% Styling
    style DUCKDB fill:#2d6a4f,color:#fff
    style FAST fill:#d62828,color:#fff
    style LMH_04 fill:#457b9d,color:#fff
    style SVI_BUMP fill:#6a4c93,color:#fff
    style FMT_05 fill:#457b9d,color:#fff
    style VAL_06 fill:#457b9d,color:#fff
    style SVI fill:#6a4c93,color:#fff
    style PLANNING fill:#e76f51,color:#fff
    style S3 fill:#ff9f1c,color:#000
    style ATHENA fill:#ff9f1c,color:#000
```

## Legend

| Color | Meaning |
|-------|---------|
| Green | Primary local pipeline (preferred) |
| Red | FAST engine core |
| Blue | L/M/H population impact pipeline |
| Purple | SVI data source + conditional bump |
| Orange | AWS services (S3/Athena) |
| Coral | Final deliverable |

## AWS Dependency Boundary

Only these scripts require AWS credentials (`boto3`):
- `04_classify_lmh.py` — Athena query
- `match_county_coverage_cloud.py` — Athena + S3

Everything else runs locally. If FAST prediction CSVs are available on local disk, the Athena query in 04 could be replaced with DuckDB to eliminate AWS entirely.
