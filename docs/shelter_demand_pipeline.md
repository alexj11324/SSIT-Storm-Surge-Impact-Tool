# Shelter Demand Pipeline (Colab Notebook)

```mermaid
graph TD
    subgraph "External Data Sources"
        NHC["NHC P-Surge GeoTIFF"]
        NSI_API["USACE NSI API"]
        CENSUS["Census ACS 5-year<br/>(tract-level population)"]
        SVI["CDC SVI 2022<br/>(tract-level, RPL_THEMES 0–1)"]
    end

    subgraph "Input"
        EXCEL["Excel Interface<br/>Storm ID, Advisory, Year<br/>+ BHI configurable params"]
    end

    subgraph "Stage 1: Data Acquisition"
        RASTER["Download P-Surge Raster<br/>NHC ZIP → TIF in memory<br/>+ identify affected states"]
        NSI_DL["Download NSI per State<br/>USACE API → GeoJSON → Parquet<br/>retains bid + cbfips"]
    end

    subgraph "Stage 2: FAST Damage Engine"
        FILTER["Spatial Filter + Column Mapping<br/>bbox clip, dedup by bid,<br/>NSI → FAST schema"]
        FAST["FAST Depth-Damage Lookup<br/>raster depth − FirstFloorHt<br/>→ DDF by occupancy"]
    end

    subgraph "Stage 3: Damage Classification (Census Tract)"
        GEOID["Derive Tract GEOID<br/>JOIN predictions ↔ NSI by FltyId=bid<br/>tract = cbfips[:11]"]
        DMGSTATE["Classify Damage State<br/>BldgDmgPct thresholds:<br/>  0–15% → Slight<br/>  15–40% → Moderate<br/>  40–60% → Extensive<br/>  60–100% → Complete"]
        TRACTAGG["Aggregate to Tract<br/>count buildings per damage state<br/>compute max_intensity,<br/>% destroyed, % major damage"]
        SEVERITY["Classify Tract Severity<br/>≥35% destroyed → HIGH<br/>11–34% → MEDIUM<br/><11% → LOW"]
    end

    subgraph "Stage 4: Shelter Demand"
        BHI["Compute BHI Factor (low/high)<br/>─────────────────────<br/>BHI = Σ frac_d × [<br/>  U[d][FU] × S[risk][FU] +<br/>  U[d][PU] × S[risk][PU] +<br/>  U[d][NU] × 1.0 ]"]
        SVIJOIN["Census Population + SVI Join<br/>SVI mapping:<br/>  0.0–0.4 → 0%<br/>  0.4–0.8 → 2.5%<br/>  0.8–1.0 → 5%"]
        SHELTER["Shelter-Seeking Estimation<br/>─────────────────────<br/>shelter = population<br/>  × BHI_factor<br/>  × SVI_Value_Mapped"]
    end

    %% Data flow
    EXCEL -->|"params JSON"| RASTER
    NHC --> RASTER
    RASTER -->|"EVENT_advN_e10_ResultMaskRaster.tif"| FILTER
    RASTER -->|"affected_states[]"| NSI_DL
    NSI_API --> NSI_DL
    NSI_DL -->|"nsi_STATE.parquet<br/>(bid, cbfips, lat, lon, ...)"| FILTER
    FILTER -->|"fast_input.csv"| FAST
    NHC -.->|raster| FAST
    FAST -->|"predictions.csv<br/>(BldgDmgPct, Depth_Grid)"| GEOID
    NSI_DL -.->|"cbfips via bid JOIN"| GEOID
    GEOID --> DMGSTATE
    DMGSTATE --> TRACTAGG
    TRACTAGG --> SEVERITY
    SEVERITY --> BHI
    BHI --> SVIJOIN
    CENSUS --> SVIJOIN
    SVI --> SVIJOIN
    SVIJOIN --> SHELTER
    SHELTER -->|"shelter_demand_output.csv<br/>+ .xlsx"| DONE["Deliverable<br/>→ paste back to Excel"]

    %% Styling
    style EXCEL fill:#1d3557,color:#fff
    style RASTER fill:#2d6a4f,color:#fff
    style NSI_DL fill:#2d6a4f,color:#fff
    style FILTER fill:#2d6a4f,color:#fff
    style FAST fill:#d62828,color:#fff
    style GEOID fill:#457b9d,color:#fff
    style DMGSTATE fill:#457b9d,color:#fff
    style TRACTAGG fill:#457b9d,color:#fff
    style SEVERITY fill:#457b9d,color:#fff
    style BHI fill:#e9c46a,color:#000
    style SVIJOIN fill:#e9c46a,color:#000
    style SHELTER fill:#e9c46a,color:#000
    style SVI fill:#6a4c93,color:#fff
    style DONE fill:#e76f51,color:#fff
```

## Legend

| Color | Meaning |
|-------|---------|
| Dark blue | User input (Excel) |
| Green | Data acquisition & preparation |
| Red | FAST damage engine |
| Blue | Damage classification & tract aggregation |
| Gold | BHI computation & shelter demand |
| Purple | SVI data source |
| Coral | Final deliverable |

## Notebook Cell Mapping

| Cell | Stage | Node |
|------|-------|------|
| 2 | Input | Excel params JSON |
| 3 | 1 | Download P-Surge Raster |
| 4 | 1 | Download NSI per State |
| 5 | 2 | Spatial Filter + Column Mapping |
| 6 | 2 | FAST Engine |
| 7 | 3 | Derive Tract GEOID |
| 8 | 3 | Classify Damage State + Aggregate |
| 9 | 3 | Classify Tract Severity |
| 10 | 4 | Compute BHI Factor |
| 11 | 4 | Census + SVI Join |
| 12 | 4 | Shelter-Seeking Estimation |
| 13 | — | Export |
