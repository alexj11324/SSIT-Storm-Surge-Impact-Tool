# Data Sources and Methodology

---

## 1. Overview: Data Sources

| Dataset | Owner | Coverage & Storage |
|---------|-------|--------------------|
| **NSI** — National Structure Inventory (2022) | USACE | 30M+ structures nationwide · S3 Parquet partitioned by state |
| **NHC P-Surge Rasters** — Probabilistic Storm Surge GeoTIFFs | NOAA / NHC | 9 events × 3 advisories = 27 rasters · `FAST-main/rasters/` |
| **Ground Truth** — ARC post-event operational records | American Red Cross | 9 hurricanes 2018–2024 · `arc_analysis.gt_input_*` (Athena) |
| **US County Boundaries** | Census / TIGER | Nationwide · `arc_analysis.us_county_boundaries` |

---

## 2. Data Source Details

### 2.1 NSI — National Structure Inventory

- **Purpose**: Building-level input to FAST damage model and population estimation
- **Granularity**: One row per structure (point geometry, WGS84)
- **S3 path**: `s3://red-cross-capstone-project-data/processed/nsi/`
- **Partition**: `state` (e.g., `state=Florida`)

**Key fields:**

| Field | Description & Notes |
|-------|---------------------|
| `bid` | Unique building ID → mapped to FAST `FltyId`; deduplicated before run |
| `occtype` | Occupancy type: `RES1` (single-family), `RES3` (multi-family), `COM1` (retail) |
| `val_struct` | Structure replacement cost ($) → FAST `Cost` |
| `val_cont` | Contents replacement cost ($) → FAST `ContentCost` |
| `sqft` | Total floor area (sq ft) → FAST `Area` |
| `num_story` | Stories above ground → FAST `NumStories` |
| `found_type` | Foundation type: `Slab`→7, `Crawl`→5, `Pile`→2, `Basement`→4 (FAST numeric code) |
| `found_ht` | First floor height above grade (ft) → FAST `FirstFloorHt` |
| `ground_elv` | Ground elevation (ft, NAVD88) — used to compute FFE |
| `latitude` / `longitude` | WGS84 coordinates |
| `cbfips` | Census Block FIPS (15-digit) — joins to SVI / ACS demographics |
| `firmzone` | FEMA FIRM zone: `AE` (1% annual flood), `VE` (coastal wave), `X` (minimal risk) |
| `st_damcat` | Damage category: `RES` / `COM` / `IND` / `PUB` |
| `pop2pmo65` / `pop2pmu65` | Nighttime population, age 65+ / <65 (evening occupancy estimate) |
| `pop2amo65` / `pop2amu65` | Daytime population, age 65+ / <65 |
| `o65disable` / `u65disable` | Disability rate for 65+ / <65 — used for high-need population estimate |

> **First Floor Elevation (FFE) = `ground_elv` + `found_ht`**
> Structure is inundated when surge depth > FFE.

---

### 2.2 NHC P-Surge Rasters — Flood Depth Input

- **Purpose**: Storm surge flood depth (ft) per advisory, consumed directly by FAST
- **Source**: NOAA National Hurricane Center — Probabilistic Storm Surge (P-Surge) product
- **Format**: GeoTIFF (`.tif`), flood depth in feet above ground
- **Location**: `FAST-main/rasters/`

**Naming convention:**

```
{EVENT}_{YEAR}_adv{N}_e10_ResultMaskRaster.tif
Example: HELENE_2024_adv15_e10_ResultMaskRaster.tif
```

| Token | Meaning |
|-------|---------|
| `adv{N}` | NHC advisory number — forecast issued at that point in the storm track |
| `e10` | 10% exceedance probability — surge depth with 10% chance of being exceeded (upper-end planning level) |
| `ResultMaskRaster` | NHC P-Surge standard product name |

**Rasters by event:**

| Event | Advisories | States & Scale |
|-------|-----------|----------------|
| `BERYL_2024` | 39, 40, 41 | TX, LA · ~107K buildings/adv |
| `DEBBY_2024` | 18, 19, 20 | FL, GA, NC, SC, VA · ~103K/adv |
| `FLORENCE_2018` | 63, 64, 65 | NC, SC, VA · 17K–32K/adv |
| `HELENE_2024` | 14, 15, 16 | FL, GA, NC, SC · 240K–475K/adv |
| `IAN_2022` | 31, 32, 33 | FL, NC, SC · ~119K–122K/adv |
| `IDALIA_2023` | 18, 19, 20 | FL, GA, SC · 62K–124K/adv |
| `IDA_2021` | 16, 17, 18 | AL, LA, MS · ~412K/adv |
| `MICHAEL_2018` | 20, 21, 22 | FL, GA coastal · ~900/adv |
| `MILTON_2024` | 20, 21, 22 | FL · 70K–208K/adv |

**Total: 27 rasters · ~3.9M building-level predictions**

---

### 2.3 Ground Truth — ARC Operational Records

- **Purpose**: Validation baseline — actual post-event shelter and impact figures
- **Coverage**: 9 hurricane events, 2018–2024
- **Stored in**: `arc_analysis.gt_input_*` (from `Ground Truth Data.xlsx`)

**Schema:**

| Field | Description & Notes |
|-------|---------------------|
| `event` / `event_key` | Hurricane name / normalized key (e.g., `"Helene"` / `"HELENE_2024"`) |
| `landfall_date` | Date of landfall (e.g., `"2024-09-26"`) |
| `county` / `county_name_norm` | County name raw / normalized (e.g., `"Lee County"` / `"LEE"`) |
| `state_abbr` / `county_fips5` | State abbreviation / 5-digit FIPS code (e.g., `"FL"` / `"12071"`) |
| `planned_shelter_population` | Pre-event planned shelter headcount |
| `actual_shelter_population` | Actual shelter headcount observed — **primary validation target** |
| `estimated_population_impacted` | ARC estimate of total population impacted |

---

### 2.4 US County Boundaries

- **Table**: `arc_analysis.us_county_boundaries`
- **Purpose**: Spatial join — map building lat/lon to counties for coverage validation

| Field | Description |
|-------|-------------|
| `county_fips5` | 5-digit county FIPS code |
| `county_name` / `state_abbr` | County name / 2-letter state abbreviation |
| `geometry` | County polygon (WKT) — used in `ST_Intersects` spatial join |

---

## 3. AWS Data Architecture

```
S3: red-cross-capstone-project-data/
├── processed/nsi/       → red_cross_hurricane.nsi_data   (NSI source)
├── arc-results/parquet/ → arc_storm_surge.predictions    (FAST output)
└── analysis/            → arc_analysis.*                 (validation artifacts)

GitHub: FAST-main/rasters/
└── {EVENT}_adv{N}_e10_ResultMaskRaster.tif               (NHC P-Surge rasters)
```

| Athena Database | Role |
|-----------------|------|
| `red_cross_hurricane` | Source data — NSI buildings |
| `arc_storm_surge` | FAST model outputs partitioned by `event` / `adv` |
| `arc_analysis` | Validation — ground truth, county matching, coverage summaries |

---

## 4. FAST Model Output — `arc_storm_surge.predictions`

Central output table, partitioned by **`event`** and **`adv`** (advisory number).

**Input passthrough (from NSI):**

| Field | Description & Notes |
|-------|---------------------|
| `fltyid` | Building ID (from NSI `bid`) |
| `occ` | Occupancy type (`RES1`, `COM1`, `RES3`, …) |
| `cost` / `contentcost` | Structure / contents replacement cost ($) |
| `area` / `numstories` | Floor area (sq ft) / stories above ground |
| `foundationtype` | Foundation code: 2=Pier, 4=Basement, 5=Crawl, 7=Slab |
| `firstfloorht` | First floor height above grade (ft) |
| `latitude` / `longitude` | WGS84 coordinates |

**FAST damage outputs:**

| Field | Description & Notes |
|-------|---------------------|
| `depth_grid` | Surge depth from P-Surge raster at building location (ft) |
| `depth_in_struc` | Depth inside structure = `depth_grid` − `firstfloorht` (ft) |
| `bldgdmgpct` / `bldglossusd` | Building damage % / structural loss ($) |
| `contdmgpct` / `contentlossusd` | Content damage % / content loss ($) |
| `invdmgpct` / `inventorylossusd` | Inventory damage % / loss ($ — commercial buildings) |
| `debris_fin` / `debris_struc` / `debris_tot` | Debris by type (tons) |
| `restor_days_min` / `restor_days_max` | Estimated restoration time range (days) |
| `bddf_id` / `cddf_id` / `iddf_id` | Depth-Damage Function IDs applied |

**Pipeline metadata:**

| Field | Description & Notes |
|-------|---------------------|
| `state` | US state name (e.g., `"Florida"`) |
| `flc` | Flood class: `CoastalA` (default) / `CoastalV` (high-risk) / `Riverine` |
| `raster_name` | Source P-Surge raster filename |
| `event` / `adv` | **Partition keys** — hurricane slug / NHC advisory number |

---

## 5. Methodology Pipeline

```
[1] NHC Issues Storm Advisory
        │
        ▼
[2] NHC P-Surge Raster (.tif)
    {EVENT}_adv{N}_e10_ResultMaskRaster.tif
    Flood depth (ft) at 10% exceedance probability
        │
        │                    [3] NSI Building Inventory (S3 Parquet)
        │                        occtype, val_struct, sqft,
        │                        found_type, found_ht, lat/lon,
        │                        population + disability fields
        │                             │
        │                             ▼ fast_e2e_from_oracle.py
        │                        FAST-ready CSV
        │                        (column mapping + spatial filter + dedup)
        │                             │
        ▼                             ▼
[4] FAST Engine (FEMA Hazus-based)
    per building: raster depth → depth_in_struc → DDF lookup → damage %
        │
        ▼
[5] arc_storm_surge.predictions
    27 runs · 9 events × 3 advisories · ~3.9M buildings
    BldgDmgPct, BldgLossUSD, Depth_in_Struc, Debris, Restoration days
        │
        ├─────────────────────────────────────────────┐
        ▼                                             ▼
[6] Population Displaced                 [7] County Coverage Validation
    Σ (pop × damage probability)             spatial join → county FIPS
    day/night × age 65± / <65               vs. ARC Ground Truth records
        │                                        coverage_rate per event
        ▼
[8] High-Need Population
    Disrupted pop × SVI weight
    (elderly, disabled, low-income)
        │
        ▼
[9] Red Cross Service Demand
    Shelter capacity · Casework · ERV deployment zones
```

---

## 6. County Coverage Validation (`arc_analysis`)

Prediction points → spatial join with `us_county_boundaries` → county FIPS,
then matched against ARC ground truth by `(event, state_abbr, county_fips5)`.

| Table Pattern | Contents |
|---------------|----------|
| `pred_event_counties_*` | Counties with ≥1 prediction point, per event |
| `gt_county_keys_*` | Ground truth counties requiring coverage |
| `matched_counties_*` | Counties in both predictions and ground truth |
| `gt_unmatched_counties_*` | Ground truth counties missed by model |
| `summary_by_event_state_*` | `coverage_rate = matched / gt_total`, by event × state |

---

## 7. Key Methodological Choices & Limitations

| Choice | Rationale |
|--------|-----------|
| NHC P-Surge `e10` rasters (10% exceedance) | Upper-end planning level; grounded in actual advisory forecasts rather than hypothetical worst-case |
| 3 advisories per event | Captures forecast evolution as storm approaches; enables uncertainty comparison |
| `CoastalA` as default FAST flood class | Conservative coastal baseline; `CoastalV` used for high-risk sensitivity runs |
| Impact-only mode (inside raster bbox) | Avoids FAST returning depth=0 for out-of-bounds buildings |
| FltyId deduplication before FAST | Prevents inflated damage totals from duplicate NSI records |

**Known limitations:**
- P-Surge `e10` is a probabilistic forecast, not a verified observed surge depth
- NSI building characteristics are statistically modeled, not field-surveyed
- SVI is at census tract level; building-level vulnerability requires interpolation
- `MICHAEL_2018` has very small raster footprint (~900 buildings)
