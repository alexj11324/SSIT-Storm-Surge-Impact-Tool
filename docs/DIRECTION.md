# ARC Population Impact — Project Direction & Required Pivot

> **Note**: References to AWS Athena, S3, and scripts like `04_classify_lmh.py` describe the original cloud-based workflow. The current pipeline runs locally with DuckDB; shelter demand is computed in `notebooks/shelter_demand.ipynb`.

> CMU Heinz MSPPM 2026 Capstone | American Red Cross
> Date: 2026-03-10 | Based on: Client meeting transcript + Mass Care Planning Assumptions Job Tool V.6.0 + Planning Assumptions Spreadsheet (Michael 2021)

---

## 0. Executive Summary

**We've been solving the wrong problem.** Our current model predicts `shelter_pop` directly from FAST damage data. ARC needs us to produce **Population Affected and Population Impacted by county, classified into Low/Medium/High intensity zones**, which feeds into their existing Mass Care Planning Assumptions Spreadsheet. ARC's own conversion rates (Shelter: H=5%, M=3%, L=1%) handle the rest.

**The pivot**: from "ML model predicting shelter population" to "deterministic pipeline classifying FAST building-level damage into L/M/H intensity zones and aggregating to county-level population counts."

---

## 1. What ARC Actually Needs (from meeting transcript)

### 1.1 Key Client Quotes

> **Louis (30:35)**: "So you see how we have three columns low, medium, high for population affected... and then we have population impacted... we go from population impacted to number that will seek shelter. **If you are able to get any of those steps, that would be success.**"

> **Louis (32:08)**: "So if you are able to either get columns J/K/L or columns M/N/O or columns P/Q/R."

> **Louis (32:24)**: "If you could get any of those sets, **our planning factors would get us the rest of the way**."

> **Louis (42:15)**: "Mass care planning assumptions spreadsheet... it's like a **chain of equations**."

> **Louis (42:27)**: "If you are able to make the prediction at any point in that chain, what we have will finish it."

> **Michael (40:55)**: "If you can give us the number of people impacted, then we can estimate the number going to require mass care services."

> **Michael (41:29)**: "**Impacted** as we've defined — there's a difference between affected... a tree falls on my neighbor's house, he is **impacted**... actual damage to the structure."

> **Louis (37:21)**: "If it goes all the way to **damage**, that's **impacted**, not just affected."

> **Louis (38:34)**: "**Damaged residences equal households.**"

### 1.2 FAST Output = Ground Truth

Louis stated three times that FAST output is effectively ground truth:
- Line 170: "we consider this output to be effectively equivalent to ground truth"
- Line 227: "the Hazus is to us, we consider that to be effectively good enough to be ground truth"
- Line 317: "that output to us is effectively ground truth"

**Implication**: We do NOT need to train an ML model to predict what FAST already computes. We need to **organize and classify FAST output** into the format ARC's spreadsheet expects.

### 1.3 Deliverable Format

> **Louis (43:28)**: "The last step has to be **by county in a CSV format**."

> **Louis (43:39)**: "The actual work and the actual running the model can be done in Colab."

---

## 2. The Chain of Equations (ARC's Framework)

Source: *Determine Mass Care Planning Assumptions Job Tool V.6.0*

### 2.1 Four-Factor Process (Figure 17)

```
Factor 1: Confirm Event Type (hurricane, notice event)
    ↓
Factor 2: Define Affected Area & Levels of Impact (L/M/H zones)
    ↓
Factor 3: Estimate Population of Impact Zones (county × L/M/H)
    ↓
Factor 4: Determine Population Requiring Mass Care (apply conversion rates)
```

### 2.2 Intensity Classification (Figure 9)

| Criterion | High | Medium | Low |
|-----------|------|--------|-----|
| Hurricane Category | Cat 4/5 | Cat 3 | Cat 1/2 |
| **Storm Surge** | **>12 ft** | **9-12 ft** | **4-8 ft** |
| Inland Rainfall | >25 in/24hr | >15 in/24hr | >10 in/24hr |
| Buildings Destroyed | >35% | 11-34% | 0-10% |
| Buildings w/ Major Damage | 35-100% | 16-34% | 0-15% |

### 2.3 Damage Classification (Figure 10)

| Factor | High | Medium | Low |
|--------|------|--------|-----|
| Structural damage | >35% destroyed + 35-100% major | 11-34% destroyed + 16-34% major | 0-10% destroyed + 0-15% major |
| Power outage | 51-100% w/o power | 21-50% w/o power | 0-20% w/o power |
| Water systems | 31-100% inoperable | 11-30% inoperable | 0-10% inoperable |

### 2.4 Population Flow (Figures 13-16)

```
County Census Population
    ↓ × % of county in affected area
Population Affected (split into L/M/H by intensity zone)
    ↓ × impacted rate per zone (from Figure 14 example: L=10%, M=30%, H=60%)
Population Impacted (L/M/H)
    ↓ × mass care conversion rate (Figure 16)
Population Needing Mass Care (shelter, feeding, DES)
```

### 2.5 Mass Care Conversion Rates (Figure 16)

| Impact Zone | Shelter % | Feeding % |
|-------------|-----------|-----------|
| **High** | **5.0%** | 12.0% |
| **Medium** | **3.0%** | 7.0% |
| **Low** | **1.0%** | 3.0% |

### 2.6 Planning Assumptions Spreadsheet Columns

| Column Group | Columns | Content |
|-------------|---------|---------|
| County Info | A-I | FIPS, county name, census population, % impacted, etc. |
| **Population Affected** | **J/K/L** | **Low / Medium / High** |
| **Population Impacted** | **M/N/O** | **Low / Medium / High** |
| **Households Needing Shelter** | **P/Q/R** | **Low / Medium / High** |

---

## 3. The Pivot: What Changes

### 3.1 Old Approach (WRONG)

```
FAST damage (3.5M buildings)
    → County aggregate (avg_damage_pct, n_displaced, total_loss)
    → ML model (XGBoost/RF/Ridge ensemble)
    → Predict: shelter_pop (single number per county)
```

**Problems**:
- Predicts the WRONG output (shelter_pop instead of L/M/H population affected/impacted)
- ML model adds no value (R² = -0.308, worse than mean prediction)
- Ignores ARC's established framework
- Output doesn't feed into Planning Assumptions Spreadsheet

### 3.2 New Approach (CORRECT)

```
FAST damage (3.5M buildings, per-building bldgdmgpct + depth)
    → Classify each building into intensity zone (L/M/H)
        based on surge depth: >12ft=H, 9-12ft=M, 4-8ft=L
        OR based on bldgdmgpct: >35%=H, 16-34%=M, ≤15%=L
    → Aggregate to county level:
        Pop_Affected_Low, Pop_Affected_Med, Pop_Affected_High
        Pop_Impacted_Low, Pop_Impacted_Med, Pop_Impacted_High
    → Output: CSV by county, ready for Planning Assumptions Spreadsheet
```

**ARC then applies their own conversion rates** (Shelter: H=5%, M=3%, L=1%) to get mass care needs.

### 3.3 Mapping FAST Fields to Intensity Zones

FAST output per building includes:
- `depth_in_struc` (ft): water depth inside structure
- `bldgdmgpct` (%): building damage percentage
- `depth_grid` (ft): raw surge depth at building location

**Proposed classification logic** (building level):

```python
def classify_intensity(row):
    surge = row['depth_grid']  # or depth_in_struc
    dmg = row['bldgdmgpct']

    # Surge-based (primary, from Figure 9)
    if surge > 12:
        return 'HIGH'
    elif surge >= 9:
        return 'MEDIUM'
    elif surge >= 4:
        return 'LOW'

    # Damage-based fallback (from Figure 10)
    if dmg > 35:
        return 'HIGH'
    elif dmg > 15:
        return 'MEDIUM'
    elif dmg > 0:
        return 'LOW'

    return 'NONE'  # not affected
```

**Note**: Michael said his storm surge thresholds (4/9/12 ft) were "gut feeling" numbers — he explicitly asked us to validate or refine them. This is an area where we add value.

### 3.4 Affected vs Impacted

Per ARC definitions:
- **Affected**: Everyone in the disaster area (all buildings in surge zone, including those with minor or no structural damage)
- **Impacted**: Subset with actual structural damage causing need for mass care (buildings with `bldgdmgpct > 0` or `depth_in_struc > 0`)

From FAST data:
- **Population Affected** = all residential buildings where `depth_grid > 0` (any surge exposure)
- **Population Impacted** = residential buildings where `bldgdmgpct > threshold` (actual damage requiring displacement)

---

## 4. Current Implementation

> The original pipeline described here used AWS Athena and scripts `04_classify_lmh.py`, `05_format_for_spreadsheet.py`, `06_validate_lmh.py`. These have been replaced by local DuckDB + Colab.

The L/M/H classification described in Sections 1-3 is now implemented as:

- **Current Colab path**: `notebooks/shelter_demand.ipynb` orchestrates Excel parameters, NHC raster download, affected-state inference, NSI loading, FAST input preparation, FAST execution, damage classification, BHI computation, Census/SVI joins, and CSV/XLSX export.
- **FAST input helper**: `scripts/duckdb_fast_pipeline.py` provides the DuckDB SQL transformation from NSI Parquet → FAST CSV and remains the canonical source for FAST column mapping.

See `docs/e2e_pipeline.md` for the full architecture diagram with cell-to-stage mapping.

---

## 5. What Was Reused from Original Design

| Component | Status | Outcome |
|-----------|--------|---------|
| DuckDB spatial filter (replaced Athena) | Working | **Reused** — raster bbox filtering |
| FAST building predictions | Local Parquet | **Reused** — primary data source |
| Census population | Downloaded | **Reused** — same data |
| Colab notebook framework | Working | **Reused** — restructured for BHI pipeline |
| Excel config interface | Working | **Reused** — now reads storm params + thresholds |
| Tier 2 ML calibration (XGBoost) | R²=-0.308 | **Dropped** — deterministic approach chosen |
| Tier 3 EVT uncertainty | Working | **Dropped** — ARC has own planning multipliers |
| Ground Truth comparison | 56 rows | **Reframed** — historical reference for calibration/review, not a separate pipeline entrypoint |

---

## 6. Value We Add Beyond "Just Formatting FAST Output"

1. **Automating Factor 2 & 3**: Currently ARC planners estimate L/M/H zones manually from maps. We automate this using building-level FAST damage data — every building is objectively classified by its actual surge depth and damage percentage.

2. **Refining Michael's Storm Surge Thresholds**: Michael admitted his 4/9/12 ft thresholds were "gut feeling." We can validate these against FAST damage distributions — at what surge depths do we see the damage patterns matching High/Medium/Low criteria in Figure 10?

3. **Pre-event Prediction Pipeline**: The full chain from NHC advisory → P-Surge raster → FAST → county L/M/H populations can run in Colab, giving ARC a tool to generate planning numbers BEFORE landfall.

4. **Consistency**: Replace subjective "draw zones on a map" with data-driven classification of every building in the affected area.

---

## 7. Validation Strategy (New)

Instead of LOEO-CV on an ML model, validate by:

1. **Back-testing historical events**: Run the pipeline on 9 historical events, compare our L/M/H population counts against what ARC actually used in their planning spreadsheets.

2. **Threshold sensitivity analysis**: Vary surge depth thresholds (±2 ft) and damage thresholds (±5%) to see how output changes — helps Michael calibrate his "gut feeling" numbers.

3. **Sanity checks**:
   - `pop_affected_high + pop_affected_med + pop_affected_low ≤ county_pop`
   - `pop_impacted_X ≤ pop_affected_X` for each intensity level
   - Zero surge → zero affected/impacted

---

## 8. Timeline

| Phase | Task | Effort |
|-------|------|--------|
| **Phase 1** | New Athena SQL with L/M/H classification | 1-2 hours |
| **Phase 2** | Python aggregation + Census join | 1-2 hours |
| **Phase 3** | Colab notebook (new version) | 1-2 hours |
| **Phase 4** | Validation against GT + threshold analysis | 2-3 hours |
| **Phase 5** | Excel template matching Planning Assumptions format | 1 hour |

---

## 9. Key Decisions Needed

1. **Surge depth field**: Use `depth_grid` (ground-level surge) or `depth_in_struc` (water inside structure) for intensity classification? `depth_grid` is more appropriate for zone classification; `depth_in_struc` is better for damage/impact determination.

2. **Population source**: Use NSI building count × 2.53 (current approach) or Census block group population? Census is more accurate but requires GIS overlay.

3. **"Affected" threshold**: Is ANY surge exposure (depth_grid > 0) "affected", or only above some minimum? The PDF's Low category starts at 4 ft surge.

4. **Dual classification**: Should we classify by surge depth (Factor 2 intensity) AND by damage percentage (Factor 2 damage level) separately, or combine them?

---

## Appendix: Terminology Alignment

| ARC Term | FAST Data Equivalent | Our Implementation |
|----------|---------------------|-------------------|
| Affected Area | Buildings with `depth_grid > 0` | All RES buildings in surge zone |
| High Intensity Zone | `depth_grid > 12 ft` | FAST query filter |
| Medium Intensity Zone | `depth_grid 9-12 ft` | FAST query filter |
| Low Intensity Zone | `depth_grid 4-8 ft` | FAST query filter |
| Population Affected | People living in affected area | RES buildings × 2.53 persons |
| Population Impacted | People with actual structural damage | RES buildings with `bldgdmgpct > 0` × 2.53 |
| Households Needing Shelter | Impacted × conversion rate | ARC applies: H=5%, M=3%, L=1% |
