# L/M/H Pipeline Validation Report

Generated: 2026-03-10T18:20:05.573225

## 1. Overview

This report compares the L/M/H intensity zone pipeline output (Phase 2: `planning_assumptions_output.csv`) against historical ARC Ground Truth shelter population data.

- Matched county-event rows: **55**
- Events with overlap: **5**
- Events matched: ['BERYL_2024', 'FLORENCE_2018', 'HELENE_2024', 'IDA_2021', 'MILTON_2024']

## 2. Overall Metrics

| Metric | L/M/H Pipeline | Tier 1 Baseline | Tier 2 Baseline |
|--------|---------------|-----------------|-----------------|
| RMSE | **1550.9** | 546.6 | 407.6 |
| MAE | **908.8** | 315.9 | 225.8 |
| R-squared | **-17.598** | N/A | -0.308 |
| N (matched rows) | 55 | 56 | 56 |

### Interpretation

The L/M/H pipeline has higher RMSE than both previous baselines. This is expected if the conversion rates need calibration. The key advantage of L/M/H is producing output in the format ARC actually uses (Low/Medium/High zone populations), not raw shelter predictions.

**Important**: The purpose of the L/M/H pipeline is NOT to beat ML accuracy, but to produce zone-based population estimates in the format matching ARC's Planning Assumptions Spreadsheet (columns J-R).

## 3. Per-Event Breakdown

| Event | N Counties | RMSE | MAE | R-squared | Mean GT | Mean Est |
|-------|-----------|------|-----|-----------|---------|----------|
| BERYL_2024 | 4 | 649.2 | 583.0 | -28.411 | 105.8 | 688.8 |
| FLORENCE_2018 | 9 | 865.9 | 645.8 | -1.081 | 707.4 | 115.0 |
| HELENE_2024 | 16 | 2419.1 | 1361.5 | -5821.228 | 34.4 | 1393.4 |
| IDA_2021 | 17 | 962.4 | 642.1 | -31.293 | 179.1 | 814.7 |
| MILTON_2024 | 9 | 1268.6 | 1015.6 | -25.815 | 237.6 | 1253.1 |

## 4. Top 10 Largest Discrepancies

| Event | County FIPS | County | GT Shelter | LMH Estimate | Abs Error | Direction |
|-------|-----------|--------|-----------|-------------|-----------|-----------|
| HELENE_2024 | 12103 | Pinellas | 117 | 6342 | 6225 | OVER |
| HELENE_2024 | 12057 | Hillsborough | 26 | 5883 | 5857 | OVER |
| HELENE_2024 | 12101 | Pasco | 91 | 2972 | 2881 | OVER |
| HELENE_2024 | 12015 | Charlotte | 19 | 2546 | 2527 | OVER |
| IDA_2021 | 22051 | Jefferson | 448 | 2892 | 2444 | OVER |
| IDA_2021 | 22071 | Orleans | 392 | 2615 | 2223 | OVER |
| HELENE_2024 | 12081 | Manatee | 33 | 2162 | 2129 | OVER |
| MILTON_2024 | 12015 | Charlotte | 49 | 2105 | 2056 | OVER |
| MILTON_2024 | 12115 | Sarasota | 432 | 2224 | 1792 | OVER |
| FLORENCE_2018 | 37129 | New Hanover | 1762 | 37 | 1725 | UNDER |

## 5. Comparison with Previous Approaches

| Approach | RMSE | MAE | R-squared | Notes |
|----------|------|-----|-----------|-------|
| Tier 1 (flat 0.73% shelter rate) | 546.6 | 315.9 | N/A | Simple: displaced_pop x 0.73% |
| Tier 2 (ML ensemble LOEO-CV) | 407.6 | 225.8 | -0.308 | XGBoost/RF/Ridge with LOEO-CV |
| **L/M/H Pipeline** | **1550.9** | **908.8** | **-17.598** | Zone-based classification + ARC conversion rates |

## 6. Threshold Sensitivity Analysis

| Variant | Low (ft) | Medium (ft) | High (ft) | Status |
|---------|---------|------------|----------|--------|
| default | >= 4 | >= 9 | > 12 | requires Athena re-run |
| tight | >= 3 | >= 8 | > 11 | requires Athena re-run |
| loose | >= 5 | >= 10 | > 13 | requires Athena re-run |
| very_low | >= 2 | >= 6 | > 10 | requires Athena re-run |

**Note**: Full reclassification requires re-running the pipeline (Phase 1) with modified surge thresholds. The current pipeline uses the 'default' thresholds (LOW >= 4ft, MEDIUM >= 9ft, HIGH > 12ft). To test alternative thresholds, modify the classification logic in `notebooks/shelter_demand.ipynb` and re-run Phases 1-2.

## 7. Sanity Check Results

| Check | Severity | Violations |
|-------|----------|------------|
| pop_impacted_low <= pop_affected_low | OK | 0 |
| pop_impacted_medium <= pop_affected_medium | OK | 0 |
| pop_impacted_high <= pop_affected_high | OK | 0 |
| zero-affected counties have zero impacted | OK | 0 |

## 8. Sample Predictions (Top Counties by GT Shelter Population)

| Event | County FIPS | County | GT Shelter Pop | Total Shelter Est | Shelter Low | Shelter Medium | Shelter High |
|---|---|---|---|---|---|---|---|
| FLORENCE_2018 | 37129 | New Hanover | 1762 | 37 | 4 | 6 | 27 |
| FLORENCE_2018 | 37133 | Onslow | 1549 | 88 | 7 | 27 | 54 |
| FLORENCE_2018 | 37019 | Brunswick | 1047 | 72 | 2 | 2 | 68 |
| FLORENCE_2018 | 37049 | Craven | 781 | 197 | 21 | 93 | 83 |
| MILTON_2024 | 12057 | Hillsborough | 726 | 2415 | 0 | 1 | 2414 |
| FLORENCE_2018 | 37031 | Carteret | 534 | 218 | 32 | 39 | 147 |
| IDA_2021 | 22109 | Terrebonne | 509 | 1947 | 147 | 478 | 1322 |
| FLORENCE_2018 | 37141 | Pender | 483 | 47 | 1 | 4 | 42 |
| IDA_2021 | 22051 | Jefferson | 448 | 2892 | 2742 | 9 | 141 |
| MILTON_2024 | 12115 | Sarasota | 432 | 2224 | 48 | 286 | 1890 |
| IDA_2021 | 22033 | East Baton Rouge | 422 | 389 | 0 | 0 | 389 |
| MILTON_2024 | 12103 | Pinellas | 403 | 522 | 1 | 2 | 519 |
| MILTON_2024 | 12081 | Manatee | 397 | 844 | 14 | 37 | 793 |
| IDA_2021 | 22071 | Orleans | 392 | 2615 | 2597 | 3 | 15 |
| IDA_2021 | 22105 | Tangipahoa | 311 | 459 | 4 | 14 | 441 |
