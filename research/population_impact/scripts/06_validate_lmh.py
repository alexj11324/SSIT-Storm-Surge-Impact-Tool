#!/usr/bin/env python3
"""Step 6: Validate L/M/H pipeline output against historical Ground Truth data.

Loads the Phase 2 planning assumptions output (county-level L/M/H population
estimates with shelter/feeding conversion), joins with ARC Ground Truth shelter
data, and computes comparison metrics.

Outputs:
  - lmh_validation_report.md  -- full validation report with metrics, tables
  - lmh_validation_joined.csv -- the joined GT vs estimate dataset

Usage:
    python 06_validate_lmh.py \
        --input outputs/planning_assumptions_output.csv \
        --gt "../../data/Ground Truth Data.xlsx" \
        --output-dir outputs
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Event name mapping (Ground Truth uses short names, our pipeline uses EVENT_YEAR)
# GT uses short event names; our pipeline uses EVENT_YEAR format
# ---------------------------------------------------------------------------
GT_EVENT_MAP = {
    "Beryl": "BERYL_2024",
    "Debby": "DEBBY_2024",
    "Florence": "FLORENCE_2018",
    "Helene": "HELENE_2024",
    "Ian": "IAN_2022",
    "Idalia": "IDALIA_2023",
    "Ida": "IDA_2021",
    "Michael": "MICHAEL_2018",
    "Milton": "MILTON_2024",
}

# Baseline metrics from previous approaches (deprecated ML pipeline)
BASELINE_TIER1 = {"name": "Tier 1 (flat 0.73% shelter rate)", "rmse": 546.6, "mae": 315.9, "r2": None}
BASELINE_TIER2 = {"name": "Tier 2 (ML ensemble LOEO-CV)", "rmse": 407.6, "mae": 225.8, "r2": -0.308}

# Threshold variants for sensitivity analysis
THRESHOLD_VARIANTS = [
    {"name": "default", "low": 4, "med": 9, "high": 12},
    {"name": "tight", "low": 3, "med": 8, "high": 11},
    {"name": "loose", "low": 5, "med": 10, "high": 13},
    {"name": "very_low", "low": 2, "med": 6, "high": 10},
]


def log(msg: str) -> None:
    print(f"[validate-lmh] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def load_ground_truth(path: Path) -> pd.DataFrame:
    """Load and normalize Ground Truth Excel.

    Normalizes event names via GT_EVENT_MAP and zero-pads county FIPS to 5 digits.
    """
    raw = pd.read_excel(path, engine="openpyxl")
    df = pd.DataFrame()
    df["event"] = raw["Event"].map(GT_EVENT_MAP)
    df["county_fips5"] = (
        raw["State FIPS"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(2)
        + raw["County FIPS"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(3)
    )
    df["gt_county_name"] = raw["County"]
    df["gt_state_name"] = raw["State"]
    df["planned_shelter"] = pd.to_numeric(
        raw["Planned Shelter Population"], errors="coerce"
    )
    df["actual_shelter"] = pd.to_numeric(
        raw["Actual Shelter Population"], errors="coerce"
    )
    # Use actual if available, otherwise planned
    df["shelter_pop"] = df["actual_shelter"].fillna(df["planned_shelter"])
    df = df.dropna(subset=["event", "county_fips5"]).copy()
    log(f"Ground Truth: {len(df)} rows, {df['event'].nunique()} events")
    return df


def load_lmh_output(path: Path) -> pd.DataFrame:
    """Load Phase 2 planning assumptions output CSV."""
    df = pd.read_csv(path)
    # Normalize county_fips5 to 5-digit zero-padded string
    df["county_fips5"] = (
        df["county_fips5"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(5)
    )
    log(f"LMH output: {len(df)} rows, columns={list(df.columns)}")
    return df


# ---------------------------------------------------------------------------
# Join + Metrics
# ---------------------------------------------------------------------------

def join_gt_and_estimates(
    lmh: pd.DataFrame, gt: pd.DataFrame
) -> pd.DataFrame:
    """Inner join on (event, county_fips5).

    Expects ~56 matches across 5 events (Beryl, Florence, Helene, Ida, Milton).
    """
    # Ensure consistent dtypes
    lmh = lmh.copy()
    gt = gt.copy()
    lmh["county_fips5"] = lmh["county_fips5"].astype(str).str.zfill(5)
    gt["county_fips5"] = gt["county_fips5"].astype(str).str.zfill(5)

    # Diagnostic: per-event match info
    log("Join diagnostics:")
    for event in sorted(gt["event"].dropna().unique()):
        gt_fips = set(gt.loc[gt["event"] == event, "county_fips5"])
        lmh_fips = set(lmh.loc[lmh["event"] == event, "county_fips5"])
        overlap = gt_fips & lmh_fips
        log(
            f"  {event}: GT={len(gt_fips)} counties, LMH={len(lmh_fips)} counties, "
            f"Match={len(overlap)}"
        )

    merged = pd.merge(
        lmh,
        gt[["event", "county_fips5", "shelter_pop", "planned_shelter",
            "actual_shelter", "gt_county_name", "gt_state_name"]],
        on=["event", "county_fips5"],
        how="inner",
    )
    log(f"After inner join: {len(merged)} matched rows across "
        f"{merged['event'].nunique()} events")
    return merged


def compute_metrics(
    y_true: np.ndarray, y_pred: np.ndarray
) -> dict:
    """Compute RMSE, MAE, R2 using sklearn (metrics only)."""
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    mask = np.isfinite(y_true) & np.isfinite(y_pred)
    y_true = y_true[mask]
    y_pred = y_pred[mask]

    if len(y_true) == 0:
        return {"rmse": np.nan, "mae": np.nan, "r2": np.nan, "n": 0}

    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    r2 = float(r2_score(y_true, y_pred)) if len(y_true) > 1 else np.nan
    return {"rmse": rmse, "mae": mae, "r2": r2, "n": int(len(y_true))}


# ---------------------------------------------------------------------------
# Sanity Checks
# ---------------------------------------------------------------------------

def run_sanity_checks(df: pd.DataFrame) -> list[dict]:
    """Validate physical constraints on the LMH output."""
    issues = []

    # Check 1: pop_impacted <= pop_affected for each zone
    for zone in ["low", "medium", "high"]:
        imp_col = f"pop_impacted_{zone}"
        aff_col = f"pop_affected_{zone}"
        if imp_col in df.columns and aff_col in df.columns:
            violations = df[df[imp_col] > df[aff_col] + 0.01]  # small float tolerance
            issues.append({
                "check": f"pop_impacted_{zone} <= pop_affected_{zone}",
                "severity": "CRITICAL" if len(violations) > 0 else "OK",
                "n_violations": len(violations),
            })

    # Check 2: All values >= 0
    pop_cols = [c for c in df.columns if c.startswith(("pop_", "hh_"))]
    for col in pop_cols:
        if col in df.columns:
            neg = df[df[col] < 0]
            if len(neg) > 0:
                issues.append({
                    "check": f"{col} >= 0",
                    "severity": "CRITICAL",
                    "n_violations": len(neg),
                })

    # Check 3: Zero surge counties have zero affected/impacted
    # (Approximate: if all pop_affected zones are 0, pop_impacted should be 0 too)
    aff_cols = [c for c in df.columns if c.startswith("pop_affected_")]
    imp_cols = [c for c in df.columns if c.startswith("pop_impacted_")]
    if aff_cols and imp_cols:
        total_aff = df[aff_cols].sum(axis=1)
        total_imp = df[imp_cols].sum(axis=1)
        zero_aff_nonzero_imp = df[(total_aff == 0) & (total_imp > 0)]
        issues.append({
            "check": "zero-affected counties have zero impacted",
            "severity": "WARNING" if len(zero_aff_nonzero_imp) > 0 else "OK",
            "n_violations": len(zero_aff_nonzero_imp),
        })

    if not issues:
        issues.append({"check": "all_passed", "severity": "OK", "n_violations": 0})

    return issues


# ---------------------------------------------------------------------------
# Threshold Sensitivity
# ---------------------------------------------------------------------------

def threshold_sensitivity_analysis(
    long_csv_path: Path | None,
) -> list[dict] | None:
    """Test alternative surge thresholds against the long-format CSV.

    If the long-format CSV (county_lmh_long.csv) from Phase 1 is available,
    re-classify buildings under different threshold variants and report how
    county totals change. If not available, return None (noted as TODO).
    """
    if long_csv_path is None or not long_csv_path.exists():
        log("  Threshold sensitivity: long-format CSV not available. "
            "Skipping reclassification (requires re-running Phase 1 with Athena).")
        return None

    log(f"  Threshold sensitivity: loading {long_csv_path}")
    long_df = pd.read_csv(long_csv_path)

    # The long-format CSV has per-building surge/damage data aggregated to
    # county x zone. We cannot re-classify individual buildings from this
    # aggregation. Full reclassification needs the building-level data
    # (Athena query). We note this as a limitation.
    log("  NOTE: Full threshold reclassification requires re-running the Athena "
        "query with modified thresholds. Reporting current zone distribution only.")

    results = []
    for variant in THRESHOLD_VARIANTS:
        # Report the threshold set; actual reclassification needs Athena re-run
        results.append({
            "name": variant["name"],
            "low_ft": variant["low"],
            "med_ft": variant["med"],
            "high_ft": variant["high"],
            "status": "requires Athena re-run",
        })

    return results


# ---------------------------------------------------------------------------
# Report Generation
# ---------------------------------------------------------------------------

def generate_report(
    joined: pd.DataFrame,
    overall_metrics: dict,
    per_event_metrics: list[dict],
    sanity_issues: list[dict],
    threshold_results: list[dict] | None,
    output_dir: Path,
) -> Path:
    """Generate markdown validation report."""
    lines = [
        "# L/M/H Pipeline Validation Report",
        "",
        f"Generated: {pd.Timestamp.now().isoformat()}",
        "",
        "## 1. Overview",
        "",
        "This report compares the L/M/H intensity zone pipeline output "
        "(Phase 2: `planning_assumptions_output.csv`) against historical "
        "ARC Ground Truth shelter population data.",
        "",
        f"- Matched county-event rows: **{len(joined)}**",
        f"- Events with overlap: **{joined['event'].nunique()}**",
        f"- Events matched: {sorted(joined['event'].unique())}",
        "",
    ]

    # --- Section 2: Overall Metrics ---
    lines.extend([
        "## 2. Overall Metrics",
        "",
        "| Metric | L/M/H Pipeline | Tier 1 Baseline | Tier 2 Baseline |",
        "|--------|---------------|-----------------|-----------------|",
    ])
    r2_lmh = f"{overall_metrics['r2']:.3f}" if not np.isnan(overall_metrics.get("r2", np.nan)) else "N/A"
    r2_t1 = f"{BASELINE_TIER1['r2']:.3f}" if BASELINE_TIER1["r2"] is not None else "N/A"
    r2_t2 = f"{BASELINE_TIER2['r2']:.3f}" if BASELINE_TIER2["r2"] is not None else "N/A"

    lines.append(
        f"| RMSE | **{overall_metrics['rmse']:.1f}** | "
        f"{BASELINE_TIER1['rmse']:.1f} | {BASELINE_TIER2['rmse']:.1f} |"
    )
    lines.append(
        f"| MAE | **{overall_metrics['mae']:.1f}** | "
        f"{BASELINE_TIER1['mae']:.1f} | {BASELINE_TIER2['mae']:.1f} |"
    )
    lines.append(f"| R-squared | **{r2_lmh}** | {r2_t1} | {r2_t2} |")
    lines.append(f"| N (matched rows) | {overall_metrics['n']} | 56 | 56 |")
    lines.append("")

    # Interpretation
    lines.extend([
        "### Interpretation",
        "",
    ])
    if overall_metrics["rmse"] <= BASELINE_TIER2["rmse"]:
        lines.append(
            "The L/M/H pipeline achieves lower RMSE than both previous baselines. "
            "This indicates the zone-based approach produces estimates closer to "
            "observed shelter populations."
        )
    elif overall_metrics["rmse"] <= BASELINE_TIER1["rmse"]:
        lines.append(
            "The L/M/H pipeline achieves lower RMSE than Tier 1 (flat rate) but "
            "higher than Tier 2 (ML ensemble). The zone-based approach is a "
            "reasonable middle ground between simplicity and accuracy."
        )
    else:
        lines.append(
            "The L/M/H pipeline has higher RMSE than both previous baselines. "
            "This is expected if the conversion rates need calibration. The key "
            "advantage of L/M/H is producing output in the format ARC actually "
            "uses (Low/Medium/High zone populations), not raw shelter predictions."
        )
    lines.append("")
    lines.append(
        "**Important**: The purpose of the L/M/H pipeline is NOT to beat ML "
        "accuracy, but to produce zone-based population estimates in the format "
        "matching ARC's Planning Assumptions Spreadsheet (columns J-R)."
    )
    lines.append("")

    # --- Section 3: Per-Event Metrics ---
    lines.extend([
        "## 3. Per-Event Breakdown",
        "",
        "| Event | N Counties | RMSE | MAE | R-squared | Mean GT | Mean Est |",
        "|-------|-----------|------|-----|-----------|---------|----------|",
    ])
    for em in per_event_metrics:
        r2_str = f"{em['r2']:.3f}" if not np.isnan(em.get("r2", np.nan)) else "N/A"
        lines.append(
            f"| {em['event']} | {em['n']} | {em['rmse']:.1f} | "
            f"{em['mae']:.1f} | {r2_str} | {em['mean_gt']:.1f} | "
            f"{em['mean_est']:.1f} |"
        )
    lines.append("")

    # --- Section 4: Top Discrepancies ---
    lines.extend([
        "## 4. Top 10 Largest Discrepancies",
        "",
    ])
    if len(joined) > 0 and "total_shelter_est" in joined.columns:
        joined_sorted = joined.copy()
        joined_sorted["abs_error"] = (
            joined_sorted["total_shelter_est"] - joined_sorted["shelter_pop"]
        ).abs()
        top_disc = joined_sorted.sort_values("abs_error", ascending=False).head(10)
        lines.extend([
            "| Event | County FIPS | County | GT Shelter | LMH Estimate | Abs Error | Direction |",
            "|-------|-----------|--------|-----------|-------------|-----------|-----------|",
        ])
        for _, row in top_disc.iterrows():
            gt_val = f"{row['shelter_pop']:.0f}"
            est_val = f"{row['total_shelter_est']:.0f}"
            err_val = f"{row['abs_error']:.0f}"
            direction = "OVER" if row["total_shelter_est"] > row["shelter_pop"] else "UNDER"
            county_name = row.get("gt_county_name", row.get("county_name", ""))
            lines.append(
                f"| {row['event']} | {row['county_fips5']} | {county_name} | "
                f"{gt_val} | {est_val} | {err_val} | {direction} |"
            )
    else:
        lines.append("No matched rows available for discrepancy analysis.")
    lines.append("")

    # --- Section 5: Baseline Comparison ---
    lines.extend([
        "## 5. Comparison with Previous Approaches",
        "",
        "| Approach | RMSE | MAE | R-squared | Notes |",
        "|----------|------|-----|-----------|-------|",
        f"| {BASELINE_TIER1['name']} | {BASELINE_TIER1['rmse']:.1f} | "
        f"{BASELINE_TIER1['mae']:.1f} | {r2_t1} | "
        f"Simple: displaced_pop x 0.73% |",
        f"| {BASELINE_TIER2['name']} | {BASELINE_TIER2['rmse']:.1f} | "
        f"{BASELINE_TIER2['mae']:.1f} | {r2_t2} | "
        f"XGBoost/RF/Ridge with LOEO-CV |",
        f"| **L/M/H Pipeline** | **{overall_metrics['rmse']:.1f}** | "
        f"**{overall_metrics['mae']:.1f}** | **{r2_lmh}** | "
        f"Zone-based classification + ARC conversion rates |",
    ])
    lines.append("")

    # --- Section 6: Threshold Sensitivity ---
    lines.extend([
        "## 6. Threshold Sensitivity Analysis",
        "",
    ])
    if threshold_results is not None:
        lines.extend([
            "| Variant | Low (ft) | Medium (ft) | High (ft) | Status |",
            "|---------|---------|------------|----------|--------|",
        ])
        for tr in threshold_results:
            lines.append(
                f"| {tr['name']} | >= {tr['low_ft']} | >= {tr['med_ft']} | "
                f"> {tr['high_ft']} | {tr['status']} |"
            )
        lines.append("")
        lines.append(
            "**Note**: Full reclassification requires re-running the Athena query "
            "(Phase 1) with modified surge thresholds. The current pipeline uses "
            "the 'default' thresholds (LOW >= 4ft, MEDIUM >= 9ft, HIGH > 12ft). "
            "To test alternative thresholds, modify the CASE expression in "
            "`04_classify_lmh.py` and re-run Phases 1-2."
        )
    else:
        lines.append(
            "Threshold sensitivity analysis was skipped because the long-format "
            "CSV (`data/county_lmh_long.csv`) was not available. This analysis "
            "requires re-running Phase 1 (`04_classify_lmh.py`) with modified "
            "thresholds. **TODO**: Run sensitivity analysis after Athena data is "
            "regenerated."
        )
    lines.append("")

    # --- Section 7: Sanity Checks ---
    lines.extend([
        "## 7. Sanity Check Results",
        "",
        "| Check | Severity | Violations |",
        "|-------|----------|------------|",
    ])
    for issue in sanity_issues:
        lines.append(
            f"| {issue['check']} | {issue['severity']} | {issue['n_violations']} |"
        )
    lines.append("")

    # --- Section 8: Sample Predictions ---
    lines.extend([
        "## 8. Sample Predictions (Top Counties by GT Shelter Population)",
        "",
    ])
    if len(joined) > 0 and "shelter_pop" in joined.columns:
        sample = joined.sort_values("shelter_pop", ascending=False).head(15)
        col_headers = ["Event", "County FIPS", "County", "GT Shelter Pop",
                       "Total Shelter Est"]
        # Add zone-level columns if available
        zone_cols = []
        for z in ["low", "medium", "high"]:
            col = f"hh_shelter_{z}"
            if col in sample.columns:
                zone_cols.append(col)
                col_headers.append(f"Shelter {z.title()}")

        lines.append("| " + " | ".join(col_headers) + " |")
        lines.append("|" + "|".join(["---"] * len(col_headers)) + "|")

        for _, row in sample.iterrows():
            gt_val = f"{row['shelter_pop']:.0f}"
            est_val = f"{row.get('total_shelter_est', 0):.0f}"
            county_name = row.get("gt_county_name", row.get("county_name", ""))
            cells = [
                str(row["event"]),
                str(row["county_fips5"]),
                str(county_name),
                gt_val,
                est_val,
            ]
            for zc in zone_cols:
                cells.append(f"{row.get(zc, 0):.0f}")
            lines.append("| " + " | ".join(cells) + " |")
    else:
        lines.append("No matched rows available.")
    lines.append("")

    # Write report
    report_path = output_dir / "lmh_validation_report.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    log(f"Report written to {report_path}")
    return report_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Validate L/M/H pipeline output against Ground Truth"
    )
    parser.add_argument(
        "--input",
        default="outputs/planning_assumptions_output.csv",
        help="Phase 2 planning assumptions CSV",
    )
    parser.add_argument(
        "--gt",
        default="../../data/Ground Truth Data.xlsx",
        help="Ground Truth Excel file path",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory for validation outputs",
    )
    parser.add_argument(
        "--long-csv",
        default=None,
        help="Optional: long-format CSV from Phase 1 for threshold sensitivity "
             "(default: auto-detect at data/county_lmh_long.csv)",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Resolve paths relative to script directory (for default paths)
    script_dir = Path(__file__).resolve().parent
    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = script_dir / input_path
    gt_path = Path(args.gt)
    if not gt_path.is_absolute():
        gt_path = script_dir / gt_path
    if not output_dir.is_absolute():
        output_dir = script_dir / output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

    # Auto-detect long-format CSV for threshold sensitivity
    long_csv_path = None
    if args.long_csv:
        long_csv_path = Path(args.long_csv)
    else:
        candidate = script_dir / "data" / "county_lmh_long.csv"
        if candidate.exists():
            long_csv_path = candidate

    # ── Step 1: Load data ──
    log("Step 1: Loading data...")
    if not input_path.exists():
        log(f"ERROR: LMH output not found at {input_path}")
        log("  Run Phase 2 (05_format_for_spreadsheet.py) first to generate this file.")
        return
    lmh = load_lmh_output(input_path)

    if not gt_path.exists():
        log(f"ERROR: Ground Truth file not found at {gt_path}")
        return
    gt = load_ground_truth(gt_path)

    # ── Step 2: Join ──
    log("Step 2: Joining estimates with Ground Truth...")
    joined = join_gt_and_estimates(lmh, gt)

    if len(joined) == 0:
        log("ERROR: Zero matched rows. Cannot compute validation metrics.")
        log("  Check that event names and county FIPS codes align between "
            "the LMH output and Ground Truth.")
        return

    # ── Step 3: Compute total shelter estimate ──
    log("Step 3: Computing total shelter estimates...")
    shelter_cols = ["hh_shelter_low", "hh_shelter_medium", "hh_shelter_high"]
    available_shelter_cols = [c for c in shelter_cols if c in joined.columns]

    if available_shelter_cols:
        joined["total_shelter_est"] = joined[available_shelter_cols].fillna(0).sum(axis=1)
        log(f"  Using shelter columns: {available_shelter_cols}")
    else:
        log("  WARNING: No hh_shelter_* columns found. Checking for alternative columns...")
        # Fallback: check if there are pop_impacted columns and apply default rates
        fallback_cols = {
            "pop_impacted_low": 0.01,
            "pop_impacted_medium": 0.03,
            "pop_impacted_high": 0.05,
        }
        available_fb = {c: r for c, r in fallback_cols.items() if c in joined.columns}
        if available_fb:
            joined["total_shelter_est"] = sum(
                joined[col].fillna(0) * rate for col, rate in available_fb.items()
            )
            log(f"  Fallback: applied default ARC conversion rates to pop_impacted columns")
        else:
            log("  ERROR: No shelter or impacted population columns found. Cannot validate.")
            return

    log(f"  Total shelter estimate range: "
        f"[{joined['total_shelter_est'].min():.0f}, "
        f"{joined['total_shelter_est'].max():.0f}], "
        f"mean={joined['total_shelter_est'].mean():.1f}")
    log(f"  GT shelter_pop range: "
        f"[{joined['shelter_pop'].min():.0f}, "
        f"{joined['shelter_pop'].max():.0f}], "
        f"mean={joined['shelter_pop'].mean():.1f}")

    # ── Step 4: Overall metrics ──
    log("Step 4: Computing overall metrics...")
    y_true = joined["shelter_pop"].values.astype(float)
    y_pred = joined["total_shelter_est"].values.astype(float)
    overall_metrics = compute_metrics(y_true, y_pred)
    log(f"  Overall: RMSE={overall_metrics['rmse']:.1f}, "
        f"MAE={overall_metrics['mae']:.1f}, R2={overall_metrics['r2']:.3f}, "
        f"N={overall_metrics['n']}")

    # ── Step 5: Per-event breakdown ──
    log("Step 5: Per-event breakdown...")
    per_event_metrics = []
    for event in sorted(joined["event"].unique()):
        mask = joined["event"] == event
        evt_df = joined[mask]
        evt_true = evt_df["shelter_pop"].values.astype(float)
        evt_pred = evt_df["total_shelter_est"].values.astype(float)
        evt_metrics = compute_metrics(evt_true, evt_pred)
        evt_metrics["event"] = event
        evt_metrics["mean_gt"] = float(np.nanmean(evt_true))
        evt_metrics["mean_est"] = float(np.nanmean(evt_pred))
        per_event_metrics.append(evt_metrics)
        log(f"  {event}: N={evt_metrics['n']}, RMSE={evt_metrics['rmse']:.1f}, "
            f"MAE={evt_metrics['mae']:.1f}")

    # ── Step 6: Sanity checks ──
    log("Step 6: Running sanity checks on LMH output...")
    sanity_issues = run_sanity_checks(lmh)
    for issue in sanity_issues:
        severity_tag = issue["severity"]
        log(f"  [{severity_tag}] {issue['check']}: {issue['n_violations']} violations")

    # ── Step 7: Threshold sensitivity ──
    log("Step 7: Threshold sensitivity analysis...")
    threshold_results = threshold_sensitivity_analysis(long_csv_path)

    # ── Step 8: Baseline comparison summary ──
    log("Step 8: Baseline comparison...")
    log(f"  L/M/H Pipeline:  RMSE={overall_metrics['rmse']:.1f}, "
        f"MAE={overall_metrics['mae']:.1f}")
    log(f"  Tier 1 Baseline: RMSE={BASELINE_TIER1['rmse']:.1f}, "
        f"MAE={BASELINE_TIER1['mae']:.1f}")
    log(f"  Tier 2 Baseline: RMSE={BASELINE_TIER2['rmse']:.1f}, "
        f"MAE={BASELINE_TIER2['mae']:.1f}")

    # ── Step 9: Save joined data ──
    log("Step 9: Saving joined validation data...")
    joined.to_csv(output_dir / "lmh_validation_joined.csv", index=False)
    log(f"  Saved to {output_dir / 'lmh_validation_joined.csv'}")

    # ── Step 10: Generate report ──
    log("Step 10: Generating validation report...")
    report_path = generate_report(
        joined=joined,
        overall_metrics=overall_metrics,
        per_event_metrics=per_event_metrics,
        sanity_issues=sanity_issues,
        threshold_results=threshold_results,
        output_dir=output_dir,
    )

    # Print final summary
    print("\n" + "=" * 60)
    print("L/M/H VALIDATION SUMMARY")
    print("=" * 60)
    print(f"  Matched rows:     {len(joined)}")
    print(f"  Events:           {sorted(joined['event'].unique())}")
    print(f"  Overall RMSE:     {overall_metrics['rmse']:.1f}")
    print(f"  Overall MAE:      {overall_metrics['mae']:.1f}")
    print(f"  Overall R2:       {overall_metrics['r2']:.3f}")
    print(f"  vs Tier 1 RMSE:   {BASELINE_TIER1['rmse']:.1f}")
    print(f"  vs Tier 2 RMSE:   {BASELINE_TIER2['rmse']:.1f}")
    print(f"  Report:           {report_path}")
    print(f"  Joined CSV:       {output_dir / 'lmh_validation_joined.csv'}")
    print("=" * 60)


if __name__ == "__main__":
    main()
