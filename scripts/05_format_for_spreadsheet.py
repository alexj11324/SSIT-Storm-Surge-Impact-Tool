#!/usr/bin/env python3
"""Step 5: Join Census population data and format output for ARC's
Planning Assumptions Spreadsheet.

Reads the wide-format L/M/H county features from Phase 1, joins Census
county population, applies ARC mass-care conversion rates (shelter,
feeding), runs sanity checks, and exports CSV + Excel.

Outputs:
  - outputs/planning_assumptions_output.csv
  - outputs/arc_planning_template_lmh.xlsx  (two sheets: Estimates, Parameters)

Usage:
    python 05_format_for_spreadsheet.py [--input data/county_lmh_features.csv]
                                        [--census data/census_county_population.csv]
                                        [--output-dir outputs/]
"""

from __future__ import annotations

import argparse
import io
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# ARC Mass Care Conversion Rates (Planning Assumptions Figure 16)
SHELTER_RATES = {"high": 0.05, "medium": 0.03, "low": 0.01}
FEEDING_RATES = {"high": 0.12, "medium": 0.07, "low": 0.03}

# SVI bump: when intensity zone is HIGH, scale pop_impacted upward by
# (1 + SVI_HIGH_BUMP_WEIGHT * svi_score).  svi_score is CDC RPL_THEMES (0-1).
# Example: SVI=0 → no bump; SVI=0.5 → +10%; SVI=1.0 → +20%.
SVI_HIGH_BUMP_WEIGHT = 0.20

# CDC SVI 2022 county-level dataset URL
CDC_SVI_URL = "https://svi.cdc.gov/Documents/Data/2022/csv/SVI_2022_US_county.csv"

ZONES = ["low", "medium", "high"]

OUTPUT_COLUMNS = [
    "event",
    "county_fips5",
    "county_name",
    "state",
    "census_pop",
    "pop_affected_low",
    "pop_affected_medium",
    "pop_affected_high",
    "pop_impacted_low",
    "pop_impacted_medium",
    "pop_impacted_high",
    "hh_shelter_low",
    "hh_shelter_medium",
    "hh_shelter_high",
    "hh_feeding_low",
    "hh_feeding_medium",
    "hh_feeding_high",
    "svi_score",
    "svi_bump_applied",
]

# Census API (ACS 5-year, 2022)
CENSUS_API_BASE = "https://api.census.gov/data/2022/acs/acs5"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Census data helpers
# ---------------------------------------------------------------------------


def _safe_int(val) -> int:
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return 0


def fetch_census_population(output_path: Path) -> pd.DataFrame:
    """Fetch county-level population from Census API and save to CSV."""
    log("Fetching Census ACS 5-year population data from API...")

    var_str = "NAME,B01001_001E"
    url = f"{CENSUS_API_BASE}?get={var_str}&for=county:*&in=state:*"

    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f"Census API request failed: {e}")

    if not data or len(data) < 2:
        raise RuntimeError("Census API returned empty data")

    headers = data[0]
    rows = []
    for row in data[1:]:
        record = dict(zip(headers, row))
        state_fips = record.get("state", "")
        county_fips = record.get("county", "")
        county_fips5 = f"{state_fips}{county_fips}"

        name_raw = record.get("NAME", "")
        # NAME format: "County Name, State Name"
        parts = name_raw.rsplit(", ", 1)
        county_name = parts[0] if parts else name_raw
        state_name = parts[1] if len(parts) > 1 else ""

        rows.append(
            {
                "county_fips5": county_fips5,
                "county_name": county_name,
                "state": state_name,
                "total_population": _safe_int(record.get("B01001_001E", 0)),
            }
        )

    df = pd.DataFrame(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    log(f"  Census data: {len(df)} counties -> {output_path}")
    return df


def load_census(census_path: Path) -> pd.DataFrame:
    """Load Census population data from local CSV or fetch from API.

    Ensures the returned DataFrame has columns:
        county_fips5, county_name, state, total_population
    """
    if census_path.exists():
        log(f"Loading Census data from {census_path}")
        df = pd.read_csv(census_path, dtype={"county_fips5": str})
        # Ensure fips is zero-padded to 5 digits
        df["county_fips5"] = df["county_fips5"].str.zfill(5)

        # Normalise column names from Census CSV output
        rename_map = {}
        if "county_name_census" in df.columns and "county_name" not in df.columns:
            rename_map["county_name_census"] = "county_name"
        df.rename(columns=rename_map, inplace=True)

        # Derive state from NAME field if missing
        if "state" not in df.columns and "county_name" in df.columns:
            # county_name format: "County Name, State"
            split = df["county_name"].str.rsplit(", ", n=1, expand=True)
            df["state"] = split[1] if split.shape[1] > 1 else ""
            df["county_name"] = split[0]

        # Ensure required columns
        for col in ("county_fips5", "county_name", "state", "total_population"):
            if col not in df.columns:
                raise RuntimeError(f"Census CSV missing required column '{col}'. Available: {list(df.columns)}")

        log(f"  Loaded {len(df)} counties from local CSV")
        return df[["county_fips5", "county_name", "state", "total_population"]]

    # CSV not found — fetch from API
    log(f"Census CSV not found at {census_path}. Fetching from Census API...")
    return fetch_census_population(census_path)


# ---------------------------------------------------------------------------
# SVI data helpers
# ---------------------------------------------------------------------------


def fetch_svi_data(output_path: Path) -> pd.DataFrame:
    """Download CDC SVI 2022 county-level data and save locally.

    Returns DataFrame with columns: county_fips5, svi_score (RPL_THEMES).
    """
    log("Fetching CDC SVI 2022 county-level data...")
    try:
        resp = requests.get(CDC_SVI_URL, timeout=120)
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError("CDC SVI download failed") from e

    df = pd.read_csv(io.StringIO(resp.text), dtype={"FIPS": str})

    # RPL_THEMES is the overall SVI percentile (0-1); -999 means missing
    df = df[["FIPS", "RPL_THEMES"]].copy()
    df.rename(columns={"FIPS": "county_fips5", "RPL_THEMES": "svi_score"}, inplace=True)
    df["county_fips5"] = df["county_fips5"].str.zfill(5)
    df.loc[df["svi_score"] < 0, "svi_score"] = 0.0  # replace -999 sentinel

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    log(f"  SVI data: {len(df)} counties -> {output_path}")
    return df


def load_svi(svi_path: Path) -> pd.DataFrame:
    """Load SVI data from local CSV or fetch from CDC.

    Returns DataFrame with columns: county_fips5, svi_score
    """
    if svi_path.exists():
        log(f"Loading SVI data from {svi_path}")
        df = pd.read_csv(svi_path, dtype={"county_fips5": str})
        df["county_fips5"] = df["county_fips5"].str.zfill(5)
        if "svi_score" not in df.columns:
            raise RuntimeError(f"SVI CSV missing 'svi_score' column. Available: {list(df.columns)}")
        log(f"  Loaded SVI for {len(df)} counties")
        return df[["county_fips5", "svi_score"]]

    log(f"SVI CSV not found at {svi_path}. Downloading from CDC...")
    return fetch_svi_data(svi_path)


def apply_svi_bump(df: pd.DataFrame, bump_weight: float = SVI_HIGH_BUMP_WEIGHT) -> pd.DataFrame:
    """Apply SVI-based bump to HIGH intensity zone population estimates.

    For HIGH zones only: pop_impacted_high *= (1 + bump_weight * svi_score)
    This reflects that socially vulnerable communities in severely damaged areas
    generate disproportionately higher demand for Red Cross services.
    """
    if "svi_score" not in df.columns:
        log("  WARNING: No SVI data available — skipping SVI bump")
        df["svi_bump_applied"] = False
        return df

    has_svi = df["svi_score"] > 0
    bump_factor = 1 + bump_weight * df["svi_score"]

    # Only bump HIGH zone impacted population
    df["pop_impacted_high"] = df["pop_impacted_high"].where(~has_svi, df["pop_impacted_high"] * bump_factor)

    df["svi_bump_applied"] = has_svi
    n_bumped = has_svi.sum()
    avg_bump = (bump_factor[has_svi].mean() - 1) * 100 if n_bumped > 0 else 0
    log(f"  SVI bump applied to {n_bumped} county-events (avg bump: +{avg_bump:.1f}% on HIGH zone)")
    return df


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def load_lmh_features(input_path: Path) -> pd.DataFrame:
    """Load Phase 1 wide-format L/M/H features."""
    log(f"Loading L/M/H features from {input_path}")
    df = pd.read_csv(input_path, dtype={"county_fips5": str})
    df["county_fips5"] = df["county_fips5"].str.zfill(5)

    # Verify expected columns exist
    expected_pop_cols = [f"pop_affected_{z}" for z in ZONES] + [f"pop_impacted_{z}" for z in ZONES]
    missing = [c for c in expected_pop_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"L/M/H features file missing columns: {missing}. Available: {list(df.columns)}")

    log(f"  Loaded {len(df)} rows, {df['event'].nunique()} events")
    return df


def apply_conversion_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Compute shelter and feeding household estimates from pop_impacted."""
    for zone in ZONES:
        df[f"hh_shelter_{zone}"] = df[f"pop_impacted_{zone}"] * SHELTER_RATES[zone]
        df[f"hh_feeding_{zone}"] = df[f"pop_impacted_{zone}"] * FEEDING_RATES[zone]
    return df


def run_sanity_checks(df: pd.DataFrame) -> pd.DataFrame:
    """Post-processing sanity checks with clipping and warnings."""
    n_issues = 0

    # Check 1: pop_impacted <= pop_affected per zone
    for zone in ZONES:
        imp_col = f"pop_impacted_{zone}"
        aff_col = f"pop_affected_{zone}"
        violations = df[imp_col] > df[aff_col]
        if violations.any():
            count = violations.sum()
            log(f"  WARNING: {count} rows have {imp_col} > {aff_col} — clipping")
            df[imp_col] = df[[imp_col, aff_col]].min(axis=1)
            n_issues += count

    # Check 2: sum(pop_affected_*) <= census_pop
    if "census_pop" in df.columns:
        total_affected = sum(df[f"pop_affected_{z}"] for z in ZONES)
        has_census = df["census_pop"] > 0
        excess_mask = has_census & (total_affected > df["census_pop"])

        if excess_mask.any():
            count = excess_mask.sum()
            log(f"  WARNING: {count} rows have total pop_affected > census_pop — scaling down")
            scale = df.loc[excess_mask, "census_pop"] / total_affected[excess_mask]
            for zone in ZONES:
                df.loc[excess_mask, f"pop_affected_{zone}"] = df.loc[excess_mask, f"pop_affected_{zone}"] * scale
                # Re-clip impacted after scaling affected
                imp_col = f"pop_impacted_{zone}"
                aff_col = f"pop_affected_{zone}"
                over = df[imp_col] > df[aff_col]
                if over.any():
                    df.loc[over, imp_col] = df.loc[over, aff_col]
            n_issues += count

    # Check 3: All values >= 0
    numeric_cols = [
        c
        for c in df.columns
        if any(c.startswith(p) for p in ("pop_affected_", "pop_impacted_", "hh_shelter_", "hh_feeding_"))
    ]
    for col in numeric_cols:
        neg_mask = df[col] < 0
        if neg_mask.any():
            count = neg_mask.sum()
            log(f"  WARNING: {count} negative values in {col} — setting to 0")
            df.loc[neg_mask, col] = 0
            n_issues += count

    # Check 4: No NaN — fill with 0
    nan_count = df[numeric_cols].isna().sum().sum()
    if nan_count > 0:
        log(f"  WARNING: {nan_count} NaN values found — filling with 0")
        df[numeric_cols] = df[numeric_cols].fillna(0)
        n_issues += nan_count

    if n_issues == 0:
        log("  All sanity checks passed")
    else:
        log(f"  Sanity checks: {n_issues} total issues fixed")

    return df


def round_estimates(df: pd.DataFrame) -> pd.DataFrame:
    """Round shelter and feeding estimates to integers."""
    for zone in ZONES:
        df[f"hh_shelter_{zone}"] = df[f"hh_shelter_{zone}"].round(0).astype(int)
        df[f"hh_feeding_{zone}"] = df[f"hh_feeding_{zone}"].round(0).astype(int)
    return df


def print_event_summary(df: pd.DataFrame) -> None:
    """Print per-event summary to stdout."""
    print("\n" + "=" * 80)
    print("EVENT SUMMARY — ARC Planning Assumptions")
    print("=" * 80)

    events = sorted(df["event"].unique())
    for event in events:
        edf = df[df["event"] == event]
        n_counties = len(edf)

        total_affected = sum(edf[f"pop_affected_{z}"].sum() for z in ZONES)
        total_impacted = sum(edf[f"pop_impacted_{z}"].sum() for z in ZONES)
        total_shelter = sum(edf[f"hh_shelter_{z}"].sum() for z in ZONES)
        total_feeding = sum(edf[f"hh_feeding_{z}"].sum() for z in ZONES)

        print(f"\n  {event}")
        print(f"    Counties:        {n_counties:>8,d}")
        print(f"    Pop Affected:    {total_affected:>12,.0f}")
        print(f"    Pop Impacted:    {total_impacted:>12,.0f}")
        print(f"    HH Shelter Est:  {total_shelter:>12,d}")
        print(f"    HH Feeding Est:  {total_feeding:>12,d}")

        # Per-zone breakdown
        for zone in ZONES:
            aff = edf[f"pop_affected_{zone}"].sum()
            imp = edf[f"pop_impacted_{zone}"].sum()
            sh = edf[f"hh_shelter_{zone}"].sum()
            print(f"      {zone.upper():>6s}:  affected={aff:>10,.0f}  impacted={imp:>10,.0f}  shelter={sh:>8,d}")

    print("\n" + "=" * 80)


def export_csv(df: pd.DataFrame, output_path: Path) -> None:
    """Export formatted CSV with exact column order."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Select and order columns, filling missing with 0
    out = df.copy()
    for col in OUTPUT_COLUMNS:
        if col not in out.columns:
            out[col] = 0

    out = out[OUTPUT_COLUMNS]
    out.to_csv(output_path, index=False)
    log(f"CSV exported: {len(out)} rows -> {output_path}")


def export_excel(
    df: pd.DataFrame,
    output_path: Path,
    svi_bump_weight: float,
    svi_enabled: bool,
) -> None:
    """Export Excel workbook with Estimates and Parameters sheets."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Prepare estimates sheet
    out = df.copy()
    for col in OUTPUT_COLUMNS:
        if col not in out.columns:
            out[col] = 0
    out = out[OUTPUT_COLUMNS]

    # Prepare parameters sheet
    svi_weight_value = svi_bump_weight if svi_enabled else 0.0
    params_data = {
        "Parameter": [
            "Shelter Rate — High",
            "Shelter Rate — Medium",
            "Shelter Rate — Low",
            "Feeding Rate — High",
            "Feeding Rate — Medium",
            "Feeding Rate — Low",
            "Surge Threshold — High (ft)",
            "Surge Threshold — Medium (ft)",
            "Surge Threshold — Low (ft)",
            "Damage Threshold — High (%)",
            "Damage Threshold — Medium (%)",
            "Damage Threshold — Low (%)",
            "Data Source — Population",
            "Data Source — Surge/Damage",
            "Data Source — Census",
            "Data Source — SVI",
            "SVI Bump Enabled",
            "SVI Bump Weight (HIGH zone only)",
            "Generated",
        ],
        "Value": [
            SHELTER_RATES["high"],
            SHELTER_RATES["medium"],
            SHELTER_RATES["low"],
            FEEDING_RATES["high"],
            FEEDING_RATES["medium"],
            FEEDING_RATES["low"],
            12,
            9,
            4,
            35,
            15,
            0,
            "NSI building-level (pop2pmu65 + pop2pmo65) or bldg count x 2.53",
            "FAST predictions via Athena (arc_storm_surge.predictions)",
            "Census ACS 5-year 2022 (B01001_001E)",
            "CDC SVI 2022 county-level (RPL_THEMES, 0-1 percentile)",
            svi_enabled,
            svi_weight_value,
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        ],
    }
    params_df = pd.DataFrame(params_data)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        out.to_excel(writer, sheet_name="Estimates", index=False)
        params_df.to_excel(writer, sheet_name="Parameters", index=False)

    log(f"Excel exported: {output_path}")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Format L/M/H county estimates for ARC Planning Assumptions Spreadsheet"
    )
    parser.add_argument(
        "--input",
        default="data/county_lmh_features.csv",
        help="Path to Phase 1 wide-format L/M/H features CSV (default: data/county_lmh_features.csv)",
    )
    parser.add_argument(
        "--census",
        default="data/census_county_population.csv",
        help="Path to Census county population CSV (default: data/census_county_population.csv)",
    )
    parser.add_argument(
        "--svi",
        default="data/svi_county.csv",
        help="Path to CDC SVI county CSV (default: data/svi_county.csv; fetched from CDC if missing)",
    )
    parser.add_argument(
        "--svi-bump-weight",
        type=float,
        default=SVI_HIGH_BUMP_WEIGHT,
        help=f"SVI bump weight for HIGH zones (default: {SVI_HIGH_BUMP_WEIGHT})",
    )
    parser.add_argument(
        "--no-svi",
        action="store_true",
        help="Disable SVI bump adjustment",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs/",
        help="Output directory (default: outputs/)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    census_path = Path(args.census)
    svi_path = Path(args.svi)
    output_dir = Path(args.output_dir)
    svi_enabled = not args.no_svi
    svi_bump_weight = args.svi_bump_weight if svi_enabled else 0.0
    output_dir.mkdir(parents=True, exist_ok=True)

    # -----------------------------------------------------------------------
    # Step 1: Load Phase 1 output
    # -----------------------------------------------------------------------
    log("Step 1: Loading L/M/H features")
    lmh_df = load_lmh_features(input_path)

    # -----------------------------------------------------------------------
    # Step 2: Load Census population
    # -----------------------------------------------------------------------
    log("Step 2: Loading Census population")
    census_df = load_census(census_path)

    # -----------------------------------------------------------------------
    # Step 3: Join Census to L/M/H features
    # -----------------------------------------------------------------------
    log("Step 3: Joining Census population to L/M/H features")
    df = lmh_df.merge(
        census_df[["county_fips5", "county_name", "state", "total_population"]],
        on="county_fips5",
        how="left",
    )
    df.rename(columns={"total_population": "census_pop"}, inplace=True)

    # Report unmatched counties
    unmatched = df["census_pop"].isna().sum()
    if unmatched > 0:
        log(f"  WARNING: {unmatched} county-event rows have no Census match — setting census_pop=0")
        df["census_pop"] = df["census_pop"].fillna(0)
    else:
        log(f"  All {len(df)} rows matched Census data")

    matched_counties = df[df["census_pop"] > 0]["county_fips5"].nunique()
    log(f"  {matched_counties} unique counties with Census population")

    # Fill missing county_name / state
    df["county_name"] = df["county_name"].fillna("")
    df["state"] = df["state"].fillna("")

    # -----------------------------------------------------------------------
    # Step 3b: Load and join SVI data
    # -----------------------------------------------------------------------
    if svi_enabled:
        log("Step 3b: Loading CDC SVI data")
        svi_df = load_svi(svi_path)
        df = df.merge(svi_df, on="county_fips5", how="left")
        matched_svi = df["svi_score"].notna().sum()
        df["svi_score"] = df["svi_score"].fillna(0.0)
        log(f"  {matched_svi}/{len(df)} county-events matched SVI data")

        # Step 3c: Apply SVI bump on HIGH intensity zones
        log("Step 3c: Applying SVI bump to HIGH intensity zones")
        df = apply_svi_bump(df, bump_weight=svi_bump_weight)
        df["svi_bump_weight"] = svi_bump_weight
    else:
        log("Step 3b: SVI bump disabled (--no-svi)")
        df["svi_score"] = 0.0
        df["svi_bump_applied"] = False
        df["svi_bump_weight"] = 0.0

    # -----------------------------------------------------------------------
    # Step 4: Apply ARC conversion rates
    # -----------------------------------------------------------------------
    log("Step 4: Applying ARC conversion rates")
    df = apply_conversion_rates(df)

    # -----------------------------------------------------------------------
    # Step 5: Sanity checks
    # -----------------------------------------------------------------------
    log("Step 5: Running sanity checks")
    df = run_sanity_checks(df)

    # -----------------------------------------------------------------------
    # Step 6: Round shelter/feeding to integers
    # -----------------------------------------------------------------------
    log("Step 6: Rounding estimates to integers")
    df = round_estimates(df)

    # -----------------------------------------------------------------------
    # Step 7: Export CSV
    # -----------------------------------------------------------------------
    csv_path = output_dir / "planning_assumptions_output.csv"
    log("Step 7: Exporting CSV")
    export_csv(df, csv_path)

    # -----------------------------------------------------------------------
    # Step 8: Export Excel
    # -----------------------------------------------------------------------
    xlsx_path = output_dir / "arc_planning_template_lmh.xlsx"
    log("Step 8: Exporting Excel")
    export_excel(
        df,
        xlsx_path,
        svi_bump_weight=svi_bump_weight,
        svi_enabled=svi_enabled,
    )

    # -----------------------------------------------------------------------
    # Step 9: Event summary
    # -----------------------------------------------------------------------
    print_event_summary(df)

    log(f"Done. CSV: {csv_path}, Excel: {xlsx_path}")


if __name__ == "__main__":
    main()
