from __future__ import annotations

import re
import warnings
from pathlib import Path

import pandas as pd


def get_default_params():
    """Return the hardcoded baseline configuration (fallbacks)."""
    return {
        # ── Storm inputs ──────────────────────────────────────────
        "storm_id": "AL022024",
        "storm_name": "BERYL",
        "advisory": 29,
        "year": 2024,
        # ── FAST engine ───────────────────────────────────────────
        "flood_load_condition": "CoastalA",  # CoastalA | CoastalV | Riverine
        "fast_timeout": 1800,  # FAST subprocess timeout (seconds)
        # ── Damage classification (Hazus standard defaults) ──────
        "DAMAGE_STATE_THRESHOLDS": {
            "Slight": (0, 15),
            "Moderate": (15, 40),
            "Extensive": (40, 60),
            "Complete": (60, 100),
        },
        # ── Tract severity (ARC Figure 10 thresholds) ────────────
        "TRACT_SEVERITY": {
            "high": {"pct_destroyed": 0.35, "pct_major_damage": 0.35},
            "medium": {"pct_destroyed": 0.11, "pct_major_damage": 0.16},
        },
        "DAMAGE_SEVERITY": {
            "high": {"pct_destroyed": 0.35, "pct_major_damage": 0.35},
            "medium": {"pct_destroyed": 0.11, "pct_major_damage": 0.16},
        },
        # ── Building Habitability Index (BHI) ────────────────────
        "BLDNG_USABILITY": {
            "Slight": {"FU": 1.00, "PU": 0.00, "NU": 0.00},
            "Moderate": {"FU": 0.87, "PU": 0.13, "NU": 0.00},
            "Extensive": {"FU": 0.25, "PU": 0.50, "NU": 0.25},
            "Complete": {"FU": 0.00, "PU": 0.02, "NU": 0.98},
        },
        # ── Utility Loss Severity — [low, high] ranges ───────────
        "UL_SEVERITY": {
            "low": {"FU": [0.00, 0.05], "PU": [0.05, 0.10]},
            "medium": {"FU": [0.00, 0.10], "PU": [0.30, 0.50]},
            "high": {"FU": [0.10, 0.30], "PU": [0.60, 0.80]},
        },
        # ── SVI configuration ────────────────────────────────────
        "SVI_SHELTER_RATES": [0.000, 0.025, 0.050],
        "SVI_BINS": [0.4, 0.8],  # bin edges for low/med/high SVI
        "PERCENT_IMPACT": {
            "high": 5.0,
            "medium": 2.5,
            "low": 0.0,
        },
        "geography": "census tract",
        # ── Network / performance ────────────────────────────────
        "download_timeout": 60,  # raster + census API timeout (s)
        "svi_timeout": 300,  # CDC SVI download timeout (s)
        "census_max_workers": 6,  # concurrent census API threads (positive int)
        "svi_rest_page_size": 2000,  # CDC REST API records per page (positive int)
        # ── Output file names (written under WORK_DIR) ───────────
        "output_csv_name": "shelter_demand_output.csv",
        "output_xlsx_name": "shelter_demand_output.xlsx",
    }


def nan_to_none(value):
    if pd.isna(value):
        return None
    return value


def parse_range_pct(value):
    """Parse '0.11 - 0.34' style values into the first percentage bound."""
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        match = re.search(r"([\d\.]+)", value)
        if match:
            parsed = float(match.group(1))
            # Normalize integer percentages like "11 - 34" into 0.11.
            if parsed > 1.0:
                return parsed / 100.0
            return parsed
    return None


def deep_update(base, overrides):
    for key, value in overrides.items():
        if isinstance(value, dict) and key in base and isinstance(base[key], dict):
            deep_update(base[key], value)
        else:
            base[key] = value
    return base


def _warn_default(message: str) -> None:
    warnings.warn(message, UserWarning, stacklevel=2)


def _read_optional_storm_inputs(df: pd.DataFrame) -> dict[str, object]:
    extracted: dict[str, object] = {}

    value = nan_to_none(df.iloc[5, 2])
    if value is not None:
        extracted["storm_id"] = str(value)

    value = nan_to_none(df.iloc[6, 2])
    if value is not None:
        extracted["storm_name"] = str(value)

    value = nan_to_none(df.iloc[7, 2])
    if value is not None:
        extracted["advisory"] = int(value)

    value = nan_to_none(df.iloc[8, 2])
    if value is not None:
        extracted["year"] = int(value)

    return extracted


def _extract_severity_override(pct_destroyed, pct_major_damage) -> dict[str, float]:
    override: dict[str, float] = {}
    if pct_destroyed is not None:
        override["pct_destroyed"] = pct_destroyed
    if pct_major_damage is not None:
        override["pct_major_damage"] = pct_major_damage
    return override


def _read_optional_damage_severity(df: pd.DataFrame) -> dict[str, dict[str, dict[str, float]]]:
    try:
        severity_overrides: dict[str, dict[str, float]] = {}

        high_override = _extract_severity_override(
            parse_range_pct(df.iloc[12, 2]),
            parse_range_pct(df.iloc[12, 4]),
        )
        if high_override:
            severity_overrides["high"] = high_override

        medium_override = _extract_severity_override(
            parse_range_pct(df.iloc[13, 2]),
            parse_range_pct(df.iloc[13, 4]),
        )
        if medium_override:
            severity_overrides["medium"] = medium_override
    except IndexError:
        return {}

    if not severity_overrides:
        return {}

    return {
        "TRACT_SEVERITY": {
            level: values.copy() for level, values in severity_overrides.items()
        },
        "DAMAGE_SEVERITY": {
            level: values.copy() for level, values in severity_overrides.items()
        },
    }


def _read_optional_svi_rates(df: pd.DataFrame) -> dict[str, list[float]]:
    try:
        raw_values = [
            nan_to_none(df.iloc[17, 3]),
            nan_to_none(df.iloc[18, 3]),
            nan_to_none(df.iloc[19, 3]),
        ]
    except IndexError:
        return {}

    if all(value is None for value in raw_values):
        return {}

    if any(value is None for value in raw_values):
        return {}

    return {"SVI_SHELTER_RATES": [float(value) for value in raw_values]}


def load_config_from_excel(xlsx_path):
    """
    Read the Interface sheet and merge non-empty values over default parameters.
    """
    params = get_default_params()

    xlsx_path = Path(xlsx_path)
    if not xlsx_path.exists():
        _warn_default(f"Config file {xlsx_path} not found. Continuing with default parameters.")
        return params

    try:
        df = pd.read_excel(xlsx_path, sheet_name="Interface", header=None)
    except Exception as exc:
        _warn_default(
            f"Failed to read {xlsx_path} ({exc}). Continuing with default parameters."
        )
        return params

    extracted: dict[str, object] = {}
    extracted.update(_read_optional_storm_inputs(df))
    extracted.update(_read_optional_damage_severity(df))
    extracted.update(_read_optional_svi_rates(df))

    return deep_update(params, extracted)


if __name__ == "__main__":
    test_path = Path(__file__).parent.parent / "data" / "ARC Storm Surge Shelter Demand.xlsx"
    loaded_params = load_config_from_excel(test_path)

    import pprint

    pprint.pprint(loaded_params)
