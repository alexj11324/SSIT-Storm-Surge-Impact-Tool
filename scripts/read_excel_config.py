from __future__ import annotations

import re
import warnings
from pathlib import Path

import pandas as pd


def get_default_params():
    """Return the hardcoded baseline configuration (fallbacks)."""
    return {
        # ── Storm inputs ──────────────────────────────────────────
        "storm_id": "",
        "storm_name": "",
        "advisory": 00,
        "year": 0000,
        # ── FAST engine ───────────────────────────────────────────
        "flood_load_condition": "CoastalA",  # CoastalA | CoastalV | Riverine
        "fast_timeout": 1800,  # FAST subprocess timeout (seconds)
        # ── Residential building types ────────────────────────────
        "BUILDING_TYPES": {
            "RES1": "",
            "RES2": "",
            "RES3A": "",
            "RES3B": "",
            "RES3C": "",
            "RES3D": "",
            "RES3E": "",
            "RES3F": "",
            "RES4": "",
            "RES5": "",
            "RES6": "",
        },
        # ── Damage assessment categories ──────────────────────────
        "DAMAGE_CATEGORIES": {">9": "", ">6": "", ">3": "", ">1": ""},
        # FAST BldgDmgPct damage-state thresholds (percent, 0-100).
        "DAMAGE_STATE_THRESHOLDS": {
            "Slight": (0, 15),
            "Moderate": (15, 40),
            "Extensive": (40, 60),
            "Complete": (60, 100),
        },
        # ARC L/M/H severity thresholds, expressed as fractions of buildings.
        "DAMAGE_SEVERITY": {
            "high": {"pct_destroyed": 0.35, "pct_major_damage": 0.35},
            "medium": {"pct_destroyed": 0.11, "pct_major_damage": 0.16},
        },
        # Population impacted rates by L/M/H severity. Values are percentages.
        "PERCENT_IMPACT": {"high": 60, "medium": 30, "low": 10},
        # ── Geography ─────────────────────────────────────────────
        "geography": "county",
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


def optional_cell(df: pd.DataFrame, row: int, col: int):
    try:
        return nan_to_none(df.iloc[row, col])
    except IndexError:
        return None


def clean_text(value, *, default: str = "") -> str:
    if value is None or pd.isna(value):
        return default
    return str(value).strip()


def normalize_flood_load_condition(value) -> str:
    raw = clean_text(value, default="CoastalA")
    key = re.sub(r"[\s_-]+", "", raw).lower()
    return {
        "coastala": "CoastalA",
        "coastalv": "CoastalV",
        "riverine": "Riverine",
    }.get(key, raw)


def normalize_params(params: dict[str, object]) -> dict[str, object]:
    params["storm_id"] = clean_text(params.get("storm_id")).upper()
    params["storm_name"] = clean_text(params.get("storm_name")).upper()
    params["geography"] = clean_text(params.get("geography"), default="county").lower()
    params["flood_load_condition"] = normalize_flood_load_condition(
        params.get("flood_load_condition")
    )
    return params


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


def _read_storm_inputs(df: pd.DataFrame) -> dict[str, object]:
    extracted: dict[str, object] = {}

    value = optional_cell(df, 5, 2)
    if value is not None:
        extracted["storm_id"] = clean_text(value).upper()

    value = optional_cell(df, 6, 2)
    if value is not None:
        extracted["storm_name"] = clean_text(value).upper()

    value = optional_cell(df, 7, 2)
    if value is not None:
        extracted["advisory"] = int(value)

    value = optional_cell(df, 8, 2)
    if value is not None:
        extracted["year"] = int(value)

    return extracted


def _read_optional_building_type(df: pd.DataFrame) -> dict[str, dict[str, str]]:
    building_types: dict[str, str] = {}

    res_types = ["1", "2", "3A", "3B", "3C", "3D", "3E", "3F", "4", "5", "6"]
    idx = list(range(12, 23))
    for res, ix in zip(res_types, idx):
        ind = optional_cell(df, ix, 2)
        if ind is not None:
            building_types["RES" + str(res)] = clean_text(ind).upper()

    if not building_types:
        return {}

    return {"BUILDING_TYPES": {res: ind for res, ind in building_types.items()}}


def _read_optional_damage_categories(df: pd.DataFrame) -> dict[str, dict[str, str]]:
    damage_categories: dict[str, str] = {}

    ht_vals = [">9", ">6", ">3", ">1"]
    idx = list(range(26, 30))
    for ht, ix in zip(ht_vals, idx):
        cat = optional_cell(df, ix, 2)
        if cat is not None:
            damage_categories[str(ht)] = clean_text(cat).upper()

    if not damage_categories:
        return {}

    return {"DAMAGE_CATEGORIES": {val: cat for val, cat in damage_categories.items()}}


def _read_geography(df: pd.DataFrame) -> dict[str, object]:
    extracted: dict[str, object] = {}

    value = optional_cell(df, 32, 2)
    if value is not None:
        extracted["geography"] = clean_text(value).lower()

    return extracted


def load_config_from_excel(xlsx_path):
    """
    Read the Interface sheet and merge non-empty values over default parameters.
    """
    params = get_default_params()

    xlsx_path = Path(xlsx_path)
    if not xlsx_path.exists():
        _warn_default(f"Config file {xlsx_path} not found. Continuing with default parameters.")
        return normalize_params(params)

    try:
        df = pd.read_excel(xlsx_path, sheet_name="Interface", header=None)
    except Exception as exc:
        _warn_default(f"Failed to read {xlsx_path} ({exc}). Continuing with default parameters.")
        return normalize_params(params)

    extracted: dict[str, object] = {}
    extracted.update(_read_storm_inputs(df))
    extracted.update(_read_optional_building_type(df))
    extracted.update(_read_optional_damage_categories(df))
    extracted.update(_read_geography(df))

    return normalize_params(deep_update(params, extracted))


if __name__ == "__main__":
    test_path = Path(__file__).parent.parent / "data" / "ARC Storm Surge Shelter Demand.xlsx"
    loaded_params = load_config_from_excel(test_path)

    import pprint

    pprint.pprint(loaded_params)
