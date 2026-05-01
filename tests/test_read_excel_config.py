from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import scripts.read_excel_config as read_excel_config


def test_load_config_reads_current_interface_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Read storm inputs, building filters, damage categories, and geography."""
    df = pd.DataFrame([[pd.NA] * 5 for _ in range(33)])
    df.iloc[5, 2] = " al142018 "
    df.iloc[6, 2] = " michael "
    df.iloc[7, 2] = 20
    df.iloc[8, 2] = 2018
    df.iloc[12, 2] = "y"
    df.iloc[13, 2] = "n"
    df.iloc[26, 2] = "Destroyed"
    df.iloc[27, 2] = "Major"
    df.iloc[32, 2] = " Census Tract "

    config_path = tmp_path / "interface.xlsx"
    config_path.touch()

    monkeypatch.setattr(read_excel_config.pd, "read_excel", lambda *args, **kwargs: df)

    params = read_excel_config.load_config_from_excel(config_path)

    assert params["storm_id"] == "AL142018"
    assert params["storm_name"] == "MICHAEL"
    assert params["advisory"] == 20
    assert params["year"] == 2018
    assert params["BUILDING_TYPES"]["RES1"] == "Y"
    assert params["BUILDING_TYPES"]["RES2"] == "N"
    assert params["DAMAGE_CATEGORIES"][">9"] == "DESTROYED"
    assert params["DAMAGE_CATEGORIES"][">6"] == "MAJOR"
    assert params["geography"] == "census tract"


def test_load_config_keeps_new_runtime_defaults_when_excel_omits_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Notebook runtime defaults should still exist after Excel overlay."""
    df = pd.DataFrame([[pd.NA] * 5 for _ in range(33)])
    config_path = tmp_path / "interface.xlsx"
    config_path.touch()

    monkeypatch.setattr(read_excel_config.pd, "read_excel", lambda *args, **kwargs: df)

    params = read_excel_config.load_config_from_excel(config_path)

    assert params["geography"] == "county"
    assert params["flood_load_condition"] == "CoastalA"
    assert params["BUILDING_TYPES"]["RES1"] == ""
    assert params["DAMAGE_CATEGORIES"][">9"] == ""
    assert params["DAMAGE_STATE_THRESHOLDS"]["Complete"] == (60, 100)
    assert params["DAMAGE_SEVERITY"]["high"]["pct_destroyed"] == 0.35
    assert params["PERCENT_IMPACT"]["medium"] == 30
    assert params["output_csv_name"] == "shelter_demand_output.csv"


def test_load_config_reads_flood_load_condition_from_interface_label(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The FAST flood setting should come from the spreadsheet when present."""
    df = pd.DataFrame([[pd.NA] * 5 for _ in range(36)])
    df.iloc[33, 0] = "Flood Load Condition"
    df.iloc[33, 2] = " coastalv "

    config_path = tmp_path / "interface.xlsx"
    config_path.touch()

    monkeypatch.setattr(read_excel_config.pd, "read_excel", lambda *args, **kwargs: df)

    params = read_excel_config.load_config_from_excel(config_path)

    assert params["flood_load_condition"] == "CoastalV"


def test_load_config_merges_partial_building_type_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A single non-empty building type cell should override only that field."""
    df = pd.DataFrame([[pd.NA] * 5 for _ in range(33)])
    df.iloc[12, 2] = "Y"

    config_path = tmp_path / "interface.xlsx"
    config_path.touch()

    monkeypatch.setattr(read_excel_config.pd, "read_excel", lambda *args, **kwargs: df)

    params = read_excel_config.load_config_from_excel(config_path)

    assert params["BUILDING_TYPES"]["RES1"] == "Y"
    assert params["BUILDING_TYPES"]["RES2"] == ""


def test_load_config_handles_short_interface_sheet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Optional rows below the provided sheet should keep defaults."""
    df = pd.DataFrame([[pd.NA] * 5 for _ in range(20)])

    config_path = tmp_path / "interface.xlsx"
    config_path.touch()

    monkeypatch.setattr(read_excel_config.pd, "read_excel", lambda *args, **kwargs: df)

    params = read_excel_config.load_config_from_excel(config_path)

    assert params["geography"] == "county"


def test_load_config_warns_for_missing_file(tmp_path: Path) -> None:
    """Missing workbook should emit a warning that callers can capture."""
    missing_path = tmp_path / "missing.xlsx"

    with pytest.warns(UserWarning, match="not found"):
        params = read_excel_config.load_config_from_excel(missing_path)

    assert params["storm_id"] == ""
