from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import scripts.read_excel_config as read_excel_config


def test_load_config_reads_major_damage_from_column_e(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: % Major Damage lives in column E on the Interface sheet."""
    df = pd.DataFrame([[pd.NA] * 5 for _ in range(20)])
    df.iloc[12, 2] = 0.35
    df.iloc[12, 3] = 0.91
    df.iloc[12, 4] = 0.12
    df.iloc[13, 2] = "0.11 - 0.34"
    df.iloc[13, 3] = "0.92 - 0.99"
    df.iloc[13, 4] = "0.22 - 0.33"

    config_path = tmp_path / "interface.xlsx"
    config_path.touch()

    monkeypatch.setattr(read_excel_config.pd, "read_excel", lambda *args, **kwargs: df)

    params = read_excel_config.load_config_from_excel(config_path)

    assert params["TRACT_SEVERITY"]["high"]["pct_major_damage"] == pytest.approx(0.12)
    assert params["TRACT_SEVERITY"]["medium"]["pct_major_damage"] == pytest.approx(0.22)
    assert params["DAMAGE_SEVERITY"]["high"]["pct_major_damage"] == pytest.approx(0.12)
    assert params["DAMAGE_SEVERITY"]["medium"]["pct_major_damage"] == pytest.approx(0.22)


def test_load_config_keeps_new_runtime_defaults_when_excel_omits_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Notebook runtime defaults should still exist after Excel overlay."""
    df = pd.DataFrame([[pd.NA] * 5 for _ in range(20)])
    config_path = tmp_path / "interface.xlsx"
    config_path.touch()

    monkeypatch.setattr(read_excel_config.pd, "read_excel", lambda *args, **kwargs: df)

    params = read_excel_config.load_config_from_excel(config_path)

    assert params["geography"] == "census tract"
    assert params["PERCENT_IMPACT"] == {
        "high": pytest.approx(5.0),
        "medium": pytest.approx(2.5),
        "low": pytest.approx(0.0),
    }


def test_load_config_merges_partial_damage_severity_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A single non-empty threshold cell should override only that field."""
    df = pd.DataFrame([[pd.NA] * 5 for _ in range(20)])
    df.iloc[12, 4] = 0.12

    config_path = tmp_path / "interface.xlsx"
    config_path.touch()

    monkeypatch.setattr(read_excel_config.pd, "read_excel", lambda *args, **kwargs: df)

    params = read_excel_config.load_config_from_excel(config_path)

    assert params["DAMAGE_SEVERITY"]["high"]["pct_destroyed"] == pytest.approx(0.35)
    assert params["DAMAGE_SEVERITY"]["high"]["pct_major_damage"] == pytest.approx(0.12)


def test_load_config_keeps_default_svi_rates_when_excel_values_are_incomplete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Incomplete SVI rows should not raise and should keep the default triplet."""
    df = pd.DataFrame([[pd.NA] * 5 for _ in range(20)])
    df.iloc[17, 3] = 0.1
    df.iloc[18, 3] = pd.NA
    df.iloc[19, 3] = 0.3

    config_path = tmp_path / "interface.xlsx"
    config_path.touch()

    monkeypatch.setattr(read_excel_config.pd, "read_excel", lambda *args, **kwargs: df)

    params = read_excel_config.load_config_from_excel(config_path)

    assert params["SVI_SHELTER_RATES"] == pytest.approx([0.0, 0.025, 0.05])


def test_load_config_warns_for_missing_file(tmp_path: Path) -> None:
    """Missing workbook should emit a warning that callers can capture."""
    missing_path = tmp_path / "missing.xlsx"

    with pytest.warns(UserWarning, match="not found"):
        params = read_excel_config.load_config_from_excel(missing_path)

    assert params["storm_id"] == "AL022024"
