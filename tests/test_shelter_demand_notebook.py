"""Regression tests for the shelter demand Colab notebook."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

NOTEBOOK_PATH = Path(__file__).resolve().parent.parent / "notebooks" / "shelter_demand.ipynb"


@pytest.fixture(scope="module")
def code_cells() -> list[str]:
    """Parse notebook once per test module and return code cell sources."""
    notebook = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    return ["".join(cell.get("source", [])) for cell in notebook["cells"] if cell["cell_type"] == "code"]


def _find_cell(code_cells: list[str], header: str) -> str:
    """Find a code cell by its header comment. Raises AssertionError if missing."""
    cell = next((c for c in code_cells if c.startswith(header)), None)
    assert cell is not None, f"Notebook is missing cell: {header}"
    return cell


@pytest.mark.parametrize(
    "header, expected_fragments",
    [
        (
            "# Cell 5: Spatial Filter + FAST Input Preparation",
            [
                "fast_csv_path = str(WORK_DIR / 'fast_input.csv')",
                "nsi_cbfips_join_path = str(WORK_DIR / 'nsi_cbfips_join.csv')",
                "nsi_filtered = (",
            ],
        ),
        (
            "# Cell 7: Load Predictions + Derive Census GEOID",
            [
                "_nsi_join = WORK_DIR / 'nsi_cbfips_join.csv'",
                "pd.read_csv(_nsi_join, dtype={'fltyid': str, 'cbfips': str})",
            ],
        ),
    ],
    ids=["fast_input_preparation", "predictions_cbfips_join"],
)
def test_notebook_cell_contains_expected_code(
    code_cells: list[str], header: str, expected_fragments: list[str]
) -> None:
    """Verify critical notebook cells contain required code fragments."""
    cell = _find_cell(code_cells, header)
    for fragment in expected_fragments:
        assert fragment in cell, f"Cell '{header}' missing: {fragment}"
