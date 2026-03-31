from __future__ import annotations

import subprocess
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "duckdb_fast_pipeline.py"


def test_cli_accepts_deprecated_flc_flag() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--parquet-glob",
            "dummy.parquet",
            "--raster",
            "missing.tif",
            "--output",
            "out.csv",
            "--flc",
            "CoastalA",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 2
    assert "unrecognized arguments: --flc CoastalA" not in result.stderr
