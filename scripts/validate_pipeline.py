#!/usr/bin/env python3
"""End-to-end validation for the FAST pipeline output predictions."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path


def load_predictions(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def validate_schema(rows: list[dict[str, str]]) -> list[str]:
    """Check that required output columns exist."""
    if not rows:
        return ["FAIL: predictions CSV is empty"]
    required = {"FltyId", "Latitude", "Longitude", "state", "flc"}
    missing = required - set(rows[0].keys())
    if missing:
        return [f"FAIL: missing columns: {missing}"]
    return []


def compute_summary(rows: list[dict[str, str]]) -> dict:
    """Compute aggregate statistics from prediction rows."""
    by_state: dict[str, int] = Counter()
    by_flc: dict[str, int] = Counter()
    by_occ: dict[str, int] = Counter()
    damage_by_state: dict[str, float] = defaultdict(float)
    zero_loss = 0
    total = len(rows)

    for row in rows:
        state = row.get("state", "?")
        flc = row.get("flc", "?")
        by_state[state] += 1
        by_flc[flc] += 1

        occ = row.get("Occ", row.get("occ", "?"))
        by_occ[occ] += 1

        # Try to find a damage/loss column (FAST output varies)
        loss = 0.0
        for col in ("BldgLoss", "BldgDmgPct", "TotalLoss", "bldg_loss"):
            val = row.get(col, "")
            if val:
                try:
                    loss = float(val)
                except ValueError:
                    pass
                break

        if loss <= 0:
            zero_loss += 1
        damage_by_state[state] += loss

    return {
        "total_rows": total,
        "zero_loss_rows": zero_loss,
        "zero_loss_pct": round(100 * zero_loss / total, 2) if total else 0,
        "rows_by_state": dict(sorted(by_state.items())),
        "rows_by_flc": dict(sorted(by_flc.items())),
        "rows_by_occ_top10": dict(Counter(by_occ).most_common(10)),
        "damage_by_state": {k: round(v, 2) for k, v in sorted(damage_by_state.items())},
    }


def run_checks(summary: dict) -> list[str]:
    """Run validation checks and return issues."""
    issues = []
    if summary["total_rows"] == 0:
        issues.append("FAIL: no prediction rows")
        return issues

    if summary["zero_loss_pct"] > 90:
        issues.append(
            f"WARN: {summary['zero_loss_pct']}% zero-loss rows — likely spatial mismatch (buildings outside raster)"
        )

    if len(summary["rows_by_state"]) == 0:
        issues.append("FAIL: no states in output")

    if not summary["rows_by_flc"]:
        issues.append("FAIL: no FLC categories in output")

    return issues


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate FAST pipeline predictions")
    parser.add_argument("predictions_csv", help="Path to merged predictions CSV")
    parser.add_argument("--output-json", default=None, help="Write report JSON to this path")
    args = parser.parse_args()

    csv_path = Path(args.predictions_csv)
    if not csv_path.exists():
        print(f"ERROR: file not found: {csv_path}")
        return 1

    rows = load_predictions(csv_path)
    schema_issues = validate_schema(rows)
    summary = compute_summary(rows)
    check_issues = run_checks(summary)
    all_issues = schema_issues + check_issues

    report = {
        "file": str(csv_path),
        "summary": summary,
        "issues": all_issues,
        "passed": len([i for i in all_issues if i.startswith("FAIL")]) == 0,
    }

    print(json.dumps(report, indent=2))

    if args.output_json:
        out = Path(args.output_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2))

    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
