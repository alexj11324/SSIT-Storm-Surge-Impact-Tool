#!/usr/bin/env python3
"""Step 4: Classify FAST building-level predictions into Low/Medium/High
intensity zones and aggregate to county level via Athena.

Joins predictions with NSI (for population + county FIPS from cbfips),
deduplicates across advisories, classifies each building into L/M/H zones
based on surge depth (primary) and damage % (fallback), then aggregates
per (event, county, intensity_zone).

Outputs:
  - county_lmh_long.csv   — long format (one row per event/county/zone)
  - county_lmh_features.csv — wide format (one row per event/county, L/M/H columns)

Usage:
    python 04_classify_lmh.py [--output-dir ./data] [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

REGION = "us-east-1"
WORKGROUP = "primary"
ATHENA_OUTPUT = "s3://red-cross-capstone-project-data/analysis/pop-impact/athena-temp/"
S3_OUTPUT_PREFIX = "s3://red-cross-capstone-project-data/analysis/pop-impact/"
DATABASE = "arc_analysis"
PREDICTIONS_TABLE = "arc_storm_surge.predictions_csv"  # CSV table avoids Parquet INT64/DOUBLE mismatch
# Try multiple possible NSI table references
NSI_TABLE_CANDIDATES = [
    "red_cross_hurricane.nsi_data",
    "red_cross_hurricane.nsi_data_parquet",
]
COUNTY_TABLE = "arc_analysis.us_county_boundaries"

# Average US household size (Census Bureau) — fallback when NSI pop columns unavailable
AVG_HOUSEHOLD_SIZE = 2.53


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


class AthenaClient:
    def __init__(self, region: str = REGION, workgroup: str = WORKGROUP):
        self.client = boto3.client("athena", region_name=region)
        self.workgroup = workgroup

    def execute(self, sql: str, database: str = DATABASE, label: str = "") -> str:
        """Execute query and return query execution ID."""
        log(f"  [{label}] Submitting query...")
        resp = self.client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
            WorkGroup=self.workgroup,
        )
        qid = resp["QueryExecutionId"]
        while True:
            status = self.client.get_query_execution(QueryExecutionId=qid)
            state = status["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(2)
        if state != "SUCCEEDED":
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena [{label}] failed ({qid}): {state} — {reason}")
        stats = status["QueryExecution"].get("Statistics", {})
        scanned_mb = stats.get("DataScannedInBytes", 0) / 1e6
        runtime_s = stats.get("EngineExecutionTimeInMillis", 0) / 1000
        log(f"  [{label}] OK — scanned {scanned_mb:.1f} MB in {runtime_s:.1f}s")
        return qid

    def fetch_rows(self, sql: str, database: str = DATABASE, label: str = "") -> list[dict]:
        qid = self.execute(sql, database, label)
        rows = []
        headers = []
        next_token = None
        first = True
        while True:
            kwargs = {"QueryExecutionId": qid, "MaxResults": 1000}
            if next_token:
                kwargs["NextToken"] = next_token
            resp = self.client.get_query_results(**kwargs)
            data = resp["ResultSet"]["Rows"]
            start = 0
            if first and data:
                headers = [c.get("VarCharValue", "") for c in data[0]["Data"]]
                start = 1
                first = False
            for row in data[start:]:
                vals = [c.get("VarCharValue", "") for c in row["Data"]]
                padded = vals + [""] * (len(headers) - len(vals))
                rows.append(dict(zip(headers, padded)))
            next_token = resp.get("NextToken")
            if not next_token:
                break
        return rows


def discover_nsi_table(athena: AthenaClient) -> str:
    """Find the correct NSI table name in Athena."""
    for candidate in NSI_TABLE_CANDIDATES:
        db, table = candidate.split(".", 1)
        try:
            rows = athena.fetch_rows(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema = '{db}' AND table_name = '{table}' LIMIT 5",
                database="default",
                label=f"probe_{candidate}",
            )
            if rows:
                log(f"  Found NSI table: {candidate}")
                return candidate
        except RuntimeError:
            continue
    raise RuntimeError(f"No NSI table found. Tried: {NSI_TABLE_CANDIDATES}")


def get_nsi_columns(athena: AthenaClient, nsi_table: str) -> set[str]:
    """Get available columns in NSI table."""
    db, table = nsi_table.split(".", 1)
    rows = athena.fetch_rows(
        f"SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = '{db}' AND table_name = '{table}'",
        database="default",
        label="nsi_columns",
    )
    cols = {r["column_name"].lower() for r in rows}
    log(f"  NSI columns ({len(cols)}): {sorted(cols)[:20]}...")
    return cols


def build_lmh_query() -> str:
    """Build the L/M/H intensity zone classification + county aggregation query.

    Strategy:
    1. Deduplicate predictions: take max damage per (event, fltyid) across advisories
    2. Spatial join with county boundaries (ST_CONTAINS) to get county FIPS
       (NSI data was deleted; spatial join replaces NSI cbfips lookup)
    3. Classify each building into LOW/MEDIUM/HIGH intensity zones
    4. Aggregate to county x intensity_zone level
    Population estimated as building count * avg household size (2.53)
    """
    return f"""
WITH
-- Stage 1: Deduplicate predictions across advisories (take worst case per building)
deduped AS (
    SELECT
        event,
        fltyid,
        MAX(CAST(bldgdmgpct AS DOUBLE)) AS bldgdmgpct,
        MAX(CAST(depth_grid AS DOUBLE)) AS depth_grid,
        MAX(CAST(depth_in_struc AS DOUBLE)) AS depth_in_struc,
        MAX(CAST(bldglossusd AS DOUBLE)) AS bldglossusd,
        ARBITRARY(occ) AS occ,
        ARBITRARY(CAST(latitude AS DOUBLE)) AS latitude,
        ARBITRARY(CAST(longitude AS DOUBLE)) AS longitude
    FROM {PREDICTIONS_TABLE}
    GROUP BY event, fltyid
),
-- Stage 2: Spatial join with county boundaries + classify intensity
joined AS (
    SELECT
        d.*,
        c.county_fips5,
        c.county_name,
        c.state_abbr,
        {AVG_HOUSEHOLD_SIZE} AS pop_night,
        CASE
            WHEN d.depth_grid > 12 THEN 'HIGH'
            WHEN d.depth_grid >= 9 THEN 'MEDIUM'
            WHEN d.depth_grid >= 4 THEN 'LOW'
            WHEN d.bldgdmgpct > 35 THEN 'HIGH'
            WHEN d.bldgdmgpct > 15 THEN 'MEDIUM'
            WHEN d.bldgdmgpct > 0 THEN 'LOW'
            ELSE 'NONE'
        END AS intensity_zone,
        CASE WHEN d.bldgdmgpct > 0 OR d.depth_in_struc > 0 THEN 1 ELSE 0 END AS is_impacted
    FROM deduped d
    JOIN {COUNTY_TABLE} c
        ON ST_CONTAINS(
            ST_GEOMETRYFROMTEXT(c.geometry),
            ST_POINT(d.longitude, d.latitude)
        )
    WHERE d.occ LIKE 'RES%'
)
-- Stage 3: County x intensity aggregation
SELECT
    event,
    county_fips5,
    intensity_zone,
    COUNT(*) AS n_buildings_affected,
    SUM(pop_night) AS pop_affected,
    SUM(is_impacted) AS n_buildings_impacted,
    SUM(CASE WHEN is_impacted = 1 THEN pop_night ELSE 0 END) AS pop_impacted,
    AVG(bldgdmgpct) AS avg_damage_pct,
    MAX(depth_grid) AS max_surge_ft,
    SUM(bldglossusd) AS total_loss_usd
FROM joined
WHERE intensity_zone != 'NONE'
  AND county_fips5 IS NOT NULL
GROUP BY event, county_fips5, intensity_zone
ORDER BY event, county_fips5, intensity_zone
"""


def pivot_to_wide(long_rows: list[dict]) -> list[dict]:
    """Pivot long-format (event, county, zone) rows to wide-format using pandas.

    Input:  one row per (event, county_fips5, intensity_zone)
    Output: one row per (event, county_fips5) with L/M/H columns
    """
    import pandas as pd

    long_df = pd.DataFrame(long_rows)

    # Convert numeric columns from string
    numeric_cols = [
        "n_buildings_affected", "pop_affected", "n_buildings_impacted",
        "pop_impacted", "avg_damage_pct", "max_surge_ft", "total_loss_usd",
    ]
    for col in numeric_cols:
        if col in long_df.columns:
            long_df[col] = pd.to_numeric(long_df[col], errors="coerce").fillna(0)

    # Pivot from long to wide
    pivot = long_df.pivot_table(
        index=["event", "county_fips5"],
        columns="intensity_zone",
        values=[
            "pop_affected", "pop_impacted",
            "n_buildings_affected", "n_buildings_impacted",
            "avg_damage_pct", "max_surge_ft", "total_loss_usd",
        ],
        fill_value=0,
    ).reset_index()

    # Flatten multi-level column names
    pivot.columns = [
        "_".join(str(c) for c in col).strip("_").lower()
        if isinstance(col, tuple) else col.lower()
        for col in pivot.columns
    ]

    return pivot.to_dict(orient="records")


def export_to_csv(rows: list[dict], output_path: Path) -> None:
    if not rows:
        log("  No rows to export")
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    log(f"  Exported {len(rows)} rows to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Classify FAST predictions into L/M/H intensity zones "
                    "and aggregate to county level"
    )
    parser.add_argument("--output-dir", default="data", help="Local output directory")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print SQL query without executing")
    parser.add_argument("--s3-output", default=None,
                        help="Athena S3 results path (default: built-in)")
    args = parser.parse_args()

    if args.s3_output is not None:
        global ATHENA_OUTPUT
        ATHENA_OUTPUT = args.s3_output

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        log("Dry run mode — printing SQL without executing")
        sql = build_lmh_query()
        print("\n=== L/M/H CLASSIFICATION QUERY ===")
        print(sql)
        return

    athena = AthenaClient()

    # Build and run L/M/H classification query (spatial join with county boundaries)
    log("Step 1: Building L/M/H classification query...")
    lmh_sql = build_lmh_query()

    log("Step 2: Executing L/M/H classification query...")
    long_rows = athena.fetch_rows(lmh_sql, label="lmh_classification")
    log(f"  Received {len(long_rows)} long-format rows")

    if not long_rows:
        log("ERROR: No rows returned from L/M/H query. Exiting.")
        return

    # Save long-format for debugging
    log("Step 3: Saving long-format results...")
    export_to_csv(long_rows, output_dir / "county_lmh_long.csv")

    # Print summary of long-format results
    events = set(r.get("event", "") for r in long_rows)
    zones = set(r.get("intensity_zone", "") for r in long_rows)
    log(f"  Events: {len(events)} — {sorted(events)}")
    log(f"  Zones: {sorted(zones)}")

    # Pivot to wide format
    log("Step 4: Pivoting to wide format...")
    wide_rows = pivot_to_wide(long_rows)
    log(f"  Wide-format: {len(wide_rows)} county-event rows")

    # Save wide-format
    log("Step 5: Saving wide-format results...")
    export_to_csv(wide_rows, output_dir / "county_lmh_features.csv")

    # Print per-event summary
    print("\n=== L/M/H Classification Summary ===")
    for event in sorted(events):
        event_rows = [r for r in long_rows if r.get("event") == event]
        counties = set(r.get("county_fips5", "") for r in event_rows)
        total_pop = sum(float(r.get("pop_affected", 0)) for r in event_rows)
        total_impacted = sum(float(r.get("pop_impacted", 0)) for r in event_rows)
        zone_counts = {}
        for r in event_rows:
            z = r.get("intensity_zone", "")
            zone_counts[z] = zone_counts.get(z, 0) + int(float(r.get("n_buildings_affected", 0)))
        print(
            f"  {event:20s}  counties={len(counties):>4d}  "
            f"pop_affected={total_pop:>12,.0f}  pop_impacted={total_impacted:>12,.0f}  "
            f"bldgs(L/M/H)={zone_counts.get('LOW', 0):>8,d}/{zone_counts.get('MEDIUM', 0):>8,d}/{zone_counts.get('HIGH', 0):>8,d}"
        )

    # Save metadata
    meta = {
        "script": "04_classify_lmh.py",
        "county_table": COUNTY_TABLE,
        "n_long_rows": len(long_rows),
        "n_wide_rows": len(wide_rows),
        "n_events": len(events),
        "events": sorted(events),
        "zones": sorted(zones),
        "thresholds": {
            "surge_low_ft": 4, "surge_medium_ft": 9, "surge_high_ft": 12,
            "damage_low_pct": 0, "damage_medium_pct": 15, "damage_high_pct": 35,
        },
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    meta_path = output_dir / "county_lmh_metadata.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    log(f"  Metadata saved to {meta_path}")

    log(f"Done. Wide-format: {output_dir}/county_lmh_features.csv "
        f"({len(wide_rows)} rows), Long-format: {output_dir}/county_lmh_long.csv "
        f"({len(long_rows)} rows)")


if __name__ == "__main__":
    main()
