#!/usr/bin/env python3
"""Cloud-native county matching between Ground Truth Excel and Athena predictions.

This script keeps cloud-source data processing inside cloud services (Athena + S3).
Local execution is only used for:
1) reading Ground Truth Excel,
2) uploading a normalized Ground Truth Parquet file to S3,
3) submitting Athena SQL jobs,
4) reading small final summaries for reporting.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import pandas as pd


DEFAULT_EVENTS = [
    "BERYL_2024",
    "DEBBY_2024",
    "FLORENCE_2018",
    "HELENE_2024",
    "IAN_2022",
    "IDALIA_2023",
    "IDA_2021",
    "MICHAEL_2018",
    "MILTON_2024",
]

EVENT_KEY_MAP = {
    "BERYL": "BERYL_2024",
    "DEBBY": "DEBBY_2024",
    "FLORENCE": "FLORENCE_2018",
    "HELENE": "HELENE_2024",
    "IAN": "IAN_2022",
    "IDALIA": "IDALIA_2023",
    "IDA": "IDA_2021",
    "MICHAEL": "MICHAEL_2018",
    "MILTON": "MILTON_2024",
}

STATE_NAME_TO_ABBR = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District Of Columbia": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
}


def log(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_s3_prefix(uri: str) -> str:
    if not uri.startswith("s3://"):
        raise ValueError(f"S3 URI must start with s3://, got: {uri}")
    return uri if uri.endswith("/") else f"{uri}/"


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"S3 URI must start with s3://, got: {uri}")
    raw = uri[len("s3://") :]
    bucket, sep, key = raw.partition("/")
    if not bucket or not sep or not key:
        raise ValueError(f"S3 URI must include bucket and key, got: {uri}")
    return bucket, key


def split_table_ref(ref: str, default_db: str) -> tuple[str, str]:
    if "." in ref:
        db, table = ref.split(".", 1)
        return db.strip(), table.strip()
    return default_db, ref.strip()


def quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def quote_sql_list(values: list[str]) -> str:
    return ", ".join(quote_sql(v) for v in values)


def sanitize_identifier(name: str) -> str:
    out = re.sub(r"[^A-Za-z0-9_]", "_", name)
    out = re.sub(r"_+", "_", out).strip("_").lower()
    if not out:
        raise ValueError(f"Invalid identifier: {name}")
    return out


def normalize_fips(value: Any, width: int) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    digits = re.sub(r"\D", "", text)
    if not digits:
        return None
    return digits.zfill(width)[-width:]


def normalize_county_name(name: Any) -> str:
    text = "" if pd.isna(name) else str(name)
    text = text.strip().upper()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s+(COUNTY|PARISH|BOROUGH|CITY)$", "", text)
    return text.strip()


def normalize_event(event: Any) -> str | None:
    if pd.isna(event):
        return None
    text = str(event).strip().upper()
    if not text:
        return None
    mapped = EVENT_KEY_MAP.get(text)
    if mapped:
        return mapped
    return text


def normalize_state_abbr(state: Any) -> str | None:
    if pd.isna(state):
        return None
    text = str(state).strip()
    if not text:
        return None
    if len(text) == 2 and text.isalpha():
        return text.upper()

    text = text.replace("_", " ")
    text = re.sub(r"\s+", " ", text).strip().title()
    return STATE_NAME_TO_ABBR.get(text)


def build_state_abbr_case_sql(col_expr: str) -> str:
    clauses = []
    for name, abbr in sorted(STATE_NAME_TO_ABBR.items()):
        key = name.upper()
        clauses.append(
            f"WHEN UPPER(REGEXP_REPLACE(REPLACE({col_expr}, '_', ' '), '\\s+', ' ')) = {quote_sql(key)} THEN {quote_sql(abbr)}"
        )
    clauses.append(f"WHEN LENGTH(TRIM({col_expr})) = 2 THEN UPPER(TRIM({col_expr}))")
    clauses.append("ELSE NULL")
    return "CASE\n    " + "\n    ".join(clauses) + "\nEND"


def build_county_norm_sql(col_expr: str) -> str:
    return (
        "TRIM(REGEXP_REPLACE("
        f"UPPER(REGEXP_REPLACE(COALESCE({col_expr}, ''), '\\s+', ' ')), "
        "'\\s+(COUNTY|PARISH|BOROUGH|CITY)$', ''"
        "))"
    )


@dataclass
class AthenaExecution:
    step: str
    query_execution_id: str
    state: str
    submitted_at: str


class AthenaRunner:
    def __init__(
        self,
        region: str,
        workgroup: str,
        output_location: str,
    ) -> None:
        self.client = boto3.client("athena", region_name=region)
        self.workgroup = workgroup
        self.output_location = ensure_s3_prefix(output_location)
        self.executions: list[AthenaExecution] = []

    def run_query(self, *, query: str, database: str, step: str, poll_seconds: int = 2) -> str:
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": self.output_location},
            WorkGroup=self.workgroup,
        )
        qid = response["QueryExecutionId"]
        log(f"[{step}] submitted Athena query: {qid}")

        state = "QUEUED"
        reason = ""
        while True:
            status = self.client.get_query_execution(QueryExecutionId=qid)
            query_status = status["QueryExecution"]["Status"]
            state = query_status["State"]
            if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                reason = query_status.get("StateChangeReason", "")
                break
            time.sleep(poll_seconds)

        self.executions.append(
            AthenaExecution(step=step, query_execution_id=qid, state=state, submitted_at=now_utc_iso())
        )

        if state != "SUCCEEDED":
            raise RuntimeError(f"Athena step failed [{step}] ({qid}): {state} {reason}")
        return qid

    def fetch_rows(self, *, query: str, database: str, step: str) -> tuple[list[str], list[dict[str, str]]]:
        qid = self.run_query(query=query, database=database, step=step)
        rows: list[dict[str, str]] = []
        headers: list[str] = []

        next_token: str | None = None
        first_page = True
        while True:
            kwargs = {"QueryExecutionId": qid, "MaxResults": 1000}
            if next_token:
                kwargs["NextToken"] = next_token
            response = self.client.get_query_results(**kwargs)
            data_rows = response["ResultSet"]["Rows"]

            start_idx = 0
            if first_page and data_rows:
                headers = [cell.get("VarCharValue", "") for cell in data_rows[0]["Data"]]
                start_idx = 1
                first_page = False

            for row in data_rows[start_idx:]:
                values = [cell.get("VarCharValue", "") for cell in row["Data"]]
                padded = values + [""] * (len(headers) - len(values))
                rows.append(dict(zip(headers, padded)))

            next_token = response.get("NextToken")
            if not next_token:
                break

        return headers, rows


def prepare_ground_truth_dataframe(excel_path: Path, events: list[str]) -> pd.DataFrame:
    raw = pd.read_excel(excel_path)
    required = {
        "ID",
        "Event",
        "Landfall Date",
        "County",
        "State",
        "County FIPS",
        "State FIPS",
        "Planned Shelter Population",
        "Actual Shelter Population",
        "Estimated Population Impacted",
    }
    missing = sorted(required - set(raw.columns))
    if missing:
        raise ValueError(f"Ground Truth missing required columns: {', '.join(missing)}")

    df = pd.DataFrame()
    df["gt_id"] = raw["ID"].astype(str).str.strip()
    df["event"] = raw["Event"].astype(str).str.strip()
    df["event_key"] = raw["Event"].apply(normalize_event)
    df["landfall_date"] = raw["Landfall Date"].astype(str).str.strip()
    df["county"] = raw["County"].astype(str).str.strip()
    df["state"] = raw["State"].astype(str).str.strip()
    df["county_fips"] = raw["County FIPS"].apply(lambda x: normalize_fips(x, 3))
    df["state_fips"] = raw["State FIPS"].apply(lambda x: normalize_fips(x, 2))
    df["state_abbr"] = raw["State"].apply(normalize_state_abbr)

    def merge_fips(row: pd.Series) -> str | None:
        if not row["state_fips"] or not row["county_fips"]:
            return None
        return f"{row['state_fips']}{row['county_fips']}"

    df["county_fips5"] = df.apply(merge_fips, axis=1)
    df["county_name_norm"] = raw["County"].apply(normalize_county_name)
    df["planned_shelter_population"] = pd.to_numeric(raw["Planned Shelter Population"], errors="coerce")
    df["actual_shelter_population"] = pd.to_numeric(raw["Actual Shelter Population"], errors="coerce")
    df["estimated_population_impacted"] = pd.to_numeric(raw["Estimated Population Impacted"], errors="coerce")

    df = df[df["event_key"].isin(events)].copy()
    df = df[df["state_abbr"].notna()].copy()
    df = df[df["county_fips5"].notna()].copy()

    return df


def upload_file_to_s3(local_path: Path, s3_uri: str, region: str) -> None:
    bucket, key = parse_s3_uri(s3_uri)
    client = boto3.client("s3", region_name=region)
    client.upload_file(str(local_path), bucket, key)


def upload_text_to_s3(body: str, s3_uri: str, region: str, content_type: str) -> None:
    bucket, key = parse_s3_uri(s3_uri)
    client = boto3.client("s3", region_name=region)
    client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"), ContentType=content_type)


def ensure_database(runner: AthenaRunner, database: str, location: str) -> None:
    query = f"CREATE DATABASE IF NOT EXISTS {database} LOCATION {quote_sql(ensure_s3_prefix(location))}"
    runner.run_query(query=query, database="default", step="create_database")


def get_table_columns(runner: AthenaRunner, *, table_db: str, table_name: str, query_db: str) -> set[str]:
    sql = (
        "SELECT LOWER(column_name) AS column_name "
        "FROM information_schema.columns "
        f"WHERE table_schema = {quote_sql(table_db)} "
        f"AND table_name = {quote_sql(table_name)}"
    )
    _, rows = runner.fetch_rows(query=sql, database=query_db, step=f"describe_table_{table_db}_{table_name}")
    return {row["column_name"] for row in rows if row.get("column_name")}


def ensure_county_table(
    runner: AthenaRunner,
    *,
    default_database: str,
    county_table_ref: str,
    county_boundary_s3_prefix: str | None,
    county_fips_col: str,
    county_name_col: str,
    county_state_abbr_col: str,
    county_geometry_col: str,
) -> tuple[str, str]:
    county_db, county_table = split_table_ref(county_table_ref, default_database)
    existing_columns = get_table_columns(
        runner,
        table_db=county_db,
        table_name=county_table,
        query_db=default_database,
    )

    if not existing_columns:
        if not county_boundary_s3_prefix:
            raise RuntimeError(
                "County boundary table is missing and --county-boundary-s3-prefix was not provided. "
                "Create a cloud table first (Glue/EMR/Lambda/Athena external table), then rerun."
            )

        location = ensure_s3_prefix(county_boundary_s3_prefix)
        ddl = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {county_db}.{county_table} (
  {county_fips_col} STRING,
  {county_name_col} STRING,
  {county_state_abbr_col} STRING,
  {county_geometry_col} STRING
)
STORED AS PARQUET
LOCATION {quote_sql(location)}
"""
        runner.run_query(query=ddl, database=county_db, step="create_county_boundary_table")

        existing_columns = get_table_columns(
            runner,
            table_db=county_db,
            table_name=county_table,
            query_db=default_database,
        )

    required = {
        county_fips_col.lower(),
        county_name_col.lower(),
        county_state_abbr_col.lower(),
        county_geometry_col.lower(),
    }
    missing = sorted(required - existing_columns)
    if missing:
        raise RuntimeError(
            "County boundary table is missing required columns: "
            + ", ".join(missing)
            + f". Table checked: {county_db}.{county_table}"
        )

    return county_db, county_table


def build_tables(
    runner: AthenaRunner,
    *,
    database: str,
    output_prefix: str,
    run_suffix: str,
    events: list[str],
    gt_table: str,
    county_db: str,
    county_table: str,
    county_fips_col: str,
    county_name_col: str,
    county_state_abbr_col: str,
    county_geometry_col: str,
    county_geometry_format: str,
) -> dict[str, str]:
    out = ensure_s3_prefix(output_prefix)
    events_sql = quote_sql_list(events)
    state_case = build_state_abbr_case_sql("p.state")

    table_names = {
        "pred_points_union": f"pred_points_union_{run_suffix}",
        "pred_event_counties": f"pred_event_counties_{run_suffix}",
        "gt_county_keys": f"gt_county_keys_{run_suffix}",
        "matched_counties": f"matched_counties_{run_suffix}",
        "gt_unmatched_counties": f"gt_unmatched_counties_{run_suffix}",
        "prediction_extra_counties": f"prediction_extra_counties_{run_suffix}",
        "name_mismatch_on_same_fips": f"name_mismatch_on_same_fips_{run_suffix}",
        "summary_by_event": f"summary_by_event_{run_suffix}",
        "summary_overall": f"summary_overall_{run_suffix}",
    }

    pred_points_sql = f"""
CREATE TABLE {database}.{table_names['pred_points_union']}
WITH (
  format = 'PARQUET',
  external_location = {quote_sql(out + 'pred_points_union/')}
) AS
WITH gt_target AS (
  SELECT DISTINCT
    event_key AS event,
    UPPER(state_abbr) AS state_abbr
  FROM {database}.{gt_table}
  WHERE event_key IN ({events_sql})
    AND state_abbr IS NOT NULL
),
base AS (
  SELECT
    p.event,
    CAST(ROUND(CAST(p.latitude AS DOUBLE), 6) AS DOUBLE) AS latitude,
    CAST(ROUND(CAST(p.longitude AS DOUBLE), 6) AS DOUBLE) AS longitude,
    {state_case} AS state_abbr
  FROM arc_storm_surge.predictions p
  WHERE p.event IN ({events_sql})
    AND p.latitude IS NOT NULL
    AND p.longitude IS NOT NULL
)
SELECT DISTINCT
  b.event,
  b.state_abbr,
  b.latitude,
  b.longitude
FROM base b
JOIN gt_target t
  ON b.event = t.event
 AND b.state_abbr = t.state_abbr
WHERE b.state_abbr IS NOT NULL
"""
    runner.run_query(query=pred_points_sql, database=database, step="create_pred_points_union")

    if county_geometry_format == "wkt":
        geom_expr = f"ST_GeometryFromText(c.{county_geometry_col})"
    else:
        geom_expr = f"c.{county_geometry_col}"

    pred_counties_sql = f"""
CREATE TABLE {database}.{table_names['pred_event_counties']}
WITH (
  format = 'PARQUET',
  external_location = {quote_sql(out + 'pred_event_counties/')}
) AS
SELECT DISTINCT
  p.event,
  p.state_abbr,
  LPAD(REGEXP_REPLACE(CAST(c.{county_fips_col} AS VARCHAR), '[^0-9]', ''), 5, '0') AS county_fips5,
  CAST(c.{county_name_col} AS VARCHAR) AS county_name
FROM {database}.{table_names['pred_points_union']} p
JOIN {county_db}.{county_table} c
  ON UPPER(CAST(c.{county_state_abbr_col} AS VARCHAR)) = p.state_abbr
 AND ST_Intersects({geom_expr}, ST_Point(p.longitude, p.latitude))
WHERE c.{county_fips_col} IS NOT NULL
"""
    runner.run_query(query=pred_counties_sql, database=database, step="create_pred_event_counties")

    gt_keys_sql = f"""
CREATE TABLE {database}.{table_names['gt_county_keys']}
WITH (
  format = 'PARQUET',
  external_location = {quote_sql(out + 'gt_county_keys/')}
) AS
SELECT DISTINCT
  event_key AS event,
  UPPER(state_abbr) AS state_abbr,
  LPAD(REGEXP_REPLACE(county_fips5, '[^0-9]', ''), 5, '0') AS county_fips5,
  county AS county_name_gt,
  county_name_norm AS county_name_gt_norm
FROM {database}.{gt_table}
WHERE event_key IN ({events_sql})
  AND county_fips5 IS NOT NULL
  AND state_abbr IS NOT NULL
"""
    runner.run_query(query=gt_keys_sql, database=database, step="create_gt_county_keys")

    matched_sql = f"""
CREATE TABLE {database}.{table_names['matched_counties']}
WITH (
  format = 'PARQUET',
  external_location = {quote_sql(out + 'matched_counties/')}
) AS
SELECT
  g.event,
  g.state_abbr,
  g.county_fips5,
  g.county_name_gt,
  p.county_name AS county_name_pred
FROM {database}.{table_names['gt_county_keys']} g
JOIN {database}.{table_names['pred_event_counties']} p
  ON g.event = p.event
 AND g.state_abbr = p.state_abbr
 AND g.county_fips5 = p.county_fips5
"""
    runner.run_query(query=matched_sql, database=database, step="create_matched_counties")

    gt_unmatched_sql = f"""
CREATE TABLE {database}.{table_names['gt_unmatched_counties']}
WITH (
  format = 'PARQUET',
  external_location = {quote_sql(out + 'gt_unmatched_counties/')}
) AS
SELECT
  g.event,
  g.state_abbr,
  g.county_fips5,
  g.county_name_gt
FROM {database}.{table_names['gt_county_keys']} g
LEFT JOIN {database}.{table_names['pred_event_counties']} p
  ON g.event = p.event
 AND g.state_abbr = p.state_abbr
 AND g.county_fips5 = p.county_fips5
WHERE p.county_fips5 IS NULL
"""
    runner.run_query(query=gt_unmatched_sql, database=database, step="create_gt_unmatched_counties")

    pred_extra_sql = f"""
CREATE TABLE {database}.{table_names['prediction_extra_counties']}
WITH (
  format = 'PARQUET',
  external_location = {quote_sql(out + 'prediction_extra_counties/')}
) AS
SELECT
  p.event,
  p.state_abbr,
  p.county_fips5,
  p.county_name
FROM {database}.{table_names['pred_event_counties']} p
LEFT JOIN {database}.{table_names['gt_county_keys']} g
  ON g.event = p.event
 AND g.state_abbr = p.state_abbr
 AND g.county_fips5 = p.county_fips5
WHERE g.county_fips5 IS NULL
"""
    runner.run_query(query=pred_extra_sql, database=database, step="create_prediction_extra_counties")

    gt_norm_sql = build_county_norm_sql("m.county_name_gt")
    pred_norm_sql = build_county_norm_sql("m.county_name_pred")
    name_mismatch_sql = f"""
CREATE TABLE {database}.{table_names['name_mismatch_on_same_fips']}
WITH (
  format = 'PARQUET',
  external_location = {quote_sql(out + 'name_mismatch_on_same_fips/')}
) AS
SELECT
  m.event,
  m.state_abbr,
  m.county_fips5,
  m.county_name_gt,
  m.county_name_pred,
  {gt_norm_sql} AS county_name_gt_norm,
  {pred_norm_sql} AS county_name_pred_norm
FROM {database}.{table_names['matched_counties']} m
WHERE {gt_norm_sql} <> {pred_norm_sql}
"""
    runner.run_query(query=name_mismatch_sql, database=database, step="create_name_mismatch_on_same_fips")

    summary_by_event_sql = f"""
CREATE TABLE {database}.{table_names['summary_by_event']}
WITH (
  format = 'PARQUET',
  external_location = {quote_sql(out + 'summary_by_event/')}
) AS
WITH
  events AS (
    SELECT event FROM {database}.{table_names['gt_county_keys']}
    UNION
    SELECT event FROM {database}.{table_names['pred_event_counties']}
  ),
  gt AS (
    SELECT event, COUNT(*) AS gt_total_count
    FROM {database}.{table_names['gt_county_keys']}
    GROUP BY event
  ),
  pred AS (
    SELECT event, COUNT(*) AS prediction_total_count
    FROM {database}.{table_names['pred_event_counties']}
    GROUP BY event
  ),
  matched AS (
    SELECT event, COUNT(*) AS matched_count
    FROM {database}.{table_names['matched_counties']}
    GROUP BY event
  ),
  unmatched AS (
    SELECT event, COUNT(*) AS gt_unmatched_count
    FROM {database}.{table_names['gt_unmatched_counties']}
    GROUP BY event
  ),
  extra AS (
    SELECT event, COUNT(*) AS prediction_extra_count
    FROM {database}.{table_names['prediction_extra_counties']}
    GROUP BY event
  ),
  name_mismatch AS (
    SELECT event, COUNT(*) AS name_mismatch_count
    FROM {database}.{table_names['name_mismatch_on_same_fips']}
    GROUP BY event
  )
SELECT
  e.event,
  COALESCE(gt.gt_total_count, 0) AS gt_total_count,
  COALESCE(pred.prediction_total_count, 0) AS prediction_total_count,
  COALESCE(matched.matched_count, 0) AS matched_count,
  COALESCE(unmatched.gt_unmatched_count, 0) AS gt_unmatched_count,
  COALESCE(extra.prediction_extra_count, 0) AS prediction_extra_count,
  COALESCE(name_mismatch.name_mismatch_count, 0) AS name_mismatch_count,
  CASE
    WHEN COALESCE(gt.gt_total_count, 0) = 0 THEN NULL
    ELSE CAST(COALESCE(matched.matched_count, 0) AS DOUBLE) / CAST(gt.gt_total_count AS DOUBLE)
  END AS coverage_rate
FROM events e
LEFT JOIN gt ON gt.event = e.event
LEFT JOIN pred ON pred.event = e.event
LEFT JOIN matched ON matched.event = e.event
LEFT JOIN unmatched ON unmatched.event = e.event
LEFT JOIN extra ON extra.event = e.event
LEFT JOIN name_mismatch ON name_mismatch.event = e.event
ORDER BY e.event
"""
    runner.run_query(query=summary_by_event_sql, database=database, step="create_summary_by_event")

    summary_overall_sql = f"""
CREATE TABLE {database}.{table_names['summary_overall']}
WITH (
  format = 'PARQUET',
  external_location = {quote_sql(out + 'summary_overall/')}
) AS
SELECT
  CAST(SUM(gt_total_count) AS BIGINT) AS gt_total_count,
  CAST(SUM(prediction_total_count) AS BIGINT) AS prediction_total_count,
  CAST(SUM(matched_count) AS BIGINT) AS matched_count,
  CAST(SUM(gt_unmatched_count) AS BIGINT) AS gt_unmatched_count,
  CAST(SUM(prediction_extra_count) AS BIGINT) AS prediction_extra_count,
  CAST(SUM(name_mismatch_count) AS BIGINT) AS name_mismatch_count,
  CASE
    WHEN SUM(gt_total_count) = 0 THEN NULL
    ELSE CAST(SUM(matched_count) AS DOUBLE) / CAST(SUM(gt_total_count) AS DOUBLE)
  END AS coverage_rate,
  (SUM(matched_count) + SUM(gt_unmatched_count) = SUM(gt_total_count)) AS match_balance_check
FROM {database}.{table_names['summary_by_event']}
"""
    runner.run_query(query=summary_overall_sql, database=database, step="create_summary_overall")

    try:
        view_by_event_sql = (
            f"CREATE OR REPLACE VIEW {database}.county_match_latest_summary_by_event "
            f"AS SELECT * FROM {database}.{table_names['summary_by_event']}"
        )
        runner.run_query(query=view_by_event_sql, database=database, step="create_latest_view_by_event")

        view_overall_sql = (
            f"CREATE OR REPLACE VIEW {database}.county_match_latest_summary_overall "
            f"AS SELECT * FROM {database}.{table_names['summary_overall']}"
        )
        runner.run_query(query=view_overall_sql, database=database, step="create_latest_view_overall")
    except RuntimeError as exc:
        log(f"View update skipped: {exc}")

    return table_names


def create_gt_external_table(
    runner: AthenaRunner,
    *,
    database: str,
    gt_table: str,
    gt_location: str,
) -> None:
    ddl = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{gt_table} (
  gt_id STRING,
  event STRING,
  event_key STRING,
  landfall_date STRING,
  county STRING,
  state STRING,
  county_fips STRING,
  state_fips STRING,
  county_fips5 STRING,
  state_abbr STRING,
  county_name_norm STRING,
  planned_shelter_population DOUBLE,
  actual_shelter_population DOUBLE,
  estimated_population_impacted DOUBLE
)
STORED AS PARQUET
LOCATION {quote_sql(ensure_s3_prefix(gt_location))}
"""
    runner.run_query(query=ddl, database=database, step="create_gt_external_table")


def rows_to_markdown(by_event_rows: list[dict[str, str]], overall_row: dict[str, str]) -> str:
    lines: list[str] = []
    lines.append("# County Match Report")
    lines.append("")
    lines.append(f"Generated at: `{now_utc_iso()}`")
    lines.append("")
    lines.append("## Overall")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---:|")
    for key in [
        "gt_total_count",
        "prediction_total_count",
        "matched_count",
        "gt_unmatched_count",
        "prediction_extra_count",
        "name_mismatch_count",
        "coverage_rate",
        "match_balance_check",
    ]:
        lines.append(f"| {key} | {overall_row.get(key, '')} |")

    lines.append("")
    lines.append("## By Event")
    lines.append("")
    lines.append(
        "| event | gt_total_count | prediction_total_count | matched_count | gt_unmatched_count | prediction_extra_count | name_mismatch_count | coverage_rate |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for row in by_event_rows:
        lines.append(
            "| {event} | {gt_total_count} | {prediction_total_count} | {matched_count} | "
            "{gt_unmatched_count} | {prediction_extra_count} | {name_mismatch_count} | {coverage_rate} |".format(
                event=row.get("event", ""),
                gt_total_count=row.get("gt_total_count", ""),
                prediction_total_count=row.get("prediction_total_count", ""),
                matched_count=row.get("matched_count", ""),
                gt_unmatched_count=row.get("gt_unmatched_count", ""),
                prediction_extra_count=row.get("prediction_extra_count", ""),
                name_mismatch_count=row.get("name_mismatch_count", ""),
                coverage_rate=row.get("coverage_rate", ""),
            )
        )

    return "\n".join(lines) + "\n"


def parse_events(event_text: str) -> list[str]:
    parsed = [item.strip().upper() for item in event_text.split(",") if item.strip()]
    if not parsed:
        raise ValueError("--events must contain at least one event")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cloud-native county match orchestration via Athena/S3.",
    )
    parser.add_argument(
        "--ground-truth-xlsx",
        default="/Users/alexjiang/Downloads/Ground Truth Data.xlsx",
        help="Local Ground Truth Excel path.",
    )
    parser.add_argument(
        "--s3-output-prefix",
        default="s3://red-cross-capstone-project-data/analysis/county-match/",
        help="S3 output prefix for this run.",
    )
    parser.add_argument(
        "--athena-database",
        default="arc_analysis",
        help="Athena database for run artifacts.",
    )
    parser.add_argument(
        "--athena-workgroup",
        default="primary",
        help="Athena workgroup name.",
    )
    parser.add_argument(
        "--events",
        default=",".join(DEFAULT_EVENTS),
        help="Comma-separated event keys.",
    )
    parser.add_argument(
        "--advisory-scope",
        default="all",
        choices=["all"],
        help="Advisory scope. Fixed to all adv union.",
    )
    parser.add_argument(
        "--aws-region",
        default="us-east-1",
        help="AWS region for Athena and S3.",
    )
    parser.add_argument(
        "--county-boundary-table",
        default="arc_analysis.us_county_boundaries",
        help="Athena county boundary table in [db.]table format.",
    )
    parser.add_argument(
        "--county-boundary-s3-prefix",
        default=None,
        help="Optional S3 prefix to create county boundary table if missing.",
    )
    parser.add_argument(
        "--county-fips-column",
        default="county_fips5",
        help="County boundary table FIPS column.",
    )
    parser.add_argument(
        "--county-name-column",
        default="county_name",
        help="County boundary table county name column.",
    )
    parser.add_argument(
        "--county-state-abbr-column",
        default="state_abbr",
        help="County boundary table state abbreviation column.",
    )
    parser.add_argument(
        "--county-geometry-column",
        default="geometry",
        help="County boundary table geometry column.",
    )
    parser.add_argument(
        "--county-geometry-format",
        default="wkt",
        choices=["wkt", "geometry"],
        help="County geometry storage format.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    events = parse_events(args.events)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_suffix = sanitize_identifier(f"cm_{run_id}")
    output_prefix = ensure_s3_prefix(args.s3_output_prefix) + f"{run_id}/"
    athena_query_output = output_prefix + "athena_query_results/"

    log(f"Run ID: {run_id}")
    log(f"Athena database: {args.athena_database}")
    log(f"S3 output prefix: {output_prefix}")

    local_output_dir = Path("exports") / f"county_match_{run_id}"
    local_output_dir.mkdir(parents=True, exist_ok=True)

    gt_df = prepare_ground_truth_dataframe(Path(args.ground_truth_xlsx), events)
    if gt_df.empty:
        raise RuntimeError("Ground Truth has no rows after normalization and event filtering.")

    local_gt_parquet = local_output_dir / "ground_truth.normalized.parquet"
    gt_df.to_parquet(local_gt_parquet, index=False)

    gt_s3_prefix = output_prefix + "ground_truth/"
    gt_s3_uri = gt_s3_prefix + "ground_truth.parquet"
    upload_file_to_s3(local_gt_parquet, gt_s3_uri, args.aws_region)
    log(f"Uploaded Ground Truth parquet: {gt_s3_uri}")

    runner = AthenaRunner(
        region=args.aws_region,
        workgroup=args.athena_workgroup,
        output_location=athena_query_output,
    )

    ensure_database(
        runner,
        database=args.athena_database,
        location=ensure_s3_prefix(args.s3_output_prefix) + "athena-db/",
    )

    county_db, county_table = ensure_county_table(
        runner,
        default_database=args.athena_database,
        county_table_ref=args.county_boundary_table,
        county_boundary_s3_prefix=args.county_boundary_s3_prefix,
        county_fips_col=args.county_fips_column,
        county_name_col=args.county_name_column,
        county_state_abbr_col=args.county_state_abbr_column,
        county_geometry_col=args.county_geometry_column,
    )

    gt_table_name = f"gt_input_{run_suffix}"
    create_gt_external_table(
        runner,
        database=args.athena_database,
        gt_table=gt_table_name,
        gt_location=gt_s3_prefix,
    )

    table_names = build_tables(
        runner,
        database=args.athena_database,
        output_prefix=output_prefix,
        run_suffix=run_suffix,
        events=events,
        gt_table=gt_table_name,
        county_db=county_db,
        county_table=county_table,
        county_fips_col=args.county_fips_column,
        county_name_col=args.county_name_column,
        county_state_abbr_col=args.county_state_abbr_column,
        county_geometry_col=args.county_geometry_column,
        county_geometry_format=args.county_geometry_format,
    )

    _, by_event_rows = runner.fetch_rows(
        query=f"SELECT * FROM {args.athena_database}.{table_names['summary_by_event']} ORDER BY event",
        database=args.athena_database,
        step="fetch_summary_by_event",
    )
    _, overall_rows = runner.fetch_rows(
        query=f"SELECT * FROM {args.athena_database}.{table_names['summary_overall']}",
        database=args.athena_database,
        step="fetch_summary_overall",
    )
    overall_row = overall_rows[0] if overall_rows else {}

    summary_json = {
        "run_id": run_id,
        "generated_at": now_utc_iso(),
        "events": events,
        "advisory_scope": args.advisory_scope,
        "athena_database": args.athena_database,
        "athena_workgroup": args.athena_workgroup,
        "s3_output_prefix": output_prefix,
        "tables": {name: f"{args.athena_database}.{table}" for name, table in table_names.items()},
        "overall": overall_row,
        "by_event": by_event_rows,
    }

    report_md = rows_to_markdown(by_event_rows, overall_row)

    query_manifest = {
        "run_id": run_id,
        "generated_at": now_utc_iso(),
        "query_executions": [
            {
                "step": item.step,
                "query_execution_id": item.query_execution_id,
                "state": item.state,
                "submitted_at": item.submitted_at,
            }
            for item in runner.executions
        ],
    }

    local_summary_json = local_output_dir / "summary_overall.json"
    local_by_event_csv = local_output_dir / "summary_by_event.csv"
    local_report_md = local_output_dir / "README_match_report.md"
    local_manifest_json = local_output_dir / "query_manifest.json"

    local_summary_json.write_text(json.dumps(summary_json, indent=2, sort_keys=True), encoding="utf-8")
    local_report_md.write_text(report_md, encoding="utf-8")
    local_manifest_json.write_text(json.dumps(query_manifest, indent=2, sort_keys=True), encoding="utf-8")

    if by_event_rows:
        pd.DataFrame(by_event_rows).to_csv(local_by_event_csv, index=False)
    else:
        pd.DataFrame(columns=[]).to_csv(local_by_event_csv, index=False)

    upload_text_to_s3(
        json.dumps(summary_json, indent=2, sort_keys=True),
        output_prefix + "summary_overall.json",
        args.aws_region,
        "application/json",
    )
    upload_text_to_s3(
        pd.DataFrame(by_event_rows).to_csv(index=False),
        output_prefix + "summary_by_event.csv",
        args.aws_region,
        "text/csv",
    )
    upload_text_to_s3(
        report_md,
        output_prefix + "README_match_report.md",
        args.aws_region,
        "text/markdown",
    )
    upload_text_to_s3(
        json.dumps(query_manifest, indent=2, sort_keys=True),
        output_prefix + "query_manifest.json",
        args.aws_region,
        "application/json",
    )

    log("County match cloud workflow completed successfully.")
    log(f"Summary table: {args.athena_database}.{table_names['summary_overall']}")
    log(f"By-event table: {args.athena_database}.{table_names['summary_by_event']}")
    log(f"S3 run prefix: {output_prefix}")


if __name__ == "__main__":
    main()
