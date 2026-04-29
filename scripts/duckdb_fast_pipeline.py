"""DuckDB-based FAST CSV pipeline — replaces row-by-row Python with a single SQL pass."""

import argparse
import json
from pathlib import Path

import duckdb
import rasterio
from rasterio.warp import transform_bounds

FAST_INPUT_COLUMNS = [
    "FltyId",
    "Occ",
    "Cost",
    "Area",
    "NumStories",
    "FoundationType",
    "FirstFloorHt",
    "ContentCost",
    "Latitude",
    "Longitude",
]

# Canonical NSI found_type -> FAST FoundationType mapping (single source of truth).
# Both the DuckDB pipeline (SQL CASE) and the notebook (pandas .map) use this dict.
FOUND_TYPE_MAP: dict[str, int] = {
    "S": 7,
    "SLAB": 7,
    "SLAB ON GRADE": 7,
    "7": 7,
    "C": 5,
    "CRAWL": 5,
    "CRAWLSPACE": 5,
    "CRAWL SPACE": 5,
    "5": 5,
    "F": 5,
    "I": 5,
    "W": 5,
    "B": 4,
    "BASEMENT": 4,
    "4": 4,
    "P": 2,
    "PIER": 2,
    "PILE": 2,
    "PILES": 2,
    "PIER/PILE": 2,
    "2": 2,
}
FOUND_TYPE_DEFAULT = 7


def _found_type_sql_case() -> str:
    """Generate SQL CASE expression from FOUND_TYPE_MAP."""
    whens = "\n".join(
        f"                WHEN '{k}'{' ' * max(0, 14 - len(k))}THEN {v}"
        for k, v in FOUND_TYPE_MAP.items()
    )
    return (
        "CASE UPPER(TRIM(found_type))\n"
        f"{whens}\n"
        f"                ELSE {FOUND_TYPE_DEFAULT}\n"
        "            END"
    )


def _raster_bbox_wgs84(raster_path: str):
    """Return (min_lon, min_lat, max_lon, max_lat) in EPSG:4326."""
    with rasterio.open(raster_path) as src:
        if src.crs and src.crs.to_epsg() != 4326:
            bounds = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
        else:
            bounds = src.bounds
    return bounds  # (left, bottom, right, top)


def _duckdb_quote(value: str) -> str:
    """Return a single-quoted SQL literal safe for DuckDB statements."""
    return "'" + value.replace("'", "''") + "'"


def _duckdb_path_array_literal(parquet_paths: list[str | Path]) -> str:
    """Render a DuckDB string-array literal for read_parquet([...])."""
    return "[" + ", ".join(_duckdb_quote(str(Path(path))) for path in parquet_paths) + "]"


def _normalized_cbfips_sql(column_name: str = "cbfips") -> str:
    digits = f"REGEXP_REPLACE(COALESCE(CAST({column_name} AS VARCHAR), ''), '[^0-9]', '', 'g')"
    return f"CASE WHEN {digits} = '' THEN NULL ELSE LPAD({digits}, 15, '0') END"


def _create_fast_inventory_view(
    con: duckdb.DuckDBPyConnection,
    source_sql: str,
    raster_path: str,
) -> None:
    """Create a temp DuckDB view containing deduped FAST-ready building inventory."""
    min_lon, min_lat, max_lon, max_lat = _raster_bbox_wgs84(raster_path)

    sql = f"""
    CREATE OR REPLACE TEMP VIEW fast_inventory AS
    WITH raw AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY bid ORDER BY val_struct DESC
            ) AS _rn
        FROM {source_sql}
        WHERE latitude  BETWEEN {min_lat} AND {max_lat}
          AND longitude BETWEEN {min_lon} AND {max_lon}
          AND bid        IS NOT NULL
          AND occtype    IS NOT NULL
          AND val_struct IS NOT NULL
          AND sqft       IS NOT NULL
          AND num_story  IS NOT NULL
          AND found_type IS NOT NULL
          AND found_ht   IS NOT NULL
          AND latitude   IS NOT NULL
          AND longitude  IS NOT NULL
    )
    SELECT
        bid                                          AS FltyId,
        UPPER(SPLIT_PART(occtype, '-', 1))           AS Occ,
        val_struct                                   AS Cost,
        sqft                                         AS Area,
        num_story                                    AS NumStories,
        {_found_type_sql_case()}                      AS FoundationType,
        found_ht                                     AS FirstFloorHt,
        COALESCE(val_cont, 0)                        AS ContentCost,
        latitude                                     AS Latitude,
        longitude                                    AS Longitude,
        {_normalized_cbfips_sql()}                   AS cbfips
    FROM raw
    WHERE _rn = 1
    """
    con.execute(sql)


def build_fast_outputs_duckdb(
    parquet_paths: list[str | Path],
    raster_path: str | Path,
    fast_csv_path: str | Path,
    join_csv_path: str | Path,
    summary_json_path: str | Path,
    flc: str = "CoastalA",
) -> dict[str, int]:
    """Build FAST input CSV, cbfips join CSV, and a small summary JSON from parquet paths."""
    _ = flc
    normalized_paths = [Path(path) for path in parquet_paths]
    if not normalized_paths:
        raise ValueError("At least one parquet path must be provided.")

    fast_csv_path = Path(fast_csv_path)
    join_csv_path = Path(join_csv_path)
    summary_json_path = Path(summary_json_path)
    for path in (fast_csv_path, join_csv_path, summary_json_path):
        path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    source_sql = f"read_parquet({_duckdb_path_array_literal(normalized_paths)}, union_by_name=true)"
    _create_fast_inventory_view(con, source_sql, str(raster_path))

    con.execute(
        f"""
        COPY (
            SELECT {", ".join(FAST_INPUT_COLUMNS)}
            FROM fast_inventory
            ORDER BY FltyId
        ) TO {_duckdb_quote(str(fast_csv_path))} (HEADER, DELIMITER ',');
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT FltyId AS fltyid, cbfips
            FROM fast_inventory
            ORDER BY FltyId
        ) TO {_duckdb_quote(str(join_csv_path))} (HEADER, DELIMITER ',');
        """
    )

    row_count, residential_count = con.execute(
        """
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN Occ LIKE 'RES%' THEN 1 ELSE 0 END) AS residential_count
        FROM fast_inventory
        """
    ).fetchone()
    con.close()

    summary = {
        "row_count": int(row_count),
        "residential_count": int(residential_count or 0),
        "source_state_count": len(list(dict.fromkeys(normalized_paths))),
    }
    summary_json_path.write_text(json.dumps(summary), encoding="utf-8")
    return summary


def build_fast_csv_duckdb(
    parquet_glob: str,
    raster_path: str,
    output_csv: str,
    flc: str = "CoastalA",
    occupancy_csv: str | None = None,
) -> int:
    """Build FAST CSV from NSI parquet files using DuckDB.

    `flc` and `occupancy_csv` are accepted for backward compatibility with older
    callers, but the DuckDB extraction step does not use them.
    """
    _ = flc, occupancy_csv
    con = duckdb.connect()
    source_sql = f"read_parquet({_duckdb_quote(parquet_glob)})"
    _create_fast_inventory_view(con, source_sql, raster_path)
    con.execute(
        f"""
        COPY (
            SELECT {", ".join(FAST_INPUT_COLUMNS)}
            FROM fast_inventory
            ORDER BY FltyId
        ) TO {_duckdb_quote(str(output_csv))} (HEADER, DELIMITER ',');
        """
    )
    count = con.execute("SELECT COUNT(*) FROM read_csv_auto(?)", [output_csv]).fetchone()[0]
    con.close()
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DuckDB FAST CSV pipeline")
    parser.add_argument("--parquet-glob", required=True, help="Glob pattern for parquet files")
    parser.add_argument("--raster", required=True, help="Path to depth raster (GeoTIFF)")
    parser.add_argument("--output", required=True, help="Output CSV path")
    parser.add_argument(
        "--flc",
        default="CoastalA",
        help=(
            "Deprecated compatibility flag; ignored here because the flood-loss "
            "category is supplied when running FAST, not when generating the CSV."
        ),
    )
    args = parser.parse_args()

    n = build_fast_csv_duckdb(args.parquet_glob, args.raster, args.output, flc=args.flc)
    print(f"Wrote {n:,} rows to {args.output}")
