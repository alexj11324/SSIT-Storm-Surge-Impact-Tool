"""DuckDB-based FAST CSV pipeline — replaces row-by-row Python with a single SQL pass."""

import argparse

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
        f"                WHEN '{k}'{' ' * max(0, 14 - len(k))}THEN {v}" for k, v in FOUND_TYPE_MAP.items()
    )
    return f"CASE UPPER(TRIM(found_type))\n{whens}\n                ELSE {FOUND_TYPE_DEFAULT}\n            END"


def _raster_bbox_wgs84(raster_path: str):
    """Return (min_lon, min_lat, max_lon, max_lat) in EPSG:4326."""
    with rasterio.open(raster_path) as src:
        if src.crs and src.crs.to_epsg() != 4326:
            bounds = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
        else:
            bounds = src.bounds
    return bounds  # (left, bottom, right, top)


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
    min_lon, min_lat, max_lon, max_lat = _raster_bbox_wgs84(raster_path)

    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")

    sql = f"""
    COPY (
        WITH raw AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY bid ORDER BY val_struct DESC
                ) AS _rn
            FROM read_parquet('{parquet_glob}')
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
            longitude                                    AS Longitude
        FROM raw
        WHERE _rn = 1
    ) TO '{output_csv}' (HEADER, DELIMITER ',');
    """

    con.execute(sql)
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
