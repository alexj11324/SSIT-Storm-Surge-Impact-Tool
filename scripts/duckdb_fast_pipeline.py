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
    """Build FAST CSV from NSI parquet files using DuckDB. Returns row count."""
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
            CASE UPPER(TRIM(found_type))
                WHEN 'S'              THEN 7
                WHEN 'SLAB'           THEN 7
                WHEN 'SLAB ON GRADE'  THEN 7
                WHEN '7'              THEN 7
                WHEN 'C'              THEN 5
                WHEN 'CRAWL'          THEN 5
                WHEN 'CRAWL SPACE'    THEN 5
                WHEN '5'              THEN 5
                WHEN 'F'              THEN 5
                WHEN 'I'              THEN 5
                WHEN 'W'              THEN 5
                WHEN 'B'              THEN 4
                WHEN 'BASEMENT'       THEN 4
                WHEN '4'              THEN 4
                WHEN 'P'              THEN 2
                WHEN 'PIER'           THEN 2
                WHEN '2'              THEN 2
                ELSE 7
            END                                          AS FoundationType,
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
    parser.add_argument("--flc", default="CoastalA", help="Flood loss category")
    args = parser.parse_args()

    n = build_fast_csv_duckdb(args.parquet_glob, args.raster, args.output, args.flc)
    print(f"Wrote {n:,} rows to {args.output}")
