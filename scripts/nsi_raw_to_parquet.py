"""Convert raw NSI GPKG/GeoJSON to processed Parquet matching pipeline schema.

Primary engine: DuckDB spatial (streaming, low memory).
Fallback: geopandas + pyogrio.
"""

from __future__ import annotations

import argparse
import glob
import sys

import pyarrow as pa
import pyarrow.parquet as pq

# Target schema — must match existing processed parquet (e.g. Alabama)
TARGET_SCHEMA = pa.schema(
    [
        ("bid", pa.string()),
        ("bldgtype", pa.string()),
        ("cbfips", pa.string()),
        ("fd_id", pa.int64()),
        ("firmzone", pa.string()),
        ("found_ht", pa.float64()),
        ("found_type", pa.string()),
        ("ftprntid", pa.string()),
        ("ftprntsrc", pa.string()),
        ("ground_elv", pa.float64()),
        ("ground_elv_m", pa.float64()),
        ("med_yr_blt", pa.int64()),
        ("num_story", pa.int64()),
        ("o65disable", pa.float64()),
        ("occtype", pa.string()),
        ("pop2amo65", pa.int64()),
        ("pop2amu65", pa.int64()),
        ("pop2pmo65", pa.int64()),
        ("pop2pmu65", pa.int64()),
        ("source", pa.string()),
        ("sqft", pa.float64()),
        ("st_damcat", pa.string()),
        ("students", pa.int64()),
        ("u65disable", pa.float64()),
        ("val_cont", pa.float64()),
        ("val_struct", pa.float64()),
        ("val_vehic", pa.int64()),
        ("x", pa.float64()),
        ("y", pa.float64()),
        ("longitude", pa.float64()),
        ("latitude", pa.float64()),
        ("processed_at", pa.timestamp("ns")),
    ]
)

TARGET_COLUMNS = [f.name for f in TARGET_SCHEMA]


def _convert_duckdb(input_path: str, output_path: str) -> int:
    """DuckDB spatial: stream GPKG → Parquet in one SQL pass."""
    import duckdb

    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")

    # Detect CRS — reproject if not WGS84
    crs_sql = f"SELECT ST_SRID(geom) FROM st_read('{input_path}') LIMIT 1"
    try:
        srid = con.execute(crs_sql).fetchone()[0]
    except Exception:
        srid = 4326  # assume WGS84 if detection fails

    geom_x = (
        "ST_X(ST_Transform(geom, 'EPSG:4326', 'EPSG:4326'))"
        if srid == 4326
        else f"ST_X(ST_Transform(geom, 'EPSG:{srid}', 'EPSG:4326'))"
    )
    geom_y = (
        "ST_Y(ST_Transform(geom, 'EPSG:4326', 'EPSG:4326'))"
        if srid == 4326
        else f"ST_Y(ST_Transform(geom, 'EPSG:{srid}', 'EPSG:4326'))"
    )

    if srid == 4326:
        geom_x, geom_y = "ST_X(geom)", "ST_Y(geom)"

    # Build SELECT list: use raw columns if they exist, NULL otherwise
    col_info = con.execute(f"DESCRIBE SELECT * FROM st_read('{input_path}') LIMIT 0").fetchall()
    raw_cols = {row[0].lower() for row in col_info}

    select_parts = []
    for field in TARGET_SCHEMA:
        name = field.name
        if name in ("x", "longitude"):
            select_parts.append(f"{geom_x} AS {name}")
        elif name in ("y", "latitude"):
            select_parts.append(f"{geom_y} AS {name}")
        elif name == "processed_at":
            select_parts.append(f"current_timestamp AS {name}")
        elif name.lower() in raw_cols:
            select_parts.append(f'"{name}"')
        else:
            select_parts.append(f"NULL AS {name}")

    select_clause = ",\n            ".join(select_parts)

    sql = f"""
    COPY (
        SELECT
            {select_clause}
        FROM st_read('{input_path}')
    ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """

    con.execute(sql)
    count = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [output_path]).fetchone()[0]
    con.close()
    return count


def _convert_geopandas(input_path: str, output_path: str) -> int:
    """Fallback: geopandas + pyogrio."""
    import geopandas as gpd
    import pandas as pd

    gdf = gpd.read_file(input_path, engine="pyogrio")

    # Reproject if needed
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    gdf["x"] = gdf.geometry.x
    gdf["y"] = gdf.geometry.y
    gdf["longitude"] = gdf["x"]
    gdf["latitude"] = gdf["y"]
    gdf["processed_at"] = pd.Timestamp.now()

    df = gdf.drop(columns="geometry")

    # Add missing columns as NULL
    for field in TARGET_SCHEMA:
        if field.name not in df.columns:
            df[field.name] = None

    df = df[TARGET_COLUMNS]
    df.to_parquet(output_path, compression="snappy", index=False)
    return len(df)


def validate_schema(output_path: str) -> bool:
    """Check output parquet has all expected columns."""
    schema = pq.read_schema(output_path)
    output_cols = set(schema.names)
    expected = set(TARGET_COLUMNS)
    missing = expected - output_cols
    if missing:
        print(f"WARNING: missing columns: {missing}")
        return False
    print(f"Schema OK — {len(schema.names)} columns")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Convert raw NSI GPKG/GeoJSON to processed Parquet"
    )
    parser.add_argument("--input", required=True, help="Path to GPKG/GeoJSON (or glob)")
    parser.add_argument("--output", required=True, help="Output parquet path")
    parser.add_argument(
        "--engine",
        default="duckdb",
        choices=["duckdb", "geopandas"],
        help="Processing engine (default: duckdb)",
    )
    args = parser.parse_args()

    # Resolve glob
    inputs = sorted(glob.glob(args.input))
    if not inputs:
        print(f"ERROR: no files match '{args.input}'")
        sys.exit(1)

    input_path = inputs[0]
    if len(inputs) > 1:
        print(f"WARNING: multiple files matched, using first: {input_path}")

    print(f"Converting {input_path} → {args.output} (engine={args.engine})")

    convert = _convert_duckdb if args.engine == "duckdb" else _convert_geopandas
    try:
        count = convert(input_path, args.output)
    except Exception as e:
        if args.engine == "duckdb":
            print(f"DuckDB failed ({e}), falling back to geopandas...")
            count = _convert_geopandas(input_path, args.output)
        else:
            raise

    print(f"Wrote {count:,} rows to {args.output}")
    validate_schema(args.output)


if __name__ == "__main__":
    main()
