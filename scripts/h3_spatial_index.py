"""H3 hexagonal pre-indexing for fast spatial filtering of NSI buildings against flood rasters."""

import argparse

import h3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
from rasterio.transform import xy


def raster_to_h3_cells(raster_path: str, resolution: int = 7, stride: int = 4) -> set[str]:
    """Convert valid flood pixels to H3 cell IDs, sampling every Nth pixel."""
    with rasterio.open(raster_path) as src:
        data = src.read(1)
        nodata = src.nodata
        mask = data > 0
        if nodata is not None:
            mask &= data != nodata

        rows, cols = np.where(mask)
        rows, cols = rows[::stride], cols[::stride]
        xs, ys = xy(src.transform, rows, cols)

        # xs/ys are in CRS coords; reproject to lon/lat if needed
        if src.crs and not src.crs.is_geographic:
            from pyproj import Transformer

            transformer = Transformer.from_crs(src.crs, "EPSG:4326", always_xy=True)
            xs, ys = transformer.transform(xs, ys)

        return {h3.latlng_to_cell(lat, lon, resolution) for lon, lat in zip(xs, ys)}


def filter_buildings_by_h3(
    parquet_path: str,
    flood_cells: set[str],
    resolution: int = 7,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
) -> pa.Table:
    """Filter parquet buildings to only those in flood H3 cells. O(1) per building."""
    table = pq.read_table(parquet_path)
    lats = table.column(lat_col).to_pylist()
    lons = table.column(lon_col).to_pylist()

    keep = [h3.latlng_to_cell(lat, lon, resolution) in flood_cells for lat, lon in zip(lats, lons)]
    return table.filter(pa.array(keep, type=pa.bool_()))


def filter_buildings_batch(
    parquet_paths: list[str],
    flood_cells: set[str],
    resolution: int = 7,
) -> pa.Table:
    """Filter multiple parquet files and combine results."""
    tables = [filter_buildings_by_h3(p, flood_cells, resolution) for p in parquet_paths]
    return pa.concat_tables(tables) if tables else pa.table({})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="H3 spatial filter for NSI buildings")
    parser.add_argument("--raster", required=True, help="Flood depth GeoTIFF")
    parser.add_argument("--parquet", required=True, nargs="+", help="NSI parquet file(s)")
    parser.add_argument("--resolution", type=int, default=7)
    parser.add_argument("--stride", type=int, default=4)
    args = parser.parse_args()

    print(f"Indexing raster {args.raster} at H3 res {args.resolution} (stride={args.stride})...")
    flood_cells = raster_to_h3_cells(args.raster, args.resolution, args.stride)
    print(f"  {len(flood_cells)} unique H3 cells with flooding")

    result = filter_buildings_batch(args.parquet, flood_cells, args.resolution)
    print(f"  {result.num_rows} buildings in flood zone (from {len(args.parquet)} file(s))")
