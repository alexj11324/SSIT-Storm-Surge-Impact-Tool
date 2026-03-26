"""Generate GeoTIFF flood depth rasters from SLOSH parquet data."""

import argparse
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_bounds
from shapely import wkt

NODATA = -9999.0


def _to_float_or_none(value):
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(number):
        return None
    return number


def slosh_to_raster(
    parquet_path: str,
    output_tif: str,
    category: int = 3,
    scenario: str = "mean",
    resolution: float = 0.001,
    crs: str | None = "EPSG:4326",
) -> str:
    """Rasterize SLOSH surge polygons to a GeoTIFF."""
    col = f"c{category}_{scenario}"
    try:
        table = pq.read_table(parquet_path, columns=["geometry_wkt", col, "topography"])
    except pa.ArrowInvalid as exc:
        raise ValueError(
            "SLOSH parquet must include geometry_wkt, topography, and scenario column {col}".format(
                col=col
            )
        ) from exc

    geom_wkts = table.column("geometry_wkt").to_pylist()
    surge_values = table.column(col).to_pylist()
    topography_values = table.column("topography").to_pylist()

    shapes = []
    for geom_wkt, surge_value, topo_value in zip(geom_wkts, surge_values, topography_values):
        if geom_wkt is None:
            continue
        surge = _to_float_or_none(surge_value)
        topography = _to_float_or_none(topo_value)
        if surge is None or topography is None:
            continue
        depth = max(0.0, surge - topography)
        if depth <= 0:
            continue
        shapes.append((wkt.loads(geom_wkt), depth))

    if not shapes:
        raise ValueError(
            "No positive inundation depth values found for scenario column {col}".format(col=col)
        )
    # Ensure higher depths overwrite lower depths when polygons overlap.
    shapes.sort(key=lambda item: item[1])

    all_bounds = [s[0].bounds for s in shapes]
    minx = min(b[0] for b in all_bounds)
    miny = min(b[1] for b in all_bounds)
    maxx = max(b[2] for b in all_bounds)
    maxy = max(b[3] for b in all_bounds)

    width = max(1, int(np.ceil((maxx - minx) / resolution)))
    height = max(1, int(np.ceil((maxy - miny) / resolution)))
    transform = from_bounds(minx, miny, maxx, maxy, width, height)

    raster = rasterize(
        shapes,
        out_shape=(height, width),
        transform=transform,
        fill=NODATA,
        dtype="float32",
    )

    Path(output_tif).parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        output_tif,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype="float32",
        crs=crs,
        transform=transform,
        nodata=NODATA,
    ) as dst:
        dst.write(raster, 1)

    return output_tif


def batch_rasterize(
    parquet_path: str,
    output_dir: str,
    categories: list[int] | None = None,
    scenarios: list[str] | None = None,
    resolution: float = 0.001,
    crs: str | None = "EPSG:4326",
) -> list[str]:
    """Generate rasters for multiple category/scenario combinations."""
    categories = categories or [0, 1, 2, 3, 4, 5]
    scenarios = scenarios or ["mean", "high"]
    schema = pq.read_schema(parquet_path)
    available_columns = set(schema.names)
    if "geometry_wkt" not in available_columns or "topography" not in available_columns:
        raise ValueError("SLOSH parquet must include geometry_wkt and topography columns.")

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = []
    for cat in categories:
        for scn in scenarios:
            scenario_col = f"c{cat}_{scn}"
            if scenario_col not in available_columns:
                continue
            tif = str(out / f"slosh_c{cat}_{scn}.tif")
            slosh_to_raster(parquet_path, tif, cat, scn, resolution, crs=crs)
            paths.append(tif)
    return paths


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SLOSH parquet to GeoTIFF")
    parser.add_argument("--parquet", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--category", type=int, default=3)
    parser.add_argument("--scenario", default="mean")
    parser.add_argument("--resolution", type=float, default=0.001)
    args = parser.parse_args()
    path = slosh_to_raster(args.parquet, args.output, args.category, args.scenario, args.resolution)
    print(f"Wrote {path}")
