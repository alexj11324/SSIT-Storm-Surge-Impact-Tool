"""Test that the IsUTM branch works correctly with projected rasters.

The existing parity test uses a geographic (NAD83) raster where IsUTM=False.
This test creates a synthetic UTM Zone 17N (EPSG:32617) raster to exercise
the IsUTM=True branch where utm.from_latlon is called for coordinate conversion.
"""

import csv
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np


class UtmRasterTest(unittest.TestCase):
    def setUp(self):
        self.project_root = Path(__file__).resolve().parents[1]
        self.temp_dir = Path(tempfile.mkdtemp(prefix="fast_utm_test_"))
        self.output_dir = self.temp_dir / "output"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_utm_raster_produces_nonzero_depth(self):
        try:
            import rasterio
            from rasterio.crs import CRS
            from rasterio.transform import from_origin
        except Exception as exc:
            self.skipTest("rasterio is required: {}".format(exc))

        try:
            import utm as utm_lib
        except Exception as exc:
            self.skipTest("utm is required: {}".format(exc))

        # Create a small synthetic UTM raster (EPSG:32617 = UTM Zone 17N, metres)
        # Cover an area around a known lat/lon point
        # lat=28.0, lon=-82.5 (Tampa FL area) -> UTM 17N: ~358000 E, ~3098000 N
        test_lat, test_lon = 28.0, -82.5
        utm_e, utm_n, _, _ = utm_lib.from_latlon(test_lat, test_lon)

        # 10x10 raster, 100m pixels, flood depth = 3.5 feet everywhere
        pixel_size = 100.0
        raster_width, raster_height = 10, 10
        origin_e = utm_e - 5 * pixel_size
        origin_n = utm_n + 5 * pixel_size

        flood_depth = 3.5
        data = np.full((raster_height, raster_width), flood_depth, dtype=np.float32)

        raster_path = self.temp_dir / "utm_test_raster.tif"
        transform = from_origin(origin_e, origin_n, pixel_size, pixel_size)

        with rasterio.open(
            raster_path,
            "w",
            driver="GTiff",
            height=raster_height,
            width=raster_width,
            count=1,
            dtype="float32",
            crs=CRS.from_proj4("+proj=utm +zone=17 +datum=WGS84 +units=m +no_defs"),
            transform=transform,
            nodata=-9999.0,
        ) as dst:
            dst.write(data, 1)

        # Verify raster is projected with metre units
        with rasterio.open(raster_path) as src:
            self.assertTrue(src.crs.is_projected)
            self.assertEqual(src.crs.linear_units, "metre")

        # Create a minimal inventory CSV with one building at the test location
        inventory_path = self.temp_dir / "test_inventory.csv"
        fieldnames = [
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
        with open(inventory_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(
                {
                    "FltyId": "TEST001",
                    "Occ": "RES1",
                    "Cost": "150000",
                    "Area": "1500",
                    "NumStories": "1",
                    "FoundationType": "7",
                    "FirstFloorHt": "1.0",
                    "ContentCost": "75000",
                    "Latitude": str(test_lat),
                    "Longitude": str(test_lon),
                }
            )

        # Run FAST
        python_env_dir = self.project_root / "Python_env"
        if str(python_env_dir) not in sys.path:
            sys.path.insert(0, str(python_env_dir))

        from run_fast import run_fast

        mapping = {
            "UserDefinedFltyId": "FltyId",
            "OCC": "Occ",
            "Cost": "Cost",
            "Area": "Area",
            "NumStories": "NumStories",
            "FoundationType": "FoundationType",
            "FirstFloorHt": "FirstFloorHt",
            "ContentCost": "ContentCost",
            "BDDF_ID": "",
            "CDDF_ID": "",
            "IDDF_ID": "",
            "InvCost": "",
            "SOID": "",
            "Latitude": "Latitude",
            "Longitude": "Longitude",
        }

        success, msg, errors = run_fast(
            inventory_path=str(inventory_path),
            mapping=mapping,
            flc="CoastalA",
            rasters=[str(raster_path)],
            output_dir=str(self.output_dir),
            project_root=str(self.project_root),
        )

        self.assertTrue(success, msg="FAST run failed: {}".format(msg))

        # Find output CSV and verify depth was extracted
        output_files = list(self.output_dir.glob("*.csv"))
        non_sorted = [f for f in output_files if "_sorted" not in f.name]
        self.assertTrue(len(non_sorted) > 0, "No output CSV produced")

        with open(non_sorted[0], newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        self.assertEqual(len(rows), 1, "Expected exactly 1 output row")
        depth_grid = float(rows[0]["Depth_Grid"])
        self.assertAlmostEqual(
            depth_grid, flood_depth, places=1, msg="UTM raster depth extraction failed: got {}".format(depth_grid)
        )

        bldg_dmg = float(rows[0]["BldgDmgPct"])
        self.assertGreater(bldg_dmg, 0, "Expected nonzero building damage for 3.5ft flood depth")


if __name__ == "__main__":
    unittest.main()
