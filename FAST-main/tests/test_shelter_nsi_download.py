import io
import json
import sys
import tempfile
import unittest
import warnings
from http.client import IncompleteRead
from pathlib import Path
from unittest import mock

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import scripts.nsi_downloader as nsi_mod  # noqa: E402
from scripts.nsi_downloader import NSIDownloader  # noqa: E402


def _feature_line(bid, longitude=1.0, latitude=2.0):
    feature = {
        "type": "Feature",
        "properties": {
            "bid": bid,
            "occtype": "RES1",
            "val_struct": 1000,
            "sqft": 100,
            "num_story": 1,
            "found_type": 4,
            "found_ht": 1,
            "val_cont": 250,
            "cbfips": "060010000000000",
            "pop2pmu65": 0,
            "pop2pmo65": 0,
        },
        "geometry": {"type": "Point", "coordinates": [longitude, latitude]},
    }
    return json.dumps(feature) + "\n"


class ShelterNsiDownloadTest(unittest.TestCase):
    def test_stream_retries_when_urlopen_raises_incomplete_read(self):
        downloader = NSIDownloader(Path("."))
        attempts = []
        responses = [
            io.BytesIO((_feature_line("retry-a") + _feature_line("retry-b")).encode("utf-8")),
        ]

        def fake_urlopen(_request, timeout=0):
            attempts.append(timeout)
            if len(attempts) == 1:
                raise IncompleteRead(b"", 1024)
            return responses[0]

        with mock.patch.object(nsi_mod.urllib_request, "urlopen", side_effect=fake_urlopen):
            with mock.patch.object(nsi_mod.time, "sleep"):
                rows, _nbytes = downloader.stream_features("https://example.test/nsi", timeout=1, retries=2)

        self.assertEqual(len(attempts), 2)
        self.assertEqual([row["bid"] for row in rows], ["retry-a", "retry-b"])

    def test_stream_retries_when_a_feature_line_is_malformed(self):
        downloader = NSIDownloader(Path("."))
        attempts = []
        responses = [
            io.BytesIO(
                (_feature_line("first-attempt") + '{"type": "Feature", "properties": {"bid": "broken"}\n').encode(
                    "utf-8"
                )
            ),
            io.BytesIO((_feature_line("retry-a") + _feature_line("retry-b")).encode("utf-8")),
        ]

        def fake_urlopen(_request, timeout=0):
            attempts.append(timeout)
            return responses[len(attempts) - 1]

        with mock.patch.object(nsi_mod.urllib_request, "urlopen", side_effect=fake_urlopen):
            with mock.patch.object(nsi_mod.time, "sleep"):
                rows, _nbytes = downloader.stream_features("https://example.test/nsi", timeout=1, retries=2)

        self.assertEqual(len(attempts), 2)
        self.assertEqual([row["bid"] for row in rows], ["retry-a", "retry-b"])

    def test_partial_large_state_downloads_are_not_cached(self):
        with tempfile.TemporaryDirectory(prefix="nsi_cache_test_") as temp_dir:
            downloader = NSIDownloader(Path(temp_dir))

            failed_county = "06003"
            counties_gdf = gpd.GeoDataFrame(
                {"GEOID": ["06001", failed_county]},
                geometry=[box(0, 0, 1, 1), box(10, 10, 11, 11)],
                crs="EPSG:4326",
            )

            def fake_get_county_fips(_state_fips):
                return ["06001", failed_county], counties_gdf

            def fake_stream(url):
                if "06001" in url:
                    return (
                        [
                            {
                                "bid": "row-1",
                                "occtype": "RES1",
                                "longitude": 1.0,
                                "latitude": 2.0,
                            }
                        ],
                        10,
                    )
                raise OSError("transient download failure")

            with mock.patch.object(downloader, "county_fips", side_effect=fake_get_county_fips):
                with mock.patch.object(downloader, "stream_features", side_effect=fake_stream):
                    with warnings.catch_warnings(record=True) as caught:
                        warnings.simplefilter("always")
                        df, _nbytes = downloader.download_state(
                            "California",
                            raster_bbox_polygon=box(0, 0, 2, 2),
                        )

            cache_path = downloader.work_dir / "nsi_california.parquet"

            self.assertEqual(len(df), 1)
            self.assertFalse(cache_path.exists())
            self.assertTrue(any("counties failed but none intersect raster" in str(item.message) for item in caught))


if __name__ == "__main__":
    unittest.main()
