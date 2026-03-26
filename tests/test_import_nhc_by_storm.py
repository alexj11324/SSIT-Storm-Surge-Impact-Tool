import io
import zipfile
from unittest.mock import MagicMock, patch

import geopandas as gpd
import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from shapely.geometry import box

from scripts import import_nhc_by_storm as nhc


def _make_zip_bytes(storm_name: str, year: int, adv: int) -> bytes:
    filename = f"{storm_name}_{year}_adv{adv}_e10_ResultMaskRaster.tif"
    transform = from_origin(0, 1, 1, 1)
    data = np.ones((1, 1), dtype=np.uint8)

    with MemoryFile() as memfile:
        with memfile.open(
            driver="GTiff",
            height=1,
            width=1,
            count=1,
            dtype="uint8",
            crs="EPSG:4326",
            transform=transform,
        ) as dataset:
            dataset.write(data, 1)
        tif_bytes = memfile.read()

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr(filename, tif_bytes)
    return buffer.getvalue()


class _DummyResponse:
    def __init__(self, content: bytes):
        self.content = content

    def raise_for_status(self) -> None:
        return None


@patch("scripts.import_nhc_by_storm.gpd.sjoin")
@patch("scripts.import_nhc_by_storm.states")
def test_importer_returns_states_and_readable_raster(mock_states, mock_sjoin):
    zip_bytes = _make_zip_bytes("BERYL", 2024, 29)
    mock_session = MagicMock()
    mock_session.get.return_value = _DummyResponse(zip_bytes)

    state_geom = box(-1, -1, 1, 1)
    states_gdf = gpd.GeoDataFrame({"NAME": ["TestState"], "geometry": [state_geom]}, crs="EPSG:4326")
    mock_states.return_value = states_gdf
    mock_sjoin.return_value = states_gdf

    result = nhc.import_surge_data("AL022024", "beryl", 29, 2024, session=mock_session, timeout=5)

    assert result["states"] == ["TestState"]
    data = result["data"].read(1)
    assert data.shape == (1, 1)
    mock_session.get.assert_called_once_with(
        "https://www.nhc.noaa.gov/gis/inundation/forecasts/AL022024_029_tidalmask.zip",
        stream=True,
        timeout=5,
    )
    result["data"].close()


@patch("scripts.import_nhc_by_storm.gpd.sjoin")
@patch("scripts.import_nhc_by_storm.states")
def test_importer_handles_no_overlapping_states(mock_states, mock_sjoin):
    zip_bytes = _make_zip_bytes("BERYL", 2024, 29)
    mock_session = MagicMock()
    mock_session.get.return_value = _DummyResponse(zip_bytes)

    geom = box(-1, -1, 1, 1)
    empty_gdf = gpd.GeoDataFrame({"NAME": [], "geometry": []}, geometry="geometry", crs="EPSG:4326")
    mock_states.return_value = gpd.GeoDataFrame({"NAME": ["Other"], "geometry": [geom]}, crs="EPSG:4326")
    mock_sjoin.return_value = empty_gdf

    result = nhc.import_surge_data("AL0224", "BERYL", 29, 2024, session=mock_session, timeout=5)

    assert result["states"] == []
    result["data"].close()


def test_normalizes_storm_id_with_two_digit_year():
    assert nhc._normalize_storm_id("AL0224", 2024) == "AL022024"
    assert nhc._normalize_storm_id("al02", 2024) == "AL022024"
    assert nhc._normalize_storm_id("AL022024", 2024) == "AL022024"
