import io
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd
import numpy as np
from shapely.geometry import box

from scripts import import_nhc_by_storm as nhc


def _make_zip_bytes(
    storm_name: str, year: int, adv: int, tif_bytes: bytes = b"fake-geotiff-bytes"
) -> bytes:
    filename = f"{storm_name}_{year}_adv{adv}_e10_ResultMaskRaster.tif"

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr(filename, tif_bytes)
    return buffer.getvalue()


def _make_archive_html(*filenames: str) -> str:
    return "\n".join(
        f'<a href="inundation/forecasts/{filename}">{filename}</a><br>' for filename in filenames
    )


class _DummyResponse:
    def __init__(self, content: bytes = b"", *, text: str | None = None, status_code: int = 200):
        self.content = content
        self.text = text if text is not None else content.decode("utf-8", errors="ignore")
        self.status_code = status_code
        self.headers = {"Content-Length": str(len(content))}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise nhc.requests.HTTPError(f"{self.status_code} Client Error")

    def iter_content(self, chunk_size: int = 1024) -> list[bytes]:
        return [self.content]

    def close(self) -> None:
        pass


class _DummyErrorResponse(_DummyResponse):
    def raise_for_status(self) -> None:
        raise nhc.requests.HTTPError("404 Client Error")


def _patch_memory_file():
    dataset = MagicMock()
    dataset.bounds = type("Bounds", (), {"left": 0, "bottom": 0, "right": 1, "top": 1})()
    dataset.crs = "EPSG:4326"
    dataset.read.return_value = np.ones((1, 1), dtype=np.uint8)
    memory_file = MagicMock()
    memory_file.open.return_value = dataset
    return patch("scripts.import_nhc_by_storm.MemoryFile", return_value=memory_file), dataset


@patch("scripts.import_nhc_by_storm.gpd.sjoin")
@patch("scripts.import_nhc_by_storm._pygris_states")
def test_importer_returns_states_and_readable_raster(mock_states, mock_sjoin):
    zip_bytes = _make_zip_bytes("BERYL", 2024, 29)
    mock_session = MagicMock()
    mock_session.get.side_effect = [
        _DummyResponse(text=_make_archive_html("AL0224_29_tidalmask.zip")),
        _DummyResponse(zip_bytes),
    ]

    state_geom = box(-1, -1, 1, 1)
    states_gdf = gpd.GeoDataFrame(
        {"NAME": ["TestState"], "geometry": [state_geom]}, crs="EPSG:4326"
    )
    mock_states.return_value = states_gdf
    mock_sjoin.return_value = states_gdf

    memory_file_patch, _ = _patch_memory_file()
    with memory_file_patch:
        result = nhc.import_surge_data(
            "AL022024", "beryl", 29, 2024, session=mock_session, timeout=5
        )

    assert result["states"] == ["TestState"]
    data = result["data"].read(1)
    assert data.shape == (1, 1)
    called_urls = [
        call.kwargs["url"] if "url" in call.kwargs else call.args[0]
        for call in mock_session.get.call_args_list
    ]
    assert called_urls == [
        nhc.NHC_INUNDATION_INDEX_URL,
        "https://www.nhc.noaa.gov/gis/inundation/forecasts/AL0224_29_tidalmask.zip",
    ]
    result["data"].close()


@patch("scripts.import_nhc_by_storm.gpd.sjoin")
@patch("scripts.import_nhc_by_storm._pygris_states")
def test_importer_handles_no_overlapping_states(mock_states, mock_sjoin):
    zip_bytes = _make_zip_bytes("BERYL", 2024, 29)
    mock_session = MagicMock()
    mock_session.get.side_effect = [
        _DummyResponse(text=_make_archive_html("AL0224_29_tidalmask.zip")),
        _DummyResponse(zip_bytes),
    ]

    geom = box(-1, -1, 1, 1)
    empty_gdf = gpd.GeoDataFrame({"NAME": [], "geometry": []}, geometry="geometry", crs="EPSG:4326")
    mock_states.return_value = gpd.GeoDataFrame(
        {"NAME": ["Other"], "geometry": [geom]}, crs="EPSG:4326"
    )
    mock_sjoin.return_value = empty_gdf

    memory_file_patch, _ = _patch_memory_file()
    with memory_file_patch:
        result = nhc.import_surge_data("AL0224", "BERYL", 29, 2024, session=mock_session, timeout=5)

    assert result["states"] == []
    result["data"].close()


def test_normalizes_storm_id_with_two_digit_year():
    assert nhc._normalize_storm_id("AL0224", 2024) == "AL022024"
    assert nhc._normalize_storm_id("al02", 2024) == "AL022024"
    assert nhc._normalize_storm_id(" AL022024 ", 2024) == "AL022024"


@patch("scripts.import_nhc_by_storm.gpd.sjoin")
@patch("scripts.import_nhc_by_storm._pygris_states")
def test_importer_falls_back_to_legacy_and_latest_urls(mock_states, mock_sjoin):
    zip_bytes = _make_zip_bytes("BERYL", 2024, 29)
    mock_session = MagicMock()
    mock_session.get.side_effect = [
        _DummyResponse(text=_make_archive_html()),
        _DummyErrorResponse(b""),
        _DummyResponse(zip_bytes),
    ]

    state_geom = box(-1, -1, 1, 1)
    states_gdf = gpd.GeoDataFrame(
        {"NAME": ["TestState"], "geometry": [state_geom]}, crs="EPSG:4326"
    )
    mock_states.return_value = states_gdf
    mock_sjoin.return_value = states_gdf

    memory_file_patch, _ = _patch_memory_file()
    with memory_file_patch:
        result = nhc.import_surge_data(
            "AL022024", "BERYL", 29, 2024, session=mock_session, timeout=5
        )

    called_urls = [
        call.kwargs["url"] if "url" in call.kwargs else call.args[0]
        for call in mock_session.get.call_args_list
    ]
    assert called_urls == [
        nhc.NHC_INUNDATION_INDEX_URL,
        "https://www.nhc.noaa.gov/gis/inundation/forecasts/AL0224_29_tidalmask.zip",
        "https://www.nhc.noaa.gov/gis/inundation/forecasts/AL0224_029_tidalmask.zip",
    ]
    assert result["states"] == ["TestState"]
    result["data"].close()


def test_download_surge_raster_writes_tif_and_returns_states():
    tif_bytes = b"saved-raster-bytes"

    with (
        patch("scripts.import_nhc_by_storm.import_surge_data") as mock_import,
        patch("scripts.import_nhc_by_storm.remap_surge_categories") as mock_remap,
    ):
        mock_data = MagicMock()
        mock_data.close = MagicMock()
        mock_import.return_value = {
            "data": mock_data,
            "states": ["Florida"],
            "tif_bytes": tif_bytes,
            "tif_name": "BERYL_2024_adv29_e10_ResultMaskRaster.tif",
        }

        temp_dir = Path(tempfile.mkdtemp(prefix="nhc_raster_write_"))
        raster_path, states = nhc.download_surge_raster(
            "AL022024", "BERYL", 29, 2024, output_dir=temp_dir
        )

        assert states == ["Florida"]
        assert Path(raster_path).exists()
        assert Path(raster_path).name == "BERYL_2024_adv29_e10_ResultMaskRaster.tif"
        assert Path(raster_path).read_bytes() == tif_bytes
        mock_data.close.assert_called_once()
        mock_remap.assert_called_once()
