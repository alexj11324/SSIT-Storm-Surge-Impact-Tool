from __future__ import annotations

import io
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import download_nsi_by_state as downloader
from scripts import nsi_raw_to_parquet as raw_to_parquet


def _write_schema_only_parquet(path: Path, rows: int = 1) -> None:
    arrays = [
        pa.array([None] * rows, type=field.type)
        for field in raw_to_parquet.TARGET_SCHEMA
    ]
    table = pa.Table.from_arrays(arrays, schema=raw_to_parquet.TARGET_SCHEMA)
    pq.write_table(table, path, compression="snappy")


def test_normalize_state_identifier_variants() -> None:
    florida = downloader.normalize_state_identifier("Florida")
    assert florida.abbr == "FL"
    assert florida.fips == "12"

    assert downloader.normalize_state_identifier("FL") == florida
    assert downloader.normalize_state_identifier("12") == florida

    north_carolina = downloader.normalize_state_identifier("North Carolina")
    assert north_carolina.abbr == "NC"
    assert north_carolina.fips == "37"
    assert downloader.normalize_state_identifier("North_Carolina") == north_carolina


def test_write_feature_collection_wraps_stream() -> None:
    lines = [
        '{"type":"Feature","geometry":{"type":"Point","coordinates":[1,2]},"properties":{"bid":"a"}}\n',
        '{"type":"Feature","geometry":{"type":"Point","coordinates":[3,4]},"properties":{"bid":"b"}}\n',
    ]

    output = io.StringIO()

    count = downloader.write_feature_collection(lines, output)
    assert count == 2

    wrapped = json.loads(output.getvalue())
    assert wrapped["type"] == "FeatureCollection"
    assert len(wrapped["features"]) == 2
    assert wrapped["features"][0]["properties"]["bid"] == "a"
    assert wrapped["features"][1]["properties"]["bid"] == "b"


def test_write_feature_collection_rejects_empty_stream() -> None:
    with pytest.raises(ValueError, match="empty feature stream"):
        downloader.write_feature_collection([], io.StringIO())


def test_download_state_inventory_uses_expected_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_download_state_geojson(*, state, destination, timeout, retries, overwrite):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            '{"type":"FeatureCollection","features":[]}',
            encoding="utf-8",
        )
        return 2, destination.stat().st_size

    def fake_convert_geojson_to_parquet(*, raw_geojson_path, parquet_path, engine, overwrite):
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        _write_schema_only_parquet(parquet_path, rows=2)
        return 2, parquet_path.stat().st_size

    monkeypatch.setattr(downloader, "download_state_geojson", fake_download_state_geojson)
    monkeypatch.setattr(downloader, "convert_geojson_to_parquet", fake_convert_geojson_to_parquet)

    state = downloader.normalize_state_identifier("North_Carolina")
    result = downloader.download_state_inventory(
        state=state,
        output_dir=tmp_path,
        engine="duckdb",
        overwrite=False,
        timeout=30.0,
        retries=1,
    )

    assert result["state"] == "North Carolina"
    assert result["raw_geojson"].endswith("raw/nsi_2022_37_North_Carolina.geojson")
    assert result["processed_parquet"].endswith(
        "processed/nsi/state=North_Carolina/part-00000.snappy.parquet"
    )
    assert Path(result["raw_geojson"]).exists()
    assert Path(result["processed_parquet"]).exists()


def test_download_state_inventory_refuses_existing_outputs_without_overwrite(tmp_path: Path) -> None:
    state = downloader.normalize_state_identifier("Delaware")
    raw_path = tmp_path / "raw" / "nsi_2022_10_Delaware.geojson"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text("already here", encoding="utf-8")

    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        downloader.download_state_inventory(
            state=state,
            output_dir=tmp_path,
            engine="duckdb",
            overwrite=False,
            timeout=30.0,
            retries=1,
        )


def test_write_manifest_persists_json_and_cleans_temp_file(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    output_dir.mkdir(parents=True)
    entries = [
        {
            "state": "Delaware",
            "state_path": "Delaware",
            "abbr": "DE",
            "fips": "10",
            "api_url": "https://example.test",
            "feature_count": 1,
            "raw_geojson": "/tmp/raw.geojson",
            "raw_geojson_bytes": 123,
            "processed_parquet": "/tmp/out.parquet",
            "processed_parquet_rows": 1,
            "processed_parquet_bytes": 456,
        }
    ]

    manifest_path = downloader.write_manifest(
        output_dir=output_dir,
        state_results=entries,
        engine="duckdb",
        timeout=60.0,
        retries=3,
        overwrite=False,
    )

    assert manifest_path.exists()
    assert not manifest_path.with_name("manifest.json.part").exists()
    content = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert content["engine"] == "duckdb"
    assert content["states"] == entries


def test_convert_raw_nsi_to_parquet_reuses_public_entrypoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"geopandas": False}

    def fake_geopandas(input_path: str, output_path: str) -> int:
        called["geopandas"] = True
        _write_schema_only_parquet(Path(output_path), rows=1)
        return 1

    monkeypatch.setattr(raw_to_parquet, "_convert_geopandas", fake_geopandas)

    output_path = tmp_path / "nested" / "out.parquet"
    count = raw_to_parquet.convert_raw_nsi_to_parquet(
        input_path=str(tmp_path / "fake.geojson"),
        output_path=str(output_path),
        engine="geopandas",
    )

    assert called["geopandas"] is True
    assert count == 1
    assert output_path.exists()
    assert raw_to_parquet.validate_schema(str(output_path)) is True
