import csv
import hashlib
import shutil
import tempfile
import unittest
from pathlib import Path


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as input_file:
        while True:
            chunk = input_file.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


class CsvParquetParityTest(unittest.TestCase):
    def setUp(self):
        self.project_root = Path(__file__).resolve().parents[1]
        self.sample_source_csv = self.project_root / "UDF" / "ND_Minot_UDF.csv"
        self.sample_raster = "BERYL_2024_adv41_e10_ResultMaskRaster.tif"
        self.temp_dir = Path(tempfile.mkdtemp(prefix="fast_parity_test_"))
        self.csv_input_path = self.temp_dir / "sample_input.csv"
        self.parquet_input_path = self.temp_dir / "sample_input.parquet"
        self.csv_output_dir = self.temp_dir / "csv_output"
        self.parquet_output_dir = self.temp_dir / "parquet_output"
        self.csv_output_dir.mkdir(parents=True, exist_ok=True)
        self.parquet_output_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_csv_and_parquet_outputs_are_byte_identical(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except Exception as exc:
            self.skipTest("pyarrow is required for parity test: {err}".format(err=exc))

        try:
            import rasterio  # noqa: F401
        except Exception as exc:
            self.skipTest("rasterio is required for parity test: {err}".format(err=exc))

        import sys

        python_env_dir = self.project_root / "Python_env"
        if str(python_env_dir) not in sys.path:
            sys.path.insert(0, str(python_env_dir))

        from run_fast import run_fast

        with open(self.sample_source_csv, newline="") as source_file:
            reader = csv.DictReader(source_file)
            rows = []
            for index, row in enumerate(reader):
                rows.append(row)
                if index >= 9:
                    break
            fieldnames = reader.fieldnames

        with open(self.csv_input_path, "w", newline="") as csv_output_file:
            writer = csv.DictWriter(csv_output_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        table = pa.Table.from_pylist(rows)
        pq.write_table(table, self.parquet_input_path)

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

        csv_result = run_fast(
            inventory_path=str(self.csv_input_path),
            mapping=mapping,
            flc="Riverine",
            rasters=[self.sample_raster],
            output_dir=str(self.csv_output_dir),
            project_root=str(self.project_root),
        )
        parquet_result = run_fast(
            inventory_path=str(self.parquet_input_path),
            mapping=mapping,
            flc="Riverine",
            rasters=[self.sample_raster],
            output_dir=str(self.parquet_output_dir),
            project_root=str(self.project_root),
        )

        self.assertTrue(csv_result[0], msg="CSV run failed: {msg}".format(msg=csv_result[1]))
        self.assertTrue(parquet_result[0], msg="Parquet run failed: {msg}".format(msg=parquet_result[1]))

        base_name = "sample_input_BERYL_2024_adv41_e10_ResultMaskRaster"
        csv_out = self.csv_output_dir / (base_name + ".csv")
        parquet_out = self.parquet_output_dir / (base_name + ".csv")
        csv_sorted_out = self.csv_output_dir / (base_name + "_sorted.csv")
        parquet_sorted_out = self.parquet_output_dir / (base_name + "_sorted.csv")

        self.assertTrue(csv_out.exists(), msg="Missing CSV output file")
        self.assertTrue(parquet_out.exists(), msg="Missing Parquet output file")
        self.assertTrue(csv_sorted_out.exists(), msg="Missing CSV sorted output file")
        self.assertTrue(parquet_sorted_out.exists(), msg="Missing Parquet sorted output file")

        self.assertEqual(_sha256(csv_out), _sha256(parquet_out))
        self.assertEqual(_sha256(csv_sorted_out), _sha256(parquet_sorted_out))


if __name__ == "__main__":
    unittest.main()
