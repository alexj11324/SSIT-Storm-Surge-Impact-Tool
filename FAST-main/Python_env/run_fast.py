"""Server/headless FAST entrypoint with unified CSV/Parquet execution."""

import argparse
import json
import os
import sys

DEFAULT_FIELD_MAP_KEYS = [
    "UserDefinedFltyId",
    "OCC",
    "Cost",
    "Area",
    "NumStories",
    "FoundationType",
    "FirstFloorHt",
    "ContentCost",
    "BDDF_ID",
    "CDDF_ID",
    "IDDF_ID",
    "InvCost",
    "SOID",
    "Latitude",
    "Longitude",
]


def _load_mapping(mapping_json_arg):
    mapping_input = (mapping_json_arg or "").strip()
    if mapping_input == "":
        raise ValueError("--mapping-json is required.")

    if os.path.isfile(mapping_input):
        with open(mapping_input, "r", encoding="utf-8") as mapping_file:
            mapping = json.load(mapping_file)
    else:
        mapping = json.loads(mapping_input)

    if not isinstance(mapping, dict):
        raise ValueError("mapping JSON must be an object keyed by FAST field names.")
    return mapping


def _build_field_map(mapping):
    return [mapping.get(key, "") for key in DEFAULT_FIELD_MAP_KEYS]


def _normalize_rasters(rasters):
    normalized = []
    for raster in rasters:
        if raster is None:
            continue
        parts = [part.strip() for part in str(raster).split(",")]
        normalized.extend([part for part in parts if part != ""])
    if len(normalized) == 0:
        raise ValueError("At least one raster must be provided.")
    return normalized


def run_fast(
    inventory_path,
    mapping,
    flc,
    rasters,
    output_dir=None,
    project_root=None,
    log_path=None,
    qc_warning=False,
):
    """Execute FAST for one inventory input (CSV or Parquet) and one/more rasters."""
    from hazus_notinuse import local_with_options

    field_map = _build_field_map(mapping) if isinstance(mapping, dict) else mapping
    return local_with_options(
        inventory_path=inventory_path,
        field_map=field_map,
        raster_names_or_paths=rasters,
        flood_type=flc,
        output_dir=output_dir,
        project_root=project_root,
        log_path=log_path,
        qc_warning="True" if qc_warning else "False",
    )


def _create_parser():
    parser = argparse.ArgumentParser(description="Run FAST headless with CSV or Parquet inventory input.")
    parser.add_argument("--inventory", required=True, help="Path to inventory file (.csv or .parquet).")
    parser.add_argument(
        "--mapping-json",
        required=True,
        help="Mapping JSON file path or inline JSON object keyed by FAST fields.",
    )
    parser.add_argument(
        "--flc",
        required=True,
        help="Flood type: Riverine / CoastalA / CoastalV (or internal codes HazardRiverine / CAE / V).",
    )
    parser.add_argument(
        "--rasters",
        nargs="+",
        required=True,
        help="One or more raster names (under rasters/) or absolute raster paths.",
    )
    parser.add_argument("--output-dir", default=None, help="Output directory for FAST result CSV files.")
    parser.add_argument("--project-root", default=None, help="FAST project root containing Lookuptables and rasters.")
    parser.add_argument("--log-path", default=None, help="Optional FAST log file path.")
    parser.add_argument("--qc-warning", action="store_true", help="Enable QC warnings.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON result.")
    return parser


def main(argv=None):
    parser = _create_parser()
    args = parser.parse_args(argv)

    try:
        mapping = _load_mapping(args.mapping_json)
        rasters = _normalize_rasters(args.rasters)
        result = run_fast(
            inventory_path=args.inventory,
            mapping=mapping,
            flc=args.flc,
            rasters=rasters,
            output_dir=args.output_dir,
            project_root=args.project_root,
            log_path=args.log_path,
            qc_warning=args.qc_warning,
        )
        success, message = result[0], result[1]
        row_errors = result[2] if len(result) > 2 else 0
        payload = {
            "success": bool(success),
            "message": message,
            "row_errors": row_errors,
            "inventory": os.path.abspath(args.inventory),
            "rasters": rasters,
            "flc": args.flc,
            "output_dir": os.path.abspath(args.output_dir)
            if args.output_dir
            else os.path.dirname(os.path.abspath(args.inventory)),
            "project_root": os.path.abspath(args.project_root) if args.project_root else None,
        }
        print(json.dumps(payload, indent=2 if args.pretty else None))
        if not success:
            print(f"FAST error: {message}", file=sys.stderr)
        return 0 if success else 1
    except Exception as exc:
        payload = {"success": False, "error": str(exc), "row_errors": 0}
        print(json.dumps(payload, indent=2 if args.pretty else None))
        print(f"FAST error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
