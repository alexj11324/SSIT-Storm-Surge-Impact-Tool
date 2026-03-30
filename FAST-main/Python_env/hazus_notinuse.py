# ***********************THIS IS OBSOLETE******************************************
# THIS IS THE ODLER PYTHON FILE THAT HAD THE LOGIC FOR FLOOD LOSS CALCULATION
# THIS IS NOW MERGED INTO THE hazPy LIBRARY. THE PYTHON ENVIRONMENT AND THE LOSS CALCULATION
# WILL NOW BE MANAGED UNDER hazPy/flood/UDF class
# ***********************THIS IS OBSOLETE******************************************

# DOGAMI ArcGIS Python Script Alternative to the Hazus-MH Flood Model for User-Defined Facilities
#
# This script is published as part of Oregon Department of Geology and Mineral Industries Open-File Report OFR O-18-04,
# and has an accompanying User Guide and Depth-Damage Function library.
# CITATION TITLE: Open-File Report O-18-04, ArcGIS Python Script Alternative to the Hazus-MH Flood Model for User-Defined Facilities
# CITATION ORIGINATOR:  John M. Bauer, Oregon Department of Geology and Mineral Industries,
# 800 NE Oregon Street, Suite 965, Portland, OR 97232
# Downloadable at  http://www.oregongeology.org/pubs/ofr/p-O-18-04.htm
#
# Description:
# Intended as a complement to the Hazus-MH Flood model. By using the Depth Damage Functions (DDF)
# exported from Hazus (methods described elsewhere), we calculate the damage calculations entirely outside
# of Hazus. Basic flow:
#    For each flood depth grid specified by the user:
#        Determine depth of flooding at the UDF point.
#        Process, record-by-record:
#            building/content/inventory damage based on the adjusted flood depth, the type of building, using the look-up tables.
#             Using the building loss ratio, calculates debris and building repair time.
#
#
# Script is intended for users with 'many' flood depth grids generated independently of Hazus-MH Flood,
# and a Hazus-compatible UDF pointfile. Please credit DOGAMI if code is used in whole or in part.
#
# The script's version number is decoupled from the Hazus-MH release version.
#
# The script requires two look-up libraries each, for Building, Content, and Inventory:
#     * One for the default situation, where the user supplies OccupancyClass, FoundationType, and NumStories but not a DDF ID
#    * One for the full library, where the user has some very specific DDF they wish to employ.
#
# Note this script does not replace all of Hazus-MH Flood functionality - it only replaces the UDF-based estimate.
#
#
# Revision History
#    20160502 v1.0 Initial Version
#    20160720 v2.0 Support for Content added. Support for user-supplied DDFs for Building and Content added.
#            Support for Coastal flooding added.
#    20160823 v2.1  Changed the inputs to be interactive. User specifies the pathnames, etc. Can be GUI'd
#            into an ArcToolBox.
#    20170331 v3.0  Extensive modification to structure, to conform to more maintainable coding styles,
#            based expert feedback:
#                Use Hazus convention for parameter Input/Output names.
#                 Added support for ArcToolBox so that the DDF library is at a default location and not hard-coded.
#            Fixed a Content Loss computation bug when user supplied their own Content Loss.
#            Added support for calculating Inventory Loss (and uncovered Hazus implementation bug).
#    20170630 v3.1
#        Debris support added.
#        Blanks for user-supplied DDFIDs are supported - they are treated as Null, and default DDFs are used
#        Enhancements for sensitivity tests.
#            Depth-in-Struc is set to Null when the structure is not exposed (versus -9999)
#            Added attribute "GridName" which specifies the gridname with each record.
#                Useful for multiple result files withappend/summary stats.
#                Results geodatabase has a time stamp embedded at the end of its name "-HHMM" (hour/minute).
#        Direct Economic Loss support added:
#            Time to full functionality - a Minimum and Maximum value (in days) are provided.
#    20171215 v3.2
#        Attribute name updates to conform to Hazus naming standards in anticipation of formal publication.
#        Added a flood depth grid attribute. Appears redundant, but useful when merging multiple
#            flood depth grids and performing pivot tables/summaries.
#        Added ContentCostUSD InventoryCostUSD output attributes.
#        Released as part of DOGAMI Open-File Report O-18-04.
# ---------------------------------------------------------------------------

# Variations between systems; some import these by default, others don't. Be explicit.

# UKS - all logging statements added
import logging
import os, csv, sys, time, math, datetime, subprocess, numpy as np, utm
import rasterio

try:
    import pyarrow.parquet as pq
except Exception:
    pq = None


logger = logging.getLogger("FAST")
logger.setLevel(logging.INFO)
logger.propagate = False

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

FLOOD_TYPE_ALIASES = {
    "": "",
    "riverine": "HazardRiverine",
    "hazardriverine": "HazardRiverine",
    "coastala": "CAE",
    "cae": "CAE",
    "coastalv": "V",
    "v": "V",
    "ve": "V",
}


def _resolve_project_root(project_root=None):
    if project_root:
        return os.path.abspath(project_root)
    return os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))


def _resolve_lookup_tables_dir(project_root):
    candidates = [
        os.path.join(project_root, "Lookuptables"),
        os.path.join(project_root, "lookuptables"),
    ]
    for candidate in candidates:
        if os.path.isdir(candidate):
            return candidate

    for entry in os.listdir(project_root):
        candidate = os.path.join(project_root, entry)
        if entry.lower() == "lookuptables" and os.path.isdir(candidate):
            return candidate

    raise FileNotFoundError("Unable to locate lookup tables directory under: " + project_root)


def _resolve_raster_paths(raster_names_or_paths, project_root):
    if raster_names_or_paths is None:
        raise ValueError("At least one raster must be provided.")
    if isinstance(raster_names_or_paths, str):
        raster_names_or_paths = [raster_names_or_paths]

    raster_dir = os.path.join(project_root, "rasters")
    resolved = []
    for raster in raster_names_or_paths:
        raster_name = "" if raster is None else str(raster).strip()
        if raster_name == "":
            continue
        if os.path.isabs(raster_name):
            raster_path = raster_name
        else:
            raster_path = raster_name if os.path.exists(raster_name) else os.path.join(raster_dir, raster_name)
        resolved.append(os.path.abspath(raster_path))

    if len(resolved) == 0:
        raise ValueError("At least one raster must be provided.")
    return resolved


def _normalize_flood_type(flood_type):
    if flood_type is None:
        return ""
    normalized = str(flood_type).strip()
    return FLOOD_TYPE_ALIASES.get(normalized.lower(), normalized)


def _configure_logger(log_path=None, project_root=None):
    root_dir = _resolve_project_root(project_root)
    if not log_path:
        log_path = os.path.join(root_dir, "Log", "app.log")

    log_path = os.path.abspath(log_path)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    existing_file_handler = None
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            existing_file_handler = handler
            break

    if existing_file_handler and os.path.abspath(existing_file_handler.baseFilename) == log_path:
        return log_path

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    handler = logging.FileHandler(log_path)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return log_path


def _is_parquet_input(path):
    return path.lower().endswith((".parquet", ".pq"))


def _normalize_input_value(value):
    if value is None:
        return ""
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.lower() in ("nan", "none", "null"):
            return ""
        return stripped
    if isinstance(value, float) and math.isnan(value):
        return ""
    if isinstance(value, np.floating) and np.isnan(value):
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore").strip()
    if isinstance(value, np.integer):
        return str(int(value))
    if isinstance(value, np.floating):
        return str(float(value))
    return str(value).strip()


def _get_input_field_names(input_path):
    if _is_parquet_input(input_path):
        if pq is None:
            raise RuntimeError("pyarrow is required for parquet input support.")
        parquet_file = pq.ParquetFile(input_path)
        return parquet_file.schema.names
    with open(input_path, "r+") as f:
        reader = csv.reader(f)
        return next(reader)


def _iter_input_rows(input_path):
    if _is_parquet_input(input_path):
        if pq is None:
            raise RuntimeError("pyarrow is required for parquet input support.")
        parquet_file = pq.ParquetFile(input_path)
        for batch in parquet_file.iter_batches(batch_size=4096):
            data = batch.to_pydict()
            cols = list(data.keys())
            for row_index in range(batch.num_rows):
                row = {}
                for col in cols:
                    row[col] = _normalize_input_value(data[col][row_index])
                yield row
    else:
        with open(input_path, newline="") as csvfile:
            for row in csv.DictReader(csvfile):
                normalized_row = {}
                for key, value in row.items():
                    normalized_row[key] = _normalize_input_value(value)
                yield normalized_row


#########################################################################################################
# Main function. Five parameters See end for main procedure.
#########################################################################################################


def flood_damage(UDFOrig, LUT_Dir, ResultsDir, DepthGrids, QC_Warning, fmap):
    log = []  # CBH
    # UDFOrig = USer-supplied UDF input file. Full pathname required
    # LUT_Dir = folder name where the Lookup table libraries reside
    # ResultsDir = Where the output file geodatabase will be created. Folder (dir) must exist, else fail
    # DepthGrids = one or more flood depth grids
    # QC_Warning = Boolean, report on informative inconsistency observations if selected, otherwise suppress them

    if len(logger.handlers) == 0:
        _configure_logger(project_root=_resolve_project_root())

    logger.info("\n")
    logger.info("Calculation FL Building & Content Losses...")
    try:
        # Measure script performance
        start_time = time.time()
        row_error_count = 0
        QC_Warning = QC_Warning.lower() == "true"

        # Get field names from either CSV or parquet input.
        field_names = _get_input_field_names(UDFOrig)
        #########################################################################################################
        # UDF Input Attributes. The following are standard Hazus names/capitalizations.
        #########################################################################################################
        (
            UserDefinedFltyId,
            OccupancyClass,
            Cost,
            Area,
            NumStories,
            FoundationType,
            FirstFloorHt,
            ContentCost,
            BldgDamageFnID,
            ContDamageFnId,
            InvDamageFnId,
            InvCost,
            SOI,
            latitude,
            longitude,
            flC,
        ) = fmap  # [map for map in fmap if map != '' else]#[value if value != '' and any(value in s for s in field_names) == True else field for field, value in fmap]

        # If your UDF Naming Convention differs from the Hazus namings,
        # you can specify your names here, and override the assignments above
        # Example: (of course, uncomment this)
        # UserDefinedFltyId = "UDF_ID"

        # Note that this script has no use for the following Hazus-MH Flood UDF variables:
        #    Name, Address, City, Statea, Zipcode, Contact, PhoneNumber, YearBuilt, BackupPower,
        #    ShelterCapacity, Latitude, Longitude, Comment, BldgType, DesignLevel, FloodProtection

        #########################################################################################################
        #  UDF Output Attributes
        #########################################################################################################
        # Good programming practice: have these names as variables rather than hardcoded within commands
        # Most users need not change these, unless you do not like the names
        BldgDmgPct = "BldgDmgPct"
        BldgLossUSD = "BldgLossUSD"
        ContentCostUSD = "ContentCostUSD"
        ContDmgPct = "ContDmgPct"
        ContentLossUSD = "ContentLossUSD"
        InventoryCostUSD = "InventoryCostUSD"
        InvDmgPct = "InvDmgPct"
        InventoryLossUSD = "InventoryLossUSD"

        # Note there are no Hazus equivalents for the following output attributes.
        # DOGAMI believes these to be value-added, and suggests Hazus provide this information.
        # See spreadsheet accompanying the script for naming convention
        flExp = "flExp"
        Depth_in_Struc = "Depth_in_Struc"
        Depth_Grid = "Depth_Grid"  # The renamed raster sample data. "RASTERVALU" is not a useful name
        SOID = "SOID" if SOI == "" else SOI  # Specific Occupancy ID
        BDDF_ID = "BDDF_ID" if BldgDamageFnID == "" else BldgDamageFnID
        CDDF_ID = "CDDF_ID" if ContDamageFnId == "" else ContDamageFnId
        IDDF_ID = "IDDF_ID" if InvDamageFnId == "" else InvDamageFnId
        DebrisID = "DebrisID"
        Debris_Fin = "Debris_Fin"  # Debris for Finish work
        Debris_Struc = "Debris_Struc"  # Debris from structural elements
        Debris_Found = "Debris_Found"  # Debris from foundation
        Debris_Tot = "Debris_Tot"  # Total Debris - sum of the previous
        GridName = "GridName"
        Restor_Days_Min = "Restor_Days_Min"  # Repair/Restoration times
        Restor_Days_Max = "Restor_Days_Max"
        #########################################################################################################
        #  Setups for other namings.
        #########################################################################################################
        # Building, Content, Inventory DDF Lookup tables. Use these if user does not supply their own DDF_ID
        # Note that Inventory has no unique LUTs for Coastal Zones.
        # Prefix Naming Convention in this program:
        #   B   Building
        #   C   Content
        #   I   Inventory
        BR = "Building_DDF_Riverine_LUT_Hazus4p0.csv"
        BCA = "Building_DDF_CoastalA_LUT_Hazus4p0.csv"
        BCV = "Building_DDF_CoastalV_LUT_Hazus4p0.csv"
        BFull = "flBldgStructDmgFn.csv"  # Full DDF library for Building Structural damage

        CR = "Content_DDF_Riverine_LUT_Hazus4p0.csv"
        CCA = "Content_DDF_CoastalA_LUT_Hazus4p0.csv"
        CCV = "Content_DDF_CoastalV_LUT_Hazus4p0.csv"
        CFull = "flBldgContDmgFn.csv"  # Full DDF library for Building Content damage

        IR = "Inventory_DDF_LUT_Hazus4p0.csv"
        IFull = "flBldgInvDmgFn.csv"  # Full DDF library for Building Inventory damage
        IEconParams = "flBldgEconParamSalesAndInv.csv"  # Needed to calculate business inventory value and loss
        DebrisX = "flDebris_LUT.csv"  # A synthesis of [dbo].[flDebris] and information Hazus Flood Technical Manual (2011), Table 11.1
        RestFnc = (
            "flRsFnGBS_LUT.csv"  # A modification of [db].[flRsFnGBS] to make it compatible for lookup table purposes
        )

        # Other Lookup tables exported from SQL database that may be of interest for Direct Economic Loss calculations.
        # The basic need DOGAMI had was to establish the building restoration times -
        # and that is fundamental information for all other direct economic loss calculations
        # DOGAMI did not calculate, for example, rental income loss.
        # You can expand the functionality, if you wish,
        # following the methods outlined in the Hazus Flood Technical Manual (2011)
        # xx = "flBldgEconParamWageCapitalIncome.csv"
        # xx = "flBldgEconParamRental.csv"
        # xx = "flBldgEconParamRecaptureFactors.csv"
        # xx = "flBldgEconParamOwnerOccupied.csv"
        # Process some of the user input.
        UDFRoot = os.path.basename(UDFOrig)

        # Resultsfgdb = os.path.join(ResultsDir,y)
        Resultsfgdb = ResultsDir
        # print("Results geodatabase: " + Resultsfgdb)
        # Set up the look-up tables
        BRP = os.path.join(LUT_Dir, BR)
        BCAP = os.path.join(LUT_Dir, BCA)
        BCVP = os.path.join(LUT_Dir, BCV)
        BFP = os.path.join(LUT_Dir, BFull)
        CRP = os.path.join(LUT_Dir, CR)
        CCAP = os.path.join(LUT_Dir, CCA)
        CCVP = os.path.join(LUT_Dir, CCV)
        CFP = os.path.join(LUT_Dir, CFull)
        IRP = os.path.join(LUT_Dir, IR)
        IFP = os.path.join(LUT_Dir, IFull)
        IEP = os.path.join(LUT_Dir, IEconParams)
        Debris = os.path.join(LUT_Dir, DebrisX)
        Rest = os.path.join(LUT_Dir, RestFnc)

        # Process the look-up tables into a list of Dictionary elements
        # Note the standard (default) Lookup Tables were separately developed.
        # Yes, they are a subset of the full lookup table
        bddf_lut_riverine = [row for row in csv.DictReader(open(BRP))]
        bddf_lut_coastalA = [row for row in csv.DictReader(open(BCAP))]
        bddf_lut_coastalV = [row for row in csv.DictReader(open(BCVP))]
        bddf_lut_full = [row for row in csv.DictReader(open(BFP))]

        cddf_lut_riverine = [row for row in csv.DictReader(open(CRP))]
        cddf_lut_coastalA = [row for row in csv.DictReader(open(CCAP))]
        cddf_lut_coastalV = [row for row in csv.DictReader(open(CCVP))]
        cddf_lut_full = [row for row in csv.DictReader(open(CFP))]

        iddf_lut_riverine = [row for row in csv.DictReader(open(IRP))]
        iddf_lut_full = [row for row in csv.DictReader(open(IFP))]
        iecon_lut = [row for row in csv.DictReader(open(IEP))]

        debris_lut = [row for row in csv.DictReader(open(Debris))]
        rest_lut = [row for row in csv.DictReader(open(Rest))]

        # Build up lists to use for checking legitimate user-supplied DDF_ID values
        bddf_lut_full_list = []
        cddf_lut_full_list = []
        iddf_lut_full_list = []
        for x in bddf_lut_full:
            bddf_lut_full_list.append(
                x["BldgDmgFnID"]
            )  # Yes, the capitalization is due to a quirk in the [dbo].[flBldgStructDmgFn].
        for x in cddf_lut_full:
            cddf_lut_full_list.append(
                x["ContDmgFnId"]
            )  # Yes, the case is inconsistent with Building column name. That's the way the Hazus database is.
        for x in iddf_lut_full:
            iddf_lut_full_list.append(x["InvDmgFnId"])

        Content_x_0p5 = [
            "RES1",
            "RES2",
            "RES3A",
            "RES3B",
            "RES3C",
            "RES3D",
            "RES3E",
            "RES3F",
            "RES4",
            "RES5",
            "RES6",
            "COM10",
        ]
        Content_x_1p0 = ["COM1", "COM2", "COM3", "COM4", "COM5", "COM8", "COM9", "IND6", "AGR1", "REL1", "GOV1", "EDU1"]
        Content_x_1p5 = ["COM6", "COM7", "IND1", "IND2", "IND3", "IND4", "IND5", "GOV2", "EDU2"]

        # Default inventory DDF only defined for a subset. IF not in this set, set default Inventory Cost Basis = 0
        Inventory_List = ["COM1", "COM2", "IND1", "IND2", "IND3", "IND4", "IND5", "IND6", "AGR1"]

        # Check for the presence of optional fields (Coastal Flooding, user-supplied DDFs for Building, Content, Inventory)
        #
        CoastalZoneSupplied = ubddf = ucddf = uiddf = cdest = idest = CoastalZoneCode = uccost = uicost = 0

        xt = True if flC != "" else False
        if xt:
            print("Coastal Flooding attribute (flC) supplied. Will use where specified")
            CoastalZoneSupplied = 1
        xt = True if BldgDamageFnID != "" else False
        if xt:
            print(
                "User-supplied Building Depth Damage Function (BldgDamageFnID) attribute supplied. Will use where specified"
            )
            ubddf = 1
        xt = True if ContDamageFnId != "" else False
        if xt:
            print(
                "User-supplied Content  Depth Damage Function attribute (ContDamageFnId supplied. Will use where specified"
            )
            ucddf = 1
        xt = True if InvDamageFnId != "" else False
        if xt:
            print(
                "User-supplied Inventory Depth Damage Function attribute (InvDamageFnId supplied. Will use where specified"
            )
            uiddf = 1
        xt = True if ContentCost != "" else False
        if xt:
            print(
                "User-supplied Content Cost supplied.  Will use user supplied value where specified, else use the default"
            )
            uccost = 1
        xt = True if InvCost != "" else False
        if xt:
            print(
                "User-supplied Inventory Cost supplied.  Will use user supplied value where specified, else use the default"
            )
            uicost = 1

        # logger.info('Custom DDF assignment based on tables...')
        requiredFields = [
            UserDefinedFltyId,
            OccupancyClass,
            Cost,
            Area,
            NumStories,
            FoundationType,
            FirstFloorHt,
            latitude,
            longitude,
        ]

        # Process each depth grid specified by user
        DGrids = DepthGrids  # .split(';')   # Using the interactive window, it's not a list. Make it so.
        for dgp in DGrids:
            # print(" ")    # A formatting step to improve readability of output)
            # print( "Querying depth grid " + dgp)

            # Set up the Results file. Extract grid to points, add needed fields, adjust for First Floor Height.
            # Depth_in_Struc:  The adjusted flood depth
            # flExp:   A simple 1/0 statement: is the UDF in the specified floodplain or is it not?
            # SOID = SpecificOccupId.  A conversion of the OccupancyClass, FoundationType, and NumStories fields into a 4 to 5 character string for lookup.
            # BDDF_ID = the particular Depth Damage Function ID used for that record
            # BldgDmgPct = Loss Ratio for Building
            # BldgLossUSD = Estimated Building Loss in US$  (some fraction of the user-specified Cost)
            #
            # Need to strip out any periods in the depth grid file name, say, "depth100.tif", as periods are COMPLETELY UNACCEPTABLE in fgdb feature class naming
            # And if an input shapefile is specified, drop the *.shp extension. So a Texas Two Step to get a clean name

            y = os.path.split(dgp)[1]
            x = UDFRoot.split(".")[0] + "_" + y.split(".")[0]
            ResultsFile = os.path.join(Resultsfgdb, x)
            # print("Writing results to " + ResultsFile)
            gridroot = y  #  Put into an attribute in the Results file. Redundant, but handy when appending multiple results files together.

            # Some research should go into INTERPOLATE versus NONE in the next function.
            # A cursory peek suggested Hazus-MH Flood does 'NONE' (it had a better match).
            # So to better match to the Hazus-MH Flood results, we (for now) choose 'NONE'.
            # Process each UDF record, calculating its damage based on depth and building type.

            # print("Processing depth grid "+ dgp +  " record by record")

            new_fields = [
                Depth_Grid,
                Depth_in_Struc,
                flExp,
                SOID,
                BDDF_ID,
                BldgDmgPct,
                BldgLossUSD,
                ContentCostUSD,
                CDDF_ID,
                ContDmgPct,
                ContentLossUSD,
                InventoryCostUSD,
                IDDF_ID,
                InvDmgPct,
                InventoryLossUSD,
                DebrisID,
                Debris_Fin,
                Debris_Struc,
                Debris_Found,
                Debris_Tot,
                Restor_Days_Min,
                Restor_Days_Max,
                GridName,
            ]
            field_names = field_names + new_fields
            counter = 0
            counter2 = 0

            recCountNonZeroDepth = 0

            invalidSOID = 0

            # file_out = open(os.path.join(Resultsfgdb,x+".csv"), 'w')

            # CBH - to display the output directory on the final message
            outputDir = os.path.join(Resultsfgdb, x + ".csv")
            file_out = open(outputDir, "w")

            print(dgp)
            _raster_src = rasterio.open(dgp)

            noData = _raster_src.nodata
            cols = _raster_src.width
            rows = _raster_src.height
            _t = _raster_src.transform
            xOrigin = _t.c
            yOrigin = _t.f
            pixelWidth = _t.a
            pixelHeight = -_t.e
            data = _raster_src.read(1)
            print(data)

            IsUTM = (
                _raster_src.crs is not None and _raster_src.crs.is_projected and _raster_src.crs.linear_units == "metre"
            )
            _raster_src.close()
            print("Is it UTM? ", IsUTM)

            """
            inProj = Proj(init='epsg:3857')
            outProj = Proj(init='epsg:4326')
            x1,y1 = xOrigin, yOrigin
            x2,y2 = Transform(inProj,outProj,x1,y1)
            print(x2,y2)
            print('utm conv ', utm.from_latlon(y1, x1))
            ds=raster
            prj=ds.GetProjection()
            print(prj,type(prj))

            srs=osr.SpatialReference(wkt=prj)
            print(srs.GetAttrValue('UNIT'))
            srs.EPSGTreatsAsNorthingEasting()
            print('special ',srs.GetAttrValue('UNIT'),srs.GetAngularUnits(),srs.GetAngularUnitsName(),srs.GetLinearUnits(),srs.GetLinearUnitsName(),srs.GetProjParm('EPSG'),srs.GetUTMZone())
            print(type(srs),srs,help(srs))
            """
            """

            def retrieve_pixel_value(geo_coord, data_source = raster):  
                x, y = geo_coord[0], geo_coord[1]
                forward_transform =  Affine.from_gdal(*data_source.GetGeoTransform())
                reverse_transform = ~forward_transform
                px, py = reverse_transform * (x, y)
                px, py = int(px + 0.5), int(py + 0.5)
                pixel_coord = px, py
                #print(px,py,x,y)
                data_array = np.array(data_source.GetRasterBand(1).ReadAsArray())
                return data_array[pixel_coord[0]][pixel_coord[1]]
            """
            with open(UDFOrig, newline="") as csvfile:
                writer = csv.DictWriter(file_out, delimiter=",", lineterminator="\n", fieldnames=field_names)
                file = _iter_input_rows(UDFOrig) if _is_parquet_input(UDFOrig) else csv.DictReader(csvfile)
                for row in file:
                    counter += 1  # CBH - counter for unmatched SoccIds
                    try:
                        # Check if any required fields are NULL
                        # If NULL do not process the record and do not make an entry in the results file
                        if None in [row[rField] for rField in requiredFields] or "" in [
                            row[rField] for rField in requiredFields
                        ]:
                            setValue(Depth_in_Struc, -99999)
                            writer.writerow(row)  # CBH 08/29/19
                            continue  # CBH - Change added 8/28/19

                        def getValue(name):  # Get value of row from name.
                            if name != Depth_Grid:
                                raw = row.get(name, "")
                                if raw is None:
                                    raw = ""
                                if isinstance(raw, str):
                                    raw = raw.strip()
                                    val = raw if raw != "" else 0
                                else:
                                    val = raw
                                try:
                                    val = float(val)  # CBH
                                except:
                                    pass  # CBH
                                return val
                            else:
                                X = float(getValue(longitude))
                                Y = float(getValue(latitude))
                                # print(utm.from_latlon(Y, X),list(utm.from_latlon(Y, X))[:2])
                                X, Y = list(utm.from_latlon(Y, X)[:2]) if IsUTM else [X, Y]
                                # print('X,Y = ', X,Y)
                                col = int((X - xOrigin) / pixelWidth)
                                roww = int((yOrigin - Y) / pixelHeight)

                                # If incorrect depth grid used the depth is set to 0
                                val = (
                                    data[roww][col]
                                    if abs(col) < abs(cols) and abs(roww) < abs(rows) and data[roww][col] != noData
                                    else 0
                                )
                                # val = retrieve_pixel_value((Y,X))

                                row[name] = val
                                return float(val)

                        def setValue(name, value):  # Set value of attribute from name to given parameter.
                            row[name] = value

                        if counter % 10000 == 0:  # and QC_Warning:
                            print("   processing record " + str(counter))

                        ###################################################
                        # Depth Adjustments
                        ###################################################
                        # Adjust Depth-in-Structure, given the First Floor Height. This will produce the occasional negative value. That is OK
                        # NOTE: Some users suggest that Coastal Flooding should be adjusted by an additional 1.0 foot, because in coastal flooding,
                        # FFH should be considered to be at the freeboard.
                        # However, we confirmed with the Hazus coding team that the Hazus-MH Flood model does NO such adjustment
                        # So for now, we do not do ANY FFH adjustment
                        #
                        # Note this simple calculation varies with the Hazus-MH flood model implementation that rounds FFH to the nearest 0.5 foot level
                        # (which will produce minor differences in the loss ratio calculation).
                        # We maintain the script implements the methods more cleanly. There was no compelling technical reason for
                        # the Hazus-MH flood model to round to the nearest 0.5 foot.
                        rastervalue = getValue(Depth_Grid)
                        FFHeight = float(getValue(FirstFloorHt))
                        depth = (
                            rastervalue - FFHeight if rastervalue is not None else None
                        )  # Must mind the empty (null) case where the UDF has no Raster values
                        userDefinedFltyId = getValue(
                            UserDefinedFltyId
                        )  # Capture it for reporting purposes when encountering records with odd values.

                        # Get some basic information for the record
                        # One could insert some quality checks here and revert to a default if an illegal OccupancyClass, FoundationType, or NumStories
                        # At minimum, clean up the Occupancy Class. This sometimes has trailing spaces, due to Hazus processing quirks.

                        x = getValue(OccupancyClass)
                        OC = x  # .strip()#CBH
                        x = getValue(FoundationType)
                        foundationType = float(
                            x
                        )  # UKS - resolved the issue of RES1 with foundationType = 4 assigned the incorrect SOID due to incorrect datatype
                        numStories = float(getValue(NumStories))
                        area = float(getValue(Area))  # Used in Inventory Loss Calculation
                        if CoastalZoneSupplied:
                            CoastalZoneCode = (
                                flC  # getValue(flC) # Only acquire if a Coastal Zone is defined for that UDF
                            )
                            CoastalZoneCode = "" if CoastalZoneCode is None else CoastalZoneCode  # .strip()

                        # Build up the SpecifOccupId based on OccupancyClass,NumStories,FoundationType:
                        # Prefix, Middle Character, Suffix
                        #
                        # Prefix: Take advantage of the Slice feature in Python. Note the negative sign for right() equivalent
                        # Note that REL1 is the exception in the OccupancyClass list.
                        # QC:  We may want to bark an exception here: check for illegal OccupancyClass? or other combos (e.g. RES2 with more than one story)
                        sopre = OC[:1] + OC[-(len(OC) - 3) :] if OC != "REL1" else "RE1"

                        # Suffix: Easy - Basement or no Basement
                        # UKS - use the value 4 as a number not string
                        sosuf = "B" if foundationType == 4 else "N"

                        # Middle Character: Number of Stories
                        if OC[:4] == "RES3":
                            # RES3 has three categories: 1 3 5
                            somid = "5" if numStories > 4 else "3" if numStories > 2 else "1"

                        elif OC[:4] == "RES1":
                            # If NumStories is not an integer, assume Split Level residence
                            # Also, cap it at 3.
                            numStories = 3 if numStories > 3.0 else numStories
                            somid = str(round(numStories)) if numStories - round(numStories) == 0 else "S"

                        elif OC[:4] == "RES2":
                            # Manuf. Housing is by definition limited to one story
                            somid = "1"

                        else:
                            # All other cases: Easy!  1-3, 4-7, 8+
                            somid = "H" if numStories > 6 else "M" if numStories > 3 else "L"
                        SpecificOccupId = sopre + somid + sosuf
                        # if OC[:4] == 'RES1' and foundationType == 4:
                        #    logger.info('foundation type = ' + str(foundationType) + ' and SOID = ' + SpecificOccupId)
                        # logger.info(SpecificOccupId)
                        setValue(SOID, SpecificOccupId)

                        # Content and Inventory Cost. Determine each, even if structure not exposed to flooding
                        # Content Loss in US$: depends if user supplied a content cost field, and if it is > 0.
                        # If not, then use a default multiplier, depending on OccupancyClass, per Hazus-MH Flood Technical Manual table
                        CMult = (
                            0.5
                            if OC in Content_x_0p5
                            else 1.0
                            if OC in Content_x_1p0
                            else 1.5
                            if OC in Content_x_1p5
                            else 0
                        )
                        if uccost:
                            xt = int(float(getValue(ContentCost)))
                            xt = (
                                -1 if xt is None else xt
                            )  # Null value check. If ContentCost is NULL,use the default value
                        else:
                            xt = -1
                        ccost = int(getValue(Cost)) * CMult if xt == -1 else xt

                        # Inventory Cost
                        OWDI = OC in Inventory_List
                        xt = getValue(InvCost) if uicost else -1
                        xt = xt if xt is not None else -1  #  Clean up case where InvCost is supplied but is null
                        if OWDI and xt == -1:
                            # Use default cost formula
                            for lutrow in iecon_lut:
                                if lutrow["Occupancy"] == OC:
                                    GrossSales = lutrow["AnnualSalesPerSqFt"]
                                    BusinessInv = lutrow["BusinessInvPctofSales"]
                                    # Table imports as string type (?!) so we must convert tabular data to a float type
                                    # Yes, raw data is typically in Integer format, be flexible for future data which may be available in dollars.cents
                                    # Must divide by 100, as BusinessInv in the input table is a Percent figure
                                    # Area is in Square Feet
                                    icost = float(GrossSales) * float(BusinessInv) * area / 100
                                    break
                        # If a user-supplied Inventory Cost is supplied, use it.
                        elif xt > -1:
                            icost = getValue(InvCost)
                        else:
                            icost = 0

                        setValue(ContentCostUSD, ccost)
                        setValue(InventoryCostUSD, icost)

                        # UKS - Negative Depth_in_Struc is OK but Negative depth from raster is not -Modified the logic to check against
                        # rastervalue

                        # depth measured for that point, set some default values for the output to clearly indicate that there is No Exposure
                        # and quickly move on to the next record

                        # UKS - modified the usage to depth (which is Depth_in_Struc) to rastervalue
                        if (
                            rastervalue is None or rastervalue <= 0
                        ):  # Depending on Depth Grid format, the Extract_2_Point returns Null or -9999
                            setValue(flExp, 0)  # Structure is NOT exposed.
                            # setValue(Depth_in_Struc,None)    # Default value, again emphasizing the point is not exposed. Make SummaryStats more straightforward

                            # UKS - Recorded even though negative
                            if rastervalue is None:
                                setValue(Depth_in_Struc, 0)
                            else:
                                setValue(Depth_in_Struc, depth)

                            setValue(BDDF_ID, 0)
                            setValue(BldgDmgPct, 0)
                            setValue(BldgLossUSD, 0)
                            setValue(CDDF_ID, 0)
                            setValue(ContDmgPct, 0)
                            setValue(ContentLossUSD, 0)
                            setValue(IDDF_ID, 0)
                            setValue(InvDmgPct, 0)
                            setValue(InventoryLossUSD, 0)
                            setValue(DebrisID, "")
                            setValue(
                                Debris_Fin, None
                            )  # Partition the Debris into its three components - Table 11.1 Hazus-MH Flood Technical Manual
                            setValue(Debris_Struc, None)
                            setValue(Debris_Found, None)
                            setValue(Debris_Tot, None)
                            setValue(Restor_Days_Min, 0)
                            setValue(Restor_Days_Max, 0)

                            # UKS
                            recCountNonZeroDepth -= 1
                        else:
                            # The UDF is exposed. Calculate Building, Content, Inventory Losses
                            setValue(flExp, 1)
                            setValue(Depth_in_Struc, depth)  # Record the depth in structure.
                            # (To be considered: optional freeboard adjustment for Coastal Flooding)
                            # Hazus 4.0 model does *no* adjustment. Some have suggested that one should add a freeboard margin; e.g., adjust FFH by -1 foot.
                            # But there is no clear consensus on such a conservative adjustment.

                            # If depth is over 24 feet or less than -4 feet, then adjust depth. LUTs do not extend beyond that range!
                            # Note that Hazus-MH flood model caps the grid raster at 24 feet, then does the subtraction. This creates some differences in results.
                            # We believe that you do the FFH subtraction before capping the depth at 24 feet.
                            depth = 24 if depth > 24 else depth
                            depth = -4 if depth < -4 else depth

                            # Get some basic information for the record
                            # One could insert some quality checks here andrevert to a default if an illegal OccupancyClass, FoundationType, or NumStories
                            # At minimum, clean up the Occupancy Class. This sometimes has trailing spaces, due to Hazus processing quirks.
                            x = getValue(OccupancyClass)
                            OC = x
                            x = str(getValue(FoundationType))
                            foundationType = float(
                                x
                            )  # UKS - resolved the issue of RES1 with foundationType = 4 assigned the incorrect SOID due to incorrect datatype
                            numStories = float(getValue(NumStories))
                            area = float(getValue(Area))  # Used in Inventory Loss Calculation

                            # Construct the strings for the LUT reference: if depth <0, use 'm'. If >0, use 'p'
                            # See the Column headings in the csv lookup tables.
                            # Need to strip out the minus sign using abs() and the decimal point using int(), and convert it to a string using str()
                            suffix_l = str(int(abs(math.floor(depth))))
                            suffix_u = str(int(abs(math.ceil(depth))))
                            prefix_l = "m" if math.floor(depth) < 0 else "p"
                            prefix_u = (
                                "m" if math.ceil(depth) < 0 else "p"
                            )  # Need to fuss over the boundary case  -1 < depth < 0
                            l_index = prefix_l + suffix_l
                            u_index = prefix_u + suffix_u

                            ###########################################################
                            # BUILDING LOSS CALCULATION
                            ###########################################################
                            # Did user specify a Building DDF? If so, use that to reference the Full LUT, else use the Default LUT.
                            # Due to Hazus-MH Flood definitions, this is Text type.
                            BID = getValue(BldgDamageFnID) if ubddf else None

                            # If BID is specified by the user, and defined, then assume they know what is best, and use the full lookup table.
                            # Tests are ok if you go left-to-right. Go from most-basic-test-to-more-advanced in the same line.  Can't flip the order here!
                            if BID is not None and BID != "" and BID in bddf_lut_full_list:
                                # Search the  full lookup table to find the DDF_ID that matches the BID
                                # 'gotcha' checks for no hits - set a check bit - that should not happen, given the membership test with bddf_lut_full_list.
                                # For more efficiency, break out of the loop if it is found
                                gotcha = 0
                                for lutrow in bddf_lut_full:
                                    if (
                                        lutrow["BldgDmgFnID"] == BID
                                    ):  # This is a string match. For completeness and trailing spaces, may want to make it an integer?
                                        gotcha += 1
                                        ddf1 = lutrow
                                        # Notify user if the OccupancyClass associated with the user-specified DDFID is inconsistent with the user-supplied OccupancyClass
                                        # This is not harmful; DOGAMI script has chosen to just process it (Hazus silently reverts back to the default!)
                                        # Simple notification
                                        OccClsCheck = ddf1["Occupancy"]
                                        if OccClsCheck != OC and QC_Warning:
                                            print(
                                                "FYI: User-supplied Building DDFID "
                                                + BID
                                                + " Occupancy Class is inconsistent with UDF Occupancy Class "
                                                + OC
                                                + " versus "
                                                + OccClsCheck
                                                + "  "
                                                + userDefinedFltyId
                                            )
                                        break
                                d_lower = float(ddf1[l_index])
                                d_upper = float(ddf1[u_index])
                                ddf_id = int(
                                    BID
                                )  # Yes, it is redundant to post, again, what the user specified. But it is consistent with Default LUT

                            else:
                                # We may have gotten here because of a bad BDDF code. If so, revert to the default and notify user
                                # Note we are in the Default DDF section, and will calculate loss in that manner.
                                if QC_Warning and BID is not None and BID != "" and int(BID) > 0:
                                    print(
                                        "User specified a non-official Building DDFID: "
                                        + BID
                                        + "    UID: "
                                        + userDefinedFltyId
                                    )
                                    print("   Reverting to default Building DDF for Occupancy Class " + OC)

                                # Go through the lookup table, one row at a time to find the Structure of interest
                                # 'gotcha' checks for no hits - set a check bit
                                # Also, for more efficiency, break out of the loop if it is found
                                gotcha = 0

                                # Change DDF table only if Coastal Zone is defined (CoastalZoneSuppled) AND a legitimate Coastal Zone Code (AE, V, VE)
                                # Otherwise use default ddf.
                                # As of Hazus 4.0, Coastal lookup tables are only applicable for RES-type structures.
                                blut = bddf_lut_riverine
                                if CoastalZoneSupplied and OC[:3] == "RES":
                                    if CoastalZoneCode == "CAE":
                                        blut = bddf_lut_coastalA
                                    if CoastalZoneCode == "VE" or CoastalZoneCode == "V":
                                        blut = bddf_lut_coastalV

                                # Now do the lookup in the Default DDF
                                for lutrow in blut:
                                    if lutrow["SpecificOccupId"] == SpecificOccupId:
                                        gotcha += 1
                                        ddf1 = lutrow
                                        ddf_id = lutrow["DDF_ID"]  # For the Record. Will go in the Results file.
                                        break  # Quit once you found it.
                                if gotcha == 0:
                                    # This should not occur
                                    print(
                                        "something wrong, no match for Specific Occupancy ID :"
                                        + SpecificOccupId
                                        + "   UDF: "
                                        + UserDefinedFltyId
                                    )
                                    invalidSOID += 1
                                    setValue(SOID, SpecificOccupId)  # CBH
                                    setValue(BDDF_ID, "Unmatched")  # CBH
                                    writer.writerow(row)  # CBH
                                    logger.info(
                                        "Unmatched SOID: "
                                        + SpecificOccupId
                                        + " with userDefinedFltyId: "
                                        + str(userDefinedFltyId)
                                    )  # CBH
                                    continue
                                    # sys.exit(2)

                            # Dictionary lookup: get damage percentage for the particular row at the particular depths
                            # The Dictionary element comes from either the Full or the Default table; common code after this point.
                            d_lower = float(ddf1[l_index])
                            d_upper = float(ddf1[u_index])
                            # Get fractional amount of depth, for interpolation
                            frac = depth - math.floor(depth)
                            damage = (d_lower + frac * (d_upper - d_lower)) / 100

                            if gotcha == 0:
                                # This should not occur, given the memebership test with bddf_lut_full_list. Just in case:
                                print(
                                    "Problem: nothing matches the SpecificOccupId of "
                                    + SpecificOccupId
                                    + "     Check entry UDFID "
                                    + userDefinedFltyId
                                    + " with "
                                    + OC
                                )
                                SpecificOccupId = "XXXX"
                                BDDF_ID = LR = bldg_loss = -9999

                            # Calculate building loss, set other attributes
                            setValue(SOID, SpecificOccupId)
                            setValue(BDDF_ID, ddf_id)
                            setValue(BldgDmgPct, damage * 100)  # Hazus convention: percentage
                            bldg_loss = damage * int(getValue(Cost))
                            setValue(BldgLossUSD, bldg_loss)

                            ###########################################################
                            # CONTENT LOSS CALCULATION
                            ###########################################################
                            # Did user specify a Content DDF? If so, use that to reference the Full LUT, else use the Default LUT.
                            # Due to Hazus-MH Flood conventions, the CDDF_ID is of type Text
                            BID = getValue(ContDamageFnId) if ucddf else None

                            # If BID is specified by the user, then assume they know what is best, and use the full lookup table.
                            # Tests are ok if you go left-to-right. Go from most-basic-test-to-more-advanced in the same line.  Can't flip the order here!
                            if BID is not None and BID != "" and BID in cddf_lut_full_list:
                                # Search the  full lookup table to find the DDF_ID that matches the BID
                                # 'gotcha' checks for no hits - set a check bit - that should not happen, given the membership test with bddf_lut_full_list.
                                # For more efficiency, break out of the loop if it is found
                                gotcha = 0
                                for lutrow in cddf_lut_full:
                                    if (
                                        lutrow["ContDmgFnId"] == BID
                                    ):  # This is a string match. For completeness and trailing spaces, may want to make it an integer?
                                        gotcha += 1
                                        ddf1 = lutrow
                                        # Notify user if the OccupancyClass associated with the user-specified DDFID is inconsistent with the user-supplied OccupancyClass
                                        # This is not harmful; DOGAMI script has chosen to just process it (Hazus silently reverts back to the default!)
                                        # Simple notification
                                        OccClsCheck = ddf1["Occupancy"]
                                        if OccClsCheck != OC and QC_Warning:
                                            print(
                                                "FYI: User-supplied Content  DDFID "
                                                + BID
                                                + " Occupancy Class is inconsistent with UDF Occupancy Class "
                                                + OC
                                                + " versus "
                                                + OccClsCheck
                                                + "  "
                                                + userDefinedFltyId
                                            )
                                        break
                                d_lower = float(ddf1[l_index])
                                d_upper = float(ddf1[u_index])
                                ddf_id = int(
                                    BID
                                )  # Yes, it is redundant to post, again, what the user specified. But it is consistent with Default LUT

                            else:
                                # We may have gotten here because of a bad CDDF code. If so, revert to the default and notify user
                                # Note we are in the Default DDF section, and will calculate loss in that manner.
                                if QC_Warning and BID is not None and BID != "" and int(BID) > 0:
                                    print(
                                        "FYI: User specified a non-official Content DDFID: "
                                        + BID
                                        + "    UID: "
                                        + userDefinedFltyId
                                        + "   Reverting to default Content DDF for Occupancy Class "
                                        + OC
                                    )

                                # Go through the lookup table, one row at a time to find the Structure of interest
                                # 'gotcha' checks for no hits - set a check bit
                                # Also, for more efficiency, break out of the loop if it is found
                                gotcha = 0

                                # Change DDF table if Coastal; otherwise use default ddf.
                                # As of Hazus 4.0, Coastal lookuptables only applicable for RES-type structures.
                                # Need to filter out "REL" from "RES" - look at second letter
                                clut = cddf_lut_riverine
                                if CoastalZoneSupplied and OC[:3] == "RES":
                                    if CoastalZoneCode == "CAE":
                                        clut = cddf_lut_coastalA
                                    if CoastalZoneCode == "VE" or CoastalZoneCode == "V":
                                        clut = cddf_lut_coastalV

                                for lutrow in clut:
                                    if lutrow["SpecificOccupId"] == SpecificOccupId:
                                        gotcha += 1
                                        ddf1 = lutrow
                                        ddf_id = lutrow["DDF_ID"]  # For the Record. Will go in the Results file.
                                        break  # Quit once you found it.
                                if gotcha == 0:
                                    # This should not occur
                                    print(
                                        "something wrong for Content lookup, no match for Specific Occupancy ID :"
                                        + SpecificOccupId
                                        + "   Counter:"
                                        + str(counter)
                                    )

                            # Dictionary lookup: get damage percentage for the particular row at the particular depths
                            # The Dictionary element comes from either the Full or the Default table; common code after this point.
                            d_lower = float(ddf1[l_index])
                            d_upper = float(ddf1[u_index])
                            # Get fractional amount of depth, for interpolation
                            frac = depth - math.floor(depth)
                            damage = (d_lower + frac * (d_upper - d_lower)) / 100

                            if gotcha == 0:
                                # Should not occur, given the check for membership in the list. But here just in case
                                print(
                                    "Problem with Content Loss: nothing matches the SpecificOccupId of "
                                    + SpecificOccupId
                                    + "Check entry "
                                    + str(counter)
                                    + " with "
                                    + OC
                                    + " "
                                    + str(numStories)
                                )
                                SpecificOccupId = "XXXX"
                                CDDF_ID = LR = bldg_loss = -9999

                            setValue(CDDF_ID, ddf_id)
                            setValue(ContDmgPct, damage * 100)  # Hazus convention: percenage
                            content_loss = damage * ccost
                            setValue(ContentLossUSD, content_loss)

                            ###########################################################
                            # INVENTORY LOSS CALCULATION
                            ###########################################################
                            # Did user specify an Inventory DDF? If so, use that to reference the Full LUT, else use the Default LUT.
                            # Due to Hazus-MH Flood conventions, the IDDF_ID is of type Text
                            BID = getValue(InvDamageFnId) if uiddf else None
                            # If BID is specified by the user, then assume they know what is best, and use the full lookup table.
                            # Tests are ok if you go left-to-right. Go from most-basic-test-to-more-advanced in the same line.  Can't flip the order here!
                            if BID is not None and BID != "" and BID in iddf_lut_full_list:
                                # Search the  full lookup table to find the DDF_ID that matches the BID
                                # 'gotcha' checks for no hits - set a check bit - that should not happen, given the membership test with bddf_lut_full_list.
                                # For more efficiency, break out of the loop if it is found
                                gotcha = 0
                                for lutrow in iddf_lut_full:
                                    if (
                                        lutrow["InvDmgFnId"] == BID
                                    ):  # This is a string match. For completeness and trailing spaces, may want to make it an integer?
                                        gotcha += 1
                                        ddf1 = lutrow
                                        # Notify user if the OccupancyClass associated with the user-specified DDFID is inconsistent with the user-supplied OccupancyClass
                                        # This is not harmful; DOGAMI script has chosen to just process it (Hazus silently reverts back to the default!)
                                        # Simple notification
                                        OccClsCheck = ddf1["Occupancy"]
                                        if OccClsCheck != OC and QC_Warning:
                                            print(
                                                "FYI: User-supplied Inventory DDFID "
                                                + BID
                                                + " Occupancy Class is inconsistent with UDF Occupancy Class "
                                                + OC
                                                + " versus "
                                                + OccClsCheck
                                                + "  "
                                                + userDefinedFltyId
                                            )
                                        break
                                d_lower = float(ddf1[l_index])
                                d_upper = float(ddf1[u_index])
                                frac = depth - math.floor(depth)
                                damage = (d_lower + frac * (d_upper - d_lower)) / 100
                                ddf_id = int(
                                    BID
                                )  # Yes, it is redundant to post, again, what the user specified. But it is consistent with Default LUT

                            else:
                                # We may have gotten here because of a bad IDDF code. If so, revert to the default and notify user
                                # Note we are in the Default DDF section, and will calculate loss in that manner.
                                if QC_Warning and BID is not None and BID != "" and int(BID) > 0:
                                    print(
                                        "User specified a non-official Inventory DDFID: "
                                        + BID
                                        + "    UID: "
                                        + userDefinedFltyId
                                        + "   Reverting to default Inventory DDF for Occupancy Class "
                                        + OC
                                    )

                                # Go through the lookup table, one row at a time to find the Structure of interest
                                # 'gotcha' checks for no hits - set a check bit
                                # Also, for more efficiency, break out of the loop if it is found
                                gotcha = 0

                                # Inventory: There is no Coastal Flooding default table to use
                                ilut = iddf_lut_riverine

                                # Default Inventory DDF defined only for a subset of OccupancyClass types
                                if OC in Inventory_List:
                                    for lutrow in ilut:
                                        if lutrow["SpecificOccupId"] == SpecificOccupId:
                                            gotcha += 1
                                            ddf1 = lutrow
                                            ddf_id = lutrow["DDF_ID"]  # For the Record. Will go in the Results file.
                                            break  # Quit once you found it.
                                    if gotcha == 0:
                                        # This should not occur
                                        print(
                                            "something wrong for Inventory lookup, no match for Specific Occupancy ID :"
                                            + SpecificOccupId
                                            + "   Counter:"
                                            + str(counter)
                                        )
                                        sys.exit(2)

                                    # Dictionary lookup: get damage percentage for the particular row at the particular depths
                                    # The Dictionary element comes from either the Full or the Default table; common code after this point.
                                    d_lower = float(ddf1[l_index])
                                    d_upper = float(ddf1[u_index])
                                    # Get fractional amount of depth, for interpolation
                                    frac = depth - math.floor(depth)
                                    damage = (d_lower + frac * (d_upper - d_lower)) / 100

                                    if gotcha == 0:
                                        # Should not occur, given the check for membership in the list. But here just in case
                                        print(
                                            "Problem with Inventory Loss: nothing matches the SpecificOccupId of "
                                            + SpecificOccupId
                                            + "Check entry "
                                            + str(counter)
                                            + " with "
                                            + OC
                                            + " "
                                            + str(numStories)
                                        )
                                        SpecificOccupId = "XXXX"
                                        IDDF_ID = LR = bldg_loss = -9999

                                else:
                                    # No default DDF ID exists for the given OccupancyClass. Fill them in with zeros
                                    damage = 0
                                    ddf_id = 0

                            setValue(IDDF_ID, ddf_id)
                            setValue(InvDmgPct, damage * 100)  # Hazus convention - percentage

                            # Inventory Loss in US$: depends if user supplied an inventory cost field, and if it is > 0.
                            # If not supplied, or 0, then use the default value based on OccupancyClass and Square Footage
                            # per Hazus-MH Flood Technical Manual table
                            # But note that the 'default value' is defined only for a subset of OccupancyClasses
                            # Logic spelled out in accompanying spreadsheet - to simplify it, create three variables
                            # OWDI = OccupancyClass with Default Inventory
                            # USID = User-supplied Inventory DDF is supplied and legitimate
                            # USIC = User-supplied Inventory Cost is supplied and non-zero and non-null

                            inventory_loss = damage * icost
                            setValue(InventoryLossUSD, inventory_loss)

                            ###########################################################
                            # DEBRIS CALCULATIONS
                            ###########################################################
                            # Calculate only for exposed buildings
                            if depth is not None and depth > 0:  # CBH - added > 0, 8/28/19
                                # Build up a DebrisID key for accessing Debris LUT table
                                # Basement/No Basement only defined for RES1.
                                # Slab/Footing: Simple mapping of FoundationType (includes Basement by definition)
                                # dsuf = depth suffix
                                bsm = "NB"  # No Basement is the default. Only override for RES1
                                # UKS - use the value 4 and 7 as a number not string
                                fnd = (
                                    "SG" if (foundationType == 4 or foundationType == 7) else "FT"
                                )  # SG: Slab on Grade.  FT = ???? DEFINE THIS - FROM BBOHN.
                                # Flood depth key varies, depending if it's a RES1/Basement.
                                if (OC == "RES1" or OC == "COM6") and foundationType == 4:
                                    # UKS - Special case handled for RES1 with FT SG
                                    # COM6 is always NB
                                    if OC == "RES1":
                                        bsm = "B"
                                    dsuf = (
                                        "-8"
                                        if depth < -4
                                        else "-4"
                                        if depth < 0
                                        else "0"
                                        if depth < 4
                                        else "4"
                                        if depth < 6
                                        else "6"
                                        if depth < 8
                                        else "8"
                                    )
                                else:  # Credit to BBohn who identified 0/1/4/8/12 as common breakpoints shared by all non-RES1-Basement
                                    dsuf = (
                                        "0"
                                        if depth < 1
                                        else "1"
                                        if depth < 4
                                        else "4"
                                        if depth < 8
                                        else "8"
                                        if depth < 12
                                        else "12"
                                    )

                                # RES2 - if depth in structure is negative (<0) - no losses should be produced
                                # RES2 - if depth in structure greater than 0 but less than 1 finishes losses will be produced
                                if OC == "RES2" and depth < 0:
                                    dsuf = ""

                                debriskey = OC + bsm + fnd + dsuf
                                for lutrow in debris_lut:
                                    if (
                                        lutrow["DebrisID"] == debriskey
                                    ):  # This is a string match. For completeness and trailing spaces, may want to make it an integer?
                                        gotcha += 1
                                        ddf1 = lutrow

                                dfin_rate = float(ddf1["Finishes"])
                                dstruc_rate = float(ddf1["Structure"])
                                dfound_rate = float(ddf1["Foundation"])
                                # All LUT numbers are in tons per 1000 square feet, so adjust for your particular structure
                                dfin = area * dfin_rate / 1000
                                dstruc = area * dstruc_rate / 1000
                                dfound = area * dfound_rate / 1000
                                dtot = dfin + dstruc + dfound
                            else:
                                dfin = dstruc = dfound = dtot = debriskey = None

                            setValue(DebrisID, debriskey)
                            setValue(Debris_Fin, dfin)
                            setValue(Debris_Struc, dstruc)
                            setValue(Debris_Found, dfound)
                            setValue(Debris_Tot, dtot)

                            ###########################################################
                            # Restoration Time Calculation - the basis for all Direct Economic Loss numbers
                            # Based on the Min and Max days listed in   [dbo].[flRsFnGBS]
                            # Note how the table differs slightly from the TM, esp with Res with basements
                            # Note that the TM suggests some of these are not subject to a 10% threshold
                            # The method suggests using the Maximum; for completeness, the script produces both.
                            ###########################################################
                            # Calculate only for exposed buildings.
                            if depth is not None and depth > 0:  # CBH - added > 0, 8/28/19
                                # Build up a key for accessing the Restoration Time LUT table
                                dsuf = (
                                    "0"
                                    if depth < 0
                                    else "1"
                                    if depth < 1
                                    else "4"
                                    if depth < 4
                                    else "8"
                                    if depth < 8
                                    else "12"
                                    if depth < 12
                                    else "24"
                                )
                                RsFnkey = OC + dsuf
                                for lutrow in rest_lut:
                                    if (
                                        lutrow["RestFnID"] == RsFnkey
                                    ):  # This is a string match. For completeness and trailing spaces, may want to make it an integer?
                                        ddf1 = lutrow
                                        break
                                restdays_min = int(
                                    ddf1["Min_Restor_Days"]
                                )  # This is the maximum days out (flRsFnGBS has a min and a max)
                                restdays_max = int(
                                    ddf1["Max_Restor_Days"]
                                )  # This is the maximum days out (flRsFnGBS has a min and a max)
                            else:
                                restdays_min = restdays_max = 0  # Or should it be None type?
                            setValue(Restor_Days_Min, restdays_min)
                            setValue(Restor_Days_Max, restdays_max)

                        # When running multiple grids, sensitivity tests, etc, adding the gridname makes it easier to sort upon an appended dataset

                        setValue(GridName, gridroot)
                        # counter += 1 #CBH
                        # counter2 += 1 #CBH

                        recCountNonZeroDepth += 1
                        if counter == 1:
                            writer.writeheader()
                            writer.writerow(row)
                            continue
                        writer.writerow(row)
                    except Exception as e:
                        print(e)
                        print(f"FAST row error: {e}", file=sys.stderr)
                        logger.info("Expection occured")
                        writer.writerow(row)
                        row_error_count += 1
                        counter2 += 1
                        continue

            file_out.close()
            # UKS - Sorting and logging
            logger.info("Loss calculations complete for the selected grid...")
            logger.info("Sorting reults by Depth in structure...")
            del data
            csv_input = csv.DictReader(open(outputDir, "r", newline=""))  # csv.DictReader(f_input)
            data = sorted(
                csv.DictReader(open(outputDir, "r", newline="")),
                key=lambda row: (abs(float(row["Depth_in_Struc"])) < 0, float(row["Depth_in_Struc"])),
                reverse=True,
            )
            # f_input.close()

            # os.unlink(ResultsFile + '.csv')
            logger.info("Results saved into " + ResultsFile + ".csv")
            with open(ResultsFile + "_sorted.csv", "w", newline="") as f_output:
                csv_output = csv.DictWriter(f_output, fieldnames=csv_input.fieldnames)
                csv_output.writeheader()
                csv_output.writerows(data)
            f_output.close()

            # logger.info('Total records processed: ' + str(counter) + ' of ' + str(counter2) + ' records total.' + 'Total records with flooding: ' + str(recCountNonZeroDepth))

            # CBH
            # UKS - modified for complete file name on the final message box
            log.append(
                [counter, counter2, recCountNonZeroDepth, invalidSOID, os.path.basename(dgp), ResultsFile + ".csv"]
            )

            # recCountNonZeroDepth counter logged, concatenated to the message and reset
        message = ""
        for grid in log:  # CBH
            message += (
                "For depth-grid: "
                + str(grid[4])
                + "\n"
                + str(grid[0])
                + " records processed of "
                + str(grid[0])
                + " records total.\n"
                + "Total records with flooding: "
                + str(grid[2])
                + "\n"
                + "Total number of records with unmatched Specific Occupancy IDs found: "
                + str(grid[3])
                + "\n File saved to: "
                + os.path.realpath(os.path.join(os.path.dirname(outputDir), str(grid[5])))
                + "\n\n"
            )  # UKS - modified for complete file name #CBH - change added 8/28/19

            recCountNonZeroDepth = 0

        # return(True, [counter,counter2,recCountNonZeroDepth,invalidSOID]) #UKS Commented
        return (True, message, row_error_count)

        # Measuring the script performance
    except Exception as e:
        logger.info(e)
        print(e)
        print(f"FAST fatal error: {e}", file=sys.stderr)
        return (False, counter, 0)


# This test allows the script to be used from the operating
# system command prompt (stand-alone), in a Python IDE,
# as a geoprocessing script tool, or as a module imported in
# another script


def _coerce_field_map(field_map):
    if isinstance(field_map, dict):
        return [field_map.get(key, "") for key in DEFAULT_FIELD_MAP_KEYS]
    if isinstance(field_map, (list, tuple)):
        return list(field_map)
    raise TypeError("field_map must be a dict, list, or tuple.")


def _normalize_field_map_values(field_map):
    normalized = []
    for value in field_map:
        normalized.append("" if value is None else str(value).strip())
    return normalized


def local_with_options(
    inventory_path,
    field_map,
    raster_names_or_paths,
    flood_type,
    output_dir=None,
    project_root=None,
    log_path=None,
    qc_warning="False",
):
    if not inventory_path:
        raise ValueError("inventory_path is required.")

    project_root = _resolve_project_root(project_root)
    inventory_path = os.path.abspath(inventory_path)
    output_dir = os.path.abspath(output_dir or os.path.dirname(inventory_path) or project_root)
    os.makedirs(output_dir, exist_ok=True)

    lookup_tables_dir = _resolve_lookup_tables_dir(project_root)
    raster_paths = _resolve_raster_paths(raster_names_or_paths, project_root)
    normalized_field_map = _normalize_field_map_values(_coerce_field_map(field_map))

    if len(normalized_field_map) != len(DEFAULT_FIELD_MAP_KEYS):
        raise ValueError(
            "field_map must contain exactly {count} values for keys: {keys}".format(
                count=len(DEFAULT_FIELD_MAP_KEYS), keys=", ".join(DEFAULT_FIELD_MAP_KEYS)
            )
        )

    _configure_logger(log_path=log_path, project_root=project_root)
    full_map = normalized_field_map + [_normalize_flood_type(flood_type)]
    argv = (inventory_path, lookup_tables_dir, output_dir, raster_paths, str(qc_warning), full_map)
    return flood_damage(*argv)


def local(spreadsheet, fmap, project_root=None, log_path=None):
    if not isinstance(fmap, (list, tuple)) or len(fmap) < 2:
        raise ValueError("fmap must include mapped fields and selected raster(s).")

    raster = fmap[-1]
    mapped_values = list(fmap[:-1])
    expected = len(DEFAULT_FIELD_MAP_KEYS) + 1
    if len(mapped_values) != expected:
        raise ValueError("fmap must include {count} mapped values before rasters.".format(count=expected))

    field_map = mapped_values[:-1]
    flood_type = mapped_values[-1]
    return local_with_options(
        inventory_path=spreadsheet,
        field_map=field_map,
        raster_names_or_paths=raster,
        flood_type=flood_type,
        output_dir=os.path.dirname(os.path.abspath(spreadsheet)),
        project_root=project_root,
        log_path=log_path,
        qc_warning="False",
    )
