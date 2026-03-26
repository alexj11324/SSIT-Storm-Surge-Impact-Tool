"""Read in NHC raster files for estimated storm surge and identify relevant states."""

import io
import re
import zipfile
from typing import Dict, List, Optional

import geopandas as gpd
import requests
from pygris import states
from rasterio.io import MemoryFile
from requests.adapters import HTTPAdapter
from shapely.geometry import box
from urllib3.util.retry import Retry


def _normalize_storm_id(storm_id: str, year: int) -> str:
    """Ensure storm_id follows basin + two-digit number + four-digit year, using provided year if missing."""
    storm_id = storm_id.upper()
    match = re.match(r"(?P<basin>[A-Z]{2})(?P<number>\d{1,2})(?P<year>\d{2,4})?$", storm_id)
    if not match:
        return f"{storm_id}{year}"

    basin = match.group("basin")
    number = match.group("number").zfill(2)
    provided_year = match.group("year")

    if provided_year:
        normalized_year = provided_year if len(provided_year) == 4 else f"20{provided_year}"
    else:
        normalized_year = str(year)

    return f"{basin}{number}{normalized_year}"


def _build_session(retries: int = 3, backoff: float = 0.5) -> requests.Session:
    retry = Retry(
        total=retries,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=backoff,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def import_surge_data(
    storm_id: str,
    storm_name: str,
    adv: int,
    year: int,
    *,
    timeout: int = 30,
    retries: int = 3,
    session: Optional[requests.Session] = None,
) -> Dict[str, object]:
    """
    Reads estimated storm surge TIFF files from NHC website for a given storm and advisory.

    Args:
        storm_id (str): The identifier associated with the storm (e.g. AL022024).
        storm_name (str): The name associated with the storm (e.g. Beryl).
        adv (int): The number of the latest advisory for the storm (e.g. 29).
        year (int): The year of the storm (e.g. 2024)
        timeout (int): Timeout in seconds for the download request.
        retries (int): Number of retry attempts for the download request.
        session (requests.Session | None): Optional session to reuse or mock for testing.

    Returns:
        dictionary: (1) The storm surge heights data from the raster file and (2) a list of states captured in the raster data
    """
    normalized_storm_id = _normalize_storm_id(storm_id, year)
    storm_name = storm_name.upper()
    adv_padded = f"{int(adv):03d}"

    url = f"https://www.nhc.noaa.gov/gis/inundation/forecasts/{normalized_storm_id}_{adv_padded}_tidalmask.zip"
    tif_filename_in_zip = f"{storm_name}_{year}_adv{int(adv)}_e10_ResultMaskRaster.tif"
    download_session = session or _build_session(retries=retries)

    print(f"Downloading {url} ...")
    response = download_session.get(url, stream=True, timeout=timeout)
    response.raise_for_status()

    zip_in_memory = io.BytesIO(response.content)

    with zipfile.ZipFile(zip_in_memory, "r") as z:
        if tif_filename_in_zip not in z.namelist():
            raise FileNotFoundError(
                f"{tif_filename_in_zip} not found in archive (available: {z.namelist()})"
            )

        print(f"Reading {tif_filename_in_zip} from archive...")
        with z.open(tif_filename_in_zip) as tif_file:
            tif_bytes = tif_file.read()

    surge_data = MemoryFile(tif_bytes).open()

    surge_bounds = surge_data.bounds
    surge_polygon = box(
        surge_bounds.left, surge_bounds.bottom, surge_bounds.right, surge_bounds.top
    )
    surge_extent_gdf = gpd.GeoDataFrame({"id": 1, "geometry": [surge_polygon]}, crs=surge_data.crs)

    us_states = states(cb=True, cache=True, year=year)
    us_states = us_states.to_crs(surge_data.crs)

    overlapping_states = gpd.sjoin(us_states, surge_extent_gdf, how="inner", predicate="intersects")

    state_names: List[str] = []
    if not overlapping_states.empty:
        state_names = overlapping_states["NAME"].unique().tolist()
    else:
        print("States not found")

    return {"data": surge_data, "states": state_names}


if __name__ == "__main__":
    ## - user inputs
    storm_name = "BERYL"
    storm_id = "AL022024"
    advisory_no = 29
    year = 2024

    ## - get storm surge data and relevant states
    surge_dict = import_surge_data(
        storm_id=storm_id, storm_name=storm_name, adv=advisory_no, year=year
    )
    surge_data = surge_dict["data"]
    surge_states = surge_dict["states"]
    print(f"States in the storm surge data for {storm_name}: {surge_states}")
