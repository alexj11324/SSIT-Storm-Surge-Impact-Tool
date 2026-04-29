"""Read in NHC raster files for estimated storm surge and identify relevant states."""

import io
import re
import time
import zipfile
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin

import geopandas as gpd
import requests
import rasterio
import numpy as np
from rasterio.io import MemoryFile
from requests.adapters import HTTPAdapter
from shapely.geometry import box
from urllib3.util.retry import Retry

try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover
    # Fallback that keeps this module runnable even if tqdm isn't installed.
    # Only supports the context-manager + update calls we use below.
    def tqdm(*_args, **_kwargs):  # type: ignore
        class _Dummy:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def update(self, n: int = 0) -> None:
                pass

            def set_postfix(self, **_kw) -> None:
                pass

        return _Dummy()


try:
    from pygris import states as _pygris_states
except Exception:  # pragma: no cover
    _pygris_states = None

NHC_INUNDATION_INDEX_URL = "https://www.nhc.noaa.gov/gis/archive_inundation_results.php"
NHC_FORECASTS_BASE_URL = "https://www.nhc.noaa.gov/gis/inundation/forecasts/"


def _normalize_storm_id(storm_id: str, year: int) -> str:
    """Ensure storm_id follows basin + two-digit number + four-digit year, using provided year if missing."""
    storm_id = str(storm_id).strip().upper()
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


def _storm_id_variants(normalized_storm_id: str, year: int) -> List[str]:
    compact_storm_id = f"{normalized_storm_id[:4]}{str(year)[-2:]}"
    variants = [compact_storm_id, normalized_storm_id]
    return list(dict.fromkeys(variants))


def _advisory_variants(adv: int) -> List[str]:
    return list(dict.fromkeys([str(int(adv)), f"{int(adv):03d}"]))


def _build_tif_filename(storm_name: str, year: int, adv: int) -> str:
    return f"{str(storm_name).strip().upper()}_{year}_adv{int(adv)}_e10_ResultMaskRaster.tif"


def _get_states(cb: bool, cache: bool, year: int):
    get_states = _pygris_states
    if get_states is None:  # pragma: no cover
        from pygris import states as get_states
    return get_states(cb=cb, cache=cache, year=year)


def _resolve_nhc_archive_urls(
    normalized_storm_id: str,
    adv: int,
    year: int,
    *,
    session: requests.Session,
    timeout: int,
) -> List[str]:
    """Discover the real tidalmask URL(s) from the NHC archive index page."""
    response = session.get(NHC_INUNDATION_INDEX_URL, timeout=timeout)
    response.raise_for_status()

    hrefs = re.findall(r'href="([^"]+_tidalmask\.zip)"', response.text, flags=re.IGNORECASE)
    available_urls = {Path(href).name: urljoin(NHC_INUNDATION_INDEX_URL, href) for href in hrefs}

    exact_names = [
        f"{storm_id}_{adv_value}_tidalmask.zip"
        for storm_id in _storm_id_variants(normalized_storm_id, year)
        for adv_value in _advisory_variants(adv)
    ]
    latest_names = [f"{storm_id}_tidalmask_latest.zip" for storm_id in _storm_id_variants(normalized_storm_id, year)]

    resolved = [available_urls[name] for name in exact_names if name in available_urls]
    if resolved:
        return list(dict.fromkeys(resolved))

    resolved = [available_urls[name] for name in latest_names if name in available_urls]
    return list(dict.fromkeys(resolved))


def _build_nhc_candidate_urls(normalized_storm_id: str, adv: int, year: int) -> List[str]:
    """Return direct forecast URLs as a fallback when archive discovery fails."""
    exact_names = [
        f"{storm_id}_{adv_value}_tidalmask.zip"
        for storm_id in _storm_id_variants(normalized_storm_id, year)
        for adv_value in _advisory_variants(adv)
    ]
    latest_names = [f"{storm_id}_tidalmask_latest.zip" for storm_id in _storm_id_variants(normalized_storm_id, year)]
    return [urljoin(NHC_FORECASTS_BASE_URL, name) for name in list(dict.fromkeys(exact_names + latest_names))]


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
    storm_name = str(storm_name).strip().upper()
    tif_filename_in_zip = _build_tif_filename(storm_name, year, adv)
    download_session = session or _build_session(retries=retries)
    candidate_urls: List[str] = []
    try:
        candidate_urls.extend(
            _resolve_nhc_archive_urls(
                normalized_storm_id,
                adv,
                year,
                session=download_session,
                timeout=timeout,
            )
        )
    except requests.RequestException as exc:
        print(f"WARNING: archive discovery failed ({exc}), falling back to direct URLs")
    candidate_urls.extend(_build_nhc_candidate_urls(normalized_storm_id, adv, year))
    candidate_urls = list(dict.fromkeys(candidate_urls))

    response = None
    selected_url = None
    last_error = None
    for url in candidate_urls:
        print(f"Downloading {url} ...")
        candidate_response = None
        try:
            candidate_response = download_session.get(url, stream=True, timeout=timeout)
            candidate_response.raise_for_status()
        except requests.RequestException as exc:
            last_error = exc
            if candidate_response is not None:
                try:
                    candidate_response.close()
                except Exception:
                    pass
            continue
        response = candidate_response
        selected_url = url
        break

    if response is None:
        raise last_error or RuntimeError("Unable to download NHC tidalmask archive.")

    total_bytes = int(response.headers.get("Content-Length", 0) or 0)
    zip_in_memory = io.BytesIO()
    chunk_size = 1024 * 1024  # 1MB
    with tqdm(
        total=total_bytes if total_bytes > 0 else None,
        desc="Downloading NHC archive",
        unit="B",
        unit_scale=True,
    ) as pbar:
        start = time.time()
        for chunk in response.iter_content(chunk_size=chunk_size):
            if not chunk:
                continue
            zip_in_memory.write(chunk)
            pbar.update(len(chunk))
            elapsed = max(time.time() - start, 1e-9)
            mb_s = zip_in_memory.tell() / elapsed / 1024 / 1024
            # tqdm returns a lightweight _Dummy when disabled; guard against missing helpers.
            if hasattr(pbar, "set_postfix_str"):
                pbar.set_postfix_str(f"{mb_s:.2f} MB/s")
            else:  # pragma: no cover - exercised in unit tests with _Dummy progress bar
                pbar.set_postfix({"rate": f"{mb_s:.2f} MB/s"})
    response.close()

    with zipfile.ZipFile(zip_in_memory, "r") as z:
        if tif_filename_in_zip not in z.namelist():
            raise FileNotFoundError(f"{tif_filename_in_zip} not found in archive (available: {z.namelist()})")

        print(f"Reading {tif_filename_in_zip} from archive...")
        with z.open(tif_filename_in_zip) as tif_file:
            tif_bytes = tif_file.read()

    surge_data = MemoryFile(tif_bytes).open()

    surge_bounds = surge_data.bounds
    surge_polygon = box(surge_bounds.left, surge_bounds.bottom, surge_bounds.right, surge_bounds.top)
    surge_extent_gdf = gpd.GeoDataFrame({"id": 1, "geometry": [surge_polygon]}, crs=surge_data.crs)

    us_states = _get_states(cb=True, cache=True, year=year)
    us_states = us_states.to_crs(surge_data.crs)

    overlapping_states = gpd.sjoin(us_states, surge_extent_gdf, how="inner", predicate="intersects")

    state_names: List[str] = []
    if not overlapping_states.empty:
        state_names = overlapping_states["NAME"].unique().tolist()
    else:
        print("States not found")

    return {
        "data": surge_data,
        "states": state_names,
        "tif_bytes": tif_bytes,
        "tif_name": tif_filename_in_zip,
        "archive_url": selected_url,
    }


def remap_surge_categories(input_path: str, output_path: str, category_map: dict):
    """
    Replace categorical storm surge codes with surge heights in feet.

    Args:
        input_path:   Path to the input GeoTIFF with categorical codes
        output_path:  Path to write the remapped GeoTIFF
        category_map: Dict mapping category code (int) -> surge height (float)
    """
    with rasterio.open(input_path) as src:
        data = src.read(1)
        profile = src.profile
        nodata = src.nodata

    remapped = np.full(data.shape, nodata if nodata is not None else -9999, dtype=np.float32)

    for code, height_ft in category_map.items():
        remapped[data == code] = height_ft

    profile.update(dtype=rasterio.float32, nodata=remapped.fill_value if hasattr(remapped, 'fill_value') else nodata)

    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(remapped, 1)

    return remapped



def download_surge_raster(
    storm_id: str,
    storm_name: str,
    adv: int,
    year: int,
    *,
    output_dir: str | Path,
    timeout: int = 30,
    retries: int = 3,
    session: Optional[requests.Session] = None,
) -> tuple[str, List[str]]:
    """Download the NHC surge raster, save it to disk, and return path + overlapping states."""
    result = import_surge_data(
        storm_id=storm_id,
        storm_name=storm_name,
        adv=adv,
        year=year,
        timeout=timeout,
        retries=retries,
        session=session,
    )
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    raster_path = output_dir / result["tif_name"]
    raster_path.write_bytes(result["tif_bytes"])
    result["data"].close()
    category_map = {
    1:  1.0,
    2:  1.0,
    3:  3.0,
    4:  6.0,
    5:  9.0,
    }   
    remap_surge_categories(input_path = str(raster_path), 
                           output_path = str(raster_path), 
                           category_map = category_map)
    return str(raster_path), result["states"]


if __name__ == "__main__":
    ## - user inputs
    storm_name = "BERYL"
    storm_id = "AL022024"
    advisory_no = 29
    year = 2024

    ## - get storm surge data and relevant states
    surge_dict = import_surge_data(storm_id=storm_id, storm_name=storm_name, adv=advisory_no, year=year)
    surge_data = surge_dict["data"]
    surge_states = surge_dict["states"]
    print(f"States in the storm surge data for {storm_name}: {surge_states}")
