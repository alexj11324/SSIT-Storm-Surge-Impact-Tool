
"""Read in NHC raster files for estimated storm surge and identify relevant states."""

import requests
import io
import zipfile
import rasterio
import geopandas as gpd
from pygris import states
from rasterio.mask import mask
from shapely.geometry import box


def import_surge_data(storm_id, storm_name, adv, year):
    """
    Reads estimated storm surge TIFF files from NHC website for a given storm and advisory.

    Args:
        storm_id (str): The identifier associated with the storm (e.g. AL022024).
        storm_name (str): The name associated with the storm (e.g. Beryl).
        adv (int): The number of the latest advisory for the storm (e.g. 29).
        year (int): The year of the storm (e.g. 2024)

    Returns:
        dictionary: (1) The storm surge heights data from the raster file and (2) a list of states captured in the raster data
    """
    storm_id = storm_id.upper()
    storm_name = storm_name.upper()
    adv = str(adv)
    year = str(year)

    url = f"https://www.nhc.noaa.gov/gis/inundation/forecasts/{storm_id}_{adv}_tidalmask.zip"
    tif_filename_in_zip = f"{storm_name}_{year}_adv{adv}_e10_ResultMaskRaster.tif"

    # Stream the ZIP file content into memory
    print(f"Downloading ZIP file from {url} into memory...")
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Ensure the download was successful

    # Use BytesIO to handle the bytes data in memory
    zip_in_memory = io.BytesIO(response.content)

    # Open the ZIP file from the in-memory bytes
    with zipfile.ZipFile(zip_in_memory, 'r') as z:
        # Check if the desired TIF file exists
        if tif_filename_in_zip not in z.namelist():
            print(f"Error: {tif_filename_in_zip} not found in the archive.")
            return None

        # Read the specific TIF file data from the ZIP archive
        print(f"Reading {tif_filename_in_zip} from archive...")
        with z.open(tif_filename_in_zip) as tif_file:
            surge_data = rasterio.open(tif_file)

    # Get surge data bounds for comparison with U.S. state boundaries
    surge_bounds = surge_data.bounds
    surge_polygon = box(surge_bounds.left, surge_bounds.bottom, surge_bounds.right, surge_bounds.top)
    surge_extent_gdf = gpd.GeoDataFrame({'id': 1, 'geometry': [surge_polygon]}, crs=surge_data.crs)

    # Compare U.S. state boudaries with surge data to identify relevant states for storm surge
    us_states = states(cb=True, cache=False, year=2024)
    us_states = us_states.to_crs(surge_data.crs)

    overlapping_states = gpd.sjoin(us_states, surge_extent_gdf, how="inner", predicate="intersects")

    # Save states
    if not overlapping_states.empty:
        state_names = overlapping_states['NAME'].unique()
    else:
      print("States not found")

    return {'data': surge_data, 'states': state_names}



if __name__ == "__main__":
    ## - user inputs
    storm_name = "BERYL"
    storm_id = "AL0224"
    advisory_no = 29
    year = 2024

    ## - get storm surge data and relevant states
    surge_dict = import_surge_data(storm_id = storm_id, 
                                   storm_name = storm_name, 
                                   adv = advisory_no, 
                                   year = year)
    surge_data = surge_dict['data']
    surge_states = surge_dict['states']
    print(f"States in the storm surge data for {storm_name}: {surge_states}")
