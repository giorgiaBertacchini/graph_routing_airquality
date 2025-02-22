import json
import sys

from interpolation import main as interpolation_main
from merge_airquality_footpath import main as merge_main


if __name__ == '__main__':
    """
    This script is used if you have new air quality data (new PM10 values) from sensors 
    and you want update the air quality data in the database.
    """

    with open("data/config.json", "r") as file:
        config_file = json.load(file)

    try:
        raster_path = interpolation_main(config_file)
        merge_main(config_file, raster_path)
    except Exception as e:
        print(e)
        sys.exit(1)
