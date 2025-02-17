import json
import sys

from interpolation import main as interpolation_main
from merge_airquality_footpath import main as merge_main


if __name__ == '__main__':
    with open("data/config.json", "r") as file:
        config = json.load(file)

    try:
        raster_path = interpolation_main()
        merge_main(raster_path)
    except Exception as e:
        print(e)
        sys.exit(1)
