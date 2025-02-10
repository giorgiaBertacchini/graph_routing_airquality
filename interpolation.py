from osgeo import gdal, ogr
import pandas as pd
import sys
import os
from graph_bridge import App


def preprocessing():
    coordinates_df = pd.read_csv('./data/sensor_coordinates.csv')
    measurements_df = pd.read_csv(measures_path)

    merged_df = pd.merge(coordinates_df, measurements_df, on='ID_STATION')

    result_df = merged_df[['LONGITUDE', 'LATITUDE', 'VALUE']]
    result_df.columns = ['X', 'Y', 'VALUE']
    result_df.to_csv('./data/visual_data_' + measures_path.split('_')[-1], index=False)


def interpolation():
    gdal.UseExceptions()

    df = pd.read_csv("data/sensor_coordinates.csv")

    xmin, xmax = df["LONGITUDE"].min(), df["LONGITUDE"].max()
    ymin, ymax = df["LATITUDE"].min(), df["LATITUDE"].max()

    # Find min and max of latitude and longitude of the RoadJunction node of graph
    # TODO
    greeter = App("neo4j://localhost:7687", "neo4j", "password")
    min_lon, max_lon, min_lat, max_lat = greeter.get_extreme_lon_lat()

    x_max = max(xmax, max_lon)
    x_min = min(xmin, min_lon)
    y_max = max(ymax, max_lat)
    y_min = min(ymin, min_lat)

    buffer_percent = 0.05  # Extend the bounds by 5%
    #resolution_offset = 250  # Resolution offset

    x_buffer = (x_max - x_min) * buffer_percent
    y_buffer = (y_max - y_min) * buffer_percent

    x_min -= x_buffer
    x_max += x_buffer
    y_min -= y_buffer
    y_max += y_buffer

    #x_resolution = int((xmax - xmin) * resolution_offset)
    #y_resolution = int((ymax - ymin) * resolution_offset)
    #print(f"Resolution: {x_resolution} x {y_resolution}")

    variation = measures_path.split('_')[-1].split('.')[0]

    res = gdal.Grid("./output/new_idw_" + variation + ".tif", f"./data/sensor_meas_" + variation + ".vrt",
              algorithm="invdist:power=4:radius1=3000:radius2=3000",
              outputBounds=[x_min, y_min, x_max, y_max])

    #res = gdal.Grid("./output/low_res_idw_" + variation + ".tif", f"./data/sensor_meas_" + variation + ".vrt",
    #                algorithm="invdist:power=4:radius1=3000:radius2=3000",
     #               outputBounds=[xmin, ymin, xmax, ymax],
    #                width=x_resolution, height=y_resolution)

    if res == 0:
        print("Interpolation completed. File saved as idw_" + variation + ".tif")


if __name__ == "__main__":
    # Check an argument exists and take first argument as the path to the measurements file
    measures_path = sys.argv[1] if len(sys.argv) > 1 else None

    if measures_path is None or not os.path.exists(measures_path):
        print("File does not exist, please provide a valid path as an argument.")
        sys.exit(2)

    try:
        preprocessing()
        interpolation()
    except Exception as e:
        print(e)
        sys.exit(1)
