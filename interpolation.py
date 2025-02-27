import pandas as pd
import sys
import os
import json
from osgeo import gdal
from graph_bridge import App
from export_to_csv import export_pm10_to_csv


def validate_file_path(file_path):
    """
    Check if the file path is valid
    """
    if file_path is None or not os.path.exists(file_path):
        print(f"File {file_path} does not exist, please provide a valid path in the config file.")
        sys.exit(2)


def interpolation(greeter, measures_path, coords_path, raster_path, power=4, radius1=3000, radius2=3000):
    """
    Interpolate the sensor measures in a raster file
    """
    df = pd.read_csv(coords_path)

    # Find min and max of latitude and longitude of the sensor nodes
    sensor_lon_min, sensor_lon_max = df["LONGITUDE"].min(), df["LONGITUDE"].max()
    sensor_lat_min, sensor_lat_max = df["LATITUDE"].min(), df["LATITUDE"].max()

    # Find min and max of latitude and longitude of the RoadJunction node of graph
    road_lon_min, road_lon_max, road_lat_min, road_lat_max = greeter.get_extreme_lon_lat()

    x_max = max(sensor_lon_max, road_lon_max)
    x_min = min(sensor_lon_min, road_lon_min)
    y_max = max(sensor_lat_max, road_lat_max)
    y_min = min(sensor_lat_min, road_lat_min)

    buffer_percent = 0.05  # Extend the bounds by 5%
    x_buffer = (x_max - x_min) * buffer_percent
    y_buffer = (y_max - y_min) * buffer_percent

    x_min -= x_buffer
    x_max += x_buffer
    y_min -= y_buffer
    y_max += y_buffer

    variation = measures_path.split('_')[-1].split('.')[0]

    gdal.Grid(raster_path, f"./output/sensors/meas_{variation}.vrt",
              algorithm=f"invdist:power={power}:radius1={radius1}:radius2={radius2}",
              outputBounds=[x_min, y_min, x_max, y_max])

    return raster_path


def main(config):
    gdal.UseExceptions()
    measures_path = config['measures_path'] if 'measures_path' in config else None
    coords_path = config['sensor_coords_path'] if 'sensor_coords_path' in config else None

    validate_file_path(measures_path)
    validate_file_path(coords_path)

    greeter = App(config['neo4j_URL'], config['neo4j_user'], config['neo4j_pwd'])

    export_pm10_to_csv(measures_path, coords_path)
    raster_path = interpolation(greeter, measures_path, coords_path, config['raster_path'],
                                config['idw']['power'], config['idw']['radius1'],
                                config['idw']['radius2'])
    print(f"Creating raster file {raster_path}...")

    greeter.close()

    return raster_path


if __name__ == "__main__":
    with open("data/config.json", "r") as file:
        config_file = json.load(file)

    try:
        main(config_file)
    except Exception as e:
        print(e)
        sys.exit(1)
