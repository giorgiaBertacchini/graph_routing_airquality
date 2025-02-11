import pandas as pd
import sys
import os
import json
from osgeo import gdal
from graph_bridge import App


def validate_file_path(file_path, file_description):
    if file_path is None or not os.path.exists(file_path):
        print(f"File {file_description} does not exist, please provide a valid path in the config file.")
        sys.exit(2)


def preprocessing():
    coordinates_df = pd.read_csv(coords_path)
    measurements_df = pd.read_csv(measures_path)

    merged_df = pd.merge(coordinates_df, measurements_df, on='ID_STATION')

    result_df = merged_df[['LONGITUDE', 'LATITUDE', 'VALUE']]
    result_df.columns = ['X', 'Y', 'VALUE']
    result_df.to_csv(f'./data/visual_data_{measures_path.split("_")[-1]}', index=False)


def interpolation():
    gdal.UseExceptions()

    df = pd.read_csv(coords_path)

    xmin, xmax = df["LONGITUDE"].min(), df["LONGITUDE"].max()
    ymin, ymax = df["LATITUDE"].min(), df["LATITUDE"].max()

    # Find min and max of latitude and longitude of the RoadJunction node of graph
    min_lon, max_lon, min_lat, max_lat = greeter.get_extreme_lon_lat()

    x_max = max(xmax, max_lon)
    x_min = min(xmin, min_lon)
    y_max = max(ymax, max_lat)
    y_min = min(ymin, min_lat)

    buffer_percent = 0.05  # Extend the bounds by 5%
    x_buffer = (x_max - x_min) * buffer_percent
    y_buffer = (y_max - y_min) * buffer_percent

    x_min -= x_buffer
    x_max += x_buffer
    y_min -= y_buffer
    y_max += y_buffer

    variation = measures_path.split('_')[-1].split('.')[0]

    res = gdal.Grid(f"./output/new_idw_{variation}.tif", f"./data/sensor_meas_{variation}.vrt",
              algorithm="invdist:power=4:radius1=3000:radius2=3000",
              outputBounds=[x_min, y_min, x_max, y_max])

    if res == 0:
        print(f"Interpolation completed. File saved as new_idw_{variation}.tif")

    """# If you want change the resolution of the output raster:
    resolution_offset = 250  # Resolution offset
    x_resolution = int((xmax - xmin) * resolution_offset)
    y_resolution = int((ymax - ymin) * resolution_offset)

    res = gdal.Grid("./output/low_res_idw_" + variation + ".tif", f"./data/sensor_meas_" + variation + ".vrt",
                    algorithm="invdist:power=4:radius1=3000:radius2=3000", outputBounds=[xmin, ymin, xmax, ymax], width=x_resolution, height=y_resolution)
    """


if __name__ == "__main__":
    config = json.loads("./data/config.json")
    measures_path = config['measures_path'] if 'measures_path' in config else None
    coords_path = config['sensor_coords_path'] if 'sensor_coords_path' in config else None

    validate_file_path(measures_path, 'measures_path')
    validate_file_path(coords_path, 'sensor_coords_path')

    try:
        greeter = App(config['neo4j_URL'], config['neo4j_user'], config['neo4j_password'])

        preprocessing()
        interpolation()
    except Exception as e:
        print(e)
        sys.exit(1)
