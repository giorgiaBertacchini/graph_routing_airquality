from osgeo import gdal, ogr
import pandas as pd
import sys
import os


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

    buffer_percent = 0.15  # Extend the bounds by 15%

    x_buffer = (xmax - xmin) * buffer_percent
    y_buffer = (ymax - ymin) * buffer_percent

    xmin -= x_buffer
    xmax += x_buffer
    ymin -= y_buffer
    ymax += y_buffer

    variation = measures_path.split('_')[-1].split('.')[0]

    res = gdal.Grid("./output/idw_" + variation + ".tif", f"./data/sensor_meas_" + variation + ".vrt",
              algorithm="invdist:power=4:radius1=3000:radius2=3000",
              outputBounds=[xmin, ymin, xmax, ymax])
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
