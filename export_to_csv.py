import csv
import sys
import pandas as pd
import json
from graph_bridge import App


def export_pm10_to_csv(measures_path, coords_path):
    """
    It is called by the interpolation.py script, to update the pm10 values each time you run the interpolation.
    Export PM10 measurements to csv file with columns: X, Y, VALUE.
    Merging two csv files: coordinates and measurements.
    """

    coordinates_df = pd.read_csv(coords_path)
    measurements_df = pd.read_csv(measures_path)

    merged_df = pd.merge(coordinates_df, measurements_df, on='ID_STATION')

    result_df = merged_df[['LONGITUDE', 'LATITUDE', 'VALUE']]
    result_df.columns = ['X', 'Y', 'VALUE']
    result_df.to_csv(f'./output/sensors/data_{measures_path.split("_")[-1]}', index=False)

    print(f"PM10 values exported to ./output/sensors/data_{measures_path.split('_')[-1]} file.")


def export_edges_to_csv(greeter):
    """
    Export road edges to csv file with columns: source, target, source_lon, source_lat, target_lon, target_lat,
    name, altitude, distance, green_area, pm10.
    """
    result = greeter.get_road_edges()

    file_path = "output/exported_graph/edges.csv"
    with open(file_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["source", "target", "source_lon", "source_lat", "target_lon", "target_lat",
                         "name", "altitude", "distance", "green_area", "pm10"])
        for record in result:
            writer.writerow(list(record))

    print(f"Edges exported to {file_path} file.")


def export_road_junctions_to_csv(greeter):
    """
    Export road junctions to csv file with columns: id, lon, lat.
    """
    result = greeter.get_road_junction_nodes()

    file_path = "output/exported_graph/road_junctions.csv"
    with open(file_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["id", "lon", "lat"])
        for record in result:
            writer.writerow(list(record))

    print(f"Road junctions exported to {file_path} file.")


if __name__ == "__main__":
    with open("data/config.json", "r") as file:
        config = json.load(file)

    greeter_app = App(config['neo4j_URL'], config['neo4j_user'], config['neo4j_pwd'])

    try:
        export_edges_to_csv(greeter_app)
        export_road_junctions_to_csv(greeter_app)
    except Exception as e:
        print(e)
        sys.exit(1)
    finally:
        greeter_app.close()
