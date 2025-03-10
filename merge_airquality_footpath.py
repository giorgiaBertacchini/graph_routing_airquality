import sys
import json
import time
import os
from osgeo import gdal
import numpy as np
from graph_bridge import App
from scipy.ndimage import map_coordinates
from export_to_csv import export_edges_to_csv, export_road_junctions_to_csv


def sample_with_window(raster, x_vals, y_vals, buffer_size=3):
    """
    Extract values from a raster using a window around the segment
    """
    half_w = buffer_size // 2  # Dimension of the buffer

    sampled_values = []

    for x, y in zip(x_vals, y_vals):

        # Generate the grid around the point
        x_grid, y_grid = np.meshgrid(
            np.arange(x - half_w, x + half_w + 1),
            np.arange(y - half_w, y + half_w + 1)
        )

        # Extract the values from the raster using a bilinear interpolation (order=1)
        values = map_coordinates(raster, [y_grid.ravel(), x_grid.ravel()], order=1)

        # Mean value of the window around a single segment point
        sampled_values.append(np.mean(values))
    return np.array(sampled_values)


def world_to_pixel(transform, lon, lat):
    """
    Convert world coordinates to pixel coordinates
    """
    x_origin, pixel_width, _, y_origin, _, pixel_height = transform

    pixel_x = (lon - x_origin) / pixel_width
    pixel_y = (lat - y_origin) / pixel_height

    return pixel_x, pixel_y


def sample_raster_along_line(config, raster_path, coordinate_pair):
    """
    Sample a raster along a segment defined by two points in the world coordinates
    """
    raster = gdal.Open(raster_path)
    if raster is None:
        print("Error: raster not found")
        return

    band = raster.GetRasterBand(1)
    transform = raster.GetGeoTransform()

    data = band.ReadAsArray(0, 0, raster.RasterXSize, raster.RasterYSize)

    px0, py0 = world_to_pixel(transform, coordinate_pair[0][0], coordinate_pair[0][1])
    px1, py1 = world_to_pixel(transform, coordinate_pair[1][0], coordinate_pair[1][1])

    # Generate the x and y values for the segment
    x_vals = np.linspace(px0, px1)
    y_vals = np.linspace(py0, py1)

    air_qualities = sample_with_window(data, x_vals, y_vals, buffer_size=config['air_quality_in_footpath']['buffer_size'])

    mean_value = np.mean(air_qualities)
    return mean_value


def main(config):
    gdal.UseExceptions()
    greeter = App(config['neo4j_URL'], config['neo4j_user'], config['neo4j_pwd'])

    # Get the coordinates of the node pairs from edges in the graph
    edges = greeter.get_edges_endpoints()
    id_pairs = []
    mean_air_quality_values = []

    print(f"Start sampling raster along {len(edges)} edges (this operation may take a while)...")
    i = 0
    start_time = time.time()
    for edge in edges:
        i += 1
        source_id, destination_id, source_lon, source_lat, destination_lon, destination_lat = edge

        raster_file = config['raster_path']
        # Find the mean air quality along the segment
        mean_air_quality = sample_raster_along_line(config, raster_file, [(source_lon, source_lat), (destination_lon, destination_lat)])

        id_pairs.append([source_id, destination_id])
        mean_air_quality_values.append(mean_air_quality)

    print("Time to sample raster: ", time.time() - start_time)

    result = greeter.add_edge_air_quality_in_bulk(id_pairs, mean_air_quality_values)
    if result == mean_air_quality_values:
        print("All air quality values have been added to the graph.")

    # Check if road junctions csv file exists
    if not os.path.exists("output/exported_graph/road_junctions.csv"):
        export_road_junctions_to_csv(greeter)
    else:
        print("Road junctions csv file already exists.")

    # Export the updated edges to a csv file
    export_edges_to_csv(greeter, config['measures_path'])

    greeter.close()


if __name__ == '__main__':
    with open("data/config.json", "r") as file:
        config_file = json.load(file)

    try:
        main(config_file)
    except Exception as e:
        print(e)
        sys.exit(1)
