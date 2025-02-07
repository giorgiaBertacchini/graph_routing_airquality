from osgeo import gdal
import numpy as np
import argparse
from graph_bridge import App
from scipy.ndimage import map_coordinates


def sample_with_window(raster, x_vals, y_vals, window_size=3):
    """
    Extract values from a raster using a window around the segment
    """
    half_w = window_size // 2  # Dimension of the window

    sampled_values = []
    for x, y in zip(x_vals, y_vals):
        # Generate the grid around the point
        x_grid, y_grid = np.meshgrid(
            np.arange(x - half_w, x + half_w + 1),
            np.arange(y - half_w, y + half_w + 1)
        )

        # TODO valutare se usare una gaussiana o altro
        # Extract the values from the raster using a bilinear interpolation (order=1)
        values = map_coordinates(raster, [y_grid.ravel(), x_grid.ravel()], order=1)

        # Mean value of the window around a single segment point
        sampled_values.append(np.mean(values))

    return np.array(sampled_values)


def world_to_pixel(transform, x, y):
    """
    Convert world coordinates to pixel coordinates
    """
    x_origin, pixel_width, _, y_origin, _, pixel_height = transform

    pixel_x = (x - x_origin) / pixel_width
    pixel_y = (y - y_origin) / pixel_height

    return pixel_x, pixel_y


def sample_raster_along_line(raster_path, coordinate_pair):
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

    air_qualities = sample_with_window(data, x_vals, y_vals, window_size=5)

    mean_value = np.mean(air_qualities)
    print(f"Mean value for segment {coordinate_pair}: {mean_value}")
    return mean_value


def add_options():
    """Parameters needed to run the script"""
    parser = argparse.ArgumentParser(description='Insertion of POI in the graph.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    return parser


def main(raster_path, args=None):
    """
    Parsing input parameters and running the script
    """
    arg_parser = add_options()
    options = arg_parser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    # Get the coordinates of the node pairs from edges in the graph
    coordinates = greeter.get_edges()

    for pair in coordinates:
        # Find the mean air quality along the segment
        mean_air_quality = sample_raster_along_line(raster_path, pair)

        # Add the air quality to the edge in the graph
        greeter.add_edge_air_quality(pair, mean_air_quality)

    greeter.close()


if __name__ == '__main__':
    gdal.UseExceptions()

    file_path = './output/idw_75pc.tif'

    #main(file_path)
    sample_raster_along_line(file_path, [(10.9532796, 44.6296937), (10.948936, 44.629214)])
