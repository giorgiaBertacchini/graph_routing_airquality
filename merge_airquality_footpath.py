#import rasterio
import numpy as np
from shapely.geometry import LineString
#from rasterio.mask import mask

import argparse
from graph_bridge import App


def sample_raster_along_line(coordinates_pair):
    raster_path = './output/idw_75pc.tif'
    #raster = rasterio.open(raster_path)

    # Create a LineString object from the coordinates pair
    segment = LineString(coordinates_pair)

    buffer_size = 10  # TODO valutare la dimensione del buffer

    # Extend the segment by the buffer size
    buffered_segment = segment.buffer(buffer_size)

    # Mask the raster with the segment
    out_image, out_transform = mask(raster, [buffered_segment], crop=True)

    # TODO valutare altre metriche...
    mean_value = np.mean(out_image)
    print(f"Mean value for segment: {mean_value}")

    # Close the raster
    #raster.close()

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


def main(args=None):
    """Parsing input parameters"""
    arg_parser = add_options()
    options = arg_parser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    # Get the coordinates of the node pairs from edges in the graph
    coordinates = greeter.get_edges()

    for pair in coordinates:
        mean_air_quality = sample_raster_along_line(pair)
        greeter.add_edge_air_quality(pair, mean_air_quality)


if __name__ == '__main__':
    main()
