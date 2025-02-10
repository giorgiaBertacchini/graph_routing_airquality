#import overpy
import json
import argparse
#import folium as fo  # TODO to add
import os
import time
from graph_bridge import App


def coordinates_to_geojson(coordinates):
    """
    Convert a list of coordinates to a GeoJSON file
    """
    geojson_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": coordinates
                },
                "properties": {}
            }
        ]
    }

    # save the GeoJSON file
    with open("path.geojson", "w") as f:
        json.dump(geojson_data, f)


def routing_shortest_path(greeter, source, target, weight, bool_map=False, file=''):
    start_time = time.time()

    #result = greeter.shortest_path(source, target, weight)
    result = greeter.single_weight_path(source, target, weight)
    path, total_distance, total_green_area, avg_pm10 = result#[0]
    #total_cost = result[1]
    #total_danger = result[2]
    #total_distance = result[3]

    # Remove duplicates from the path
    final_path = [str(path[0])] + [str(p) for prev, p in zip(path, path[1:]) if str(p) != str(prev)]

    dic = {
        'exec_time': time.time() - start_time,
        'hops': len(final_path)
    }

    if bool_map:
        # Visualization of the path
        coordinates = greeter.get_coordinates(final_path=str(final_path))
        if len(coordinates[0][0]) == 0:
            print('\nNo result for query')
        else:
            # Save the path in a GeoJSON file
            coordinates_to_geojson(coordinates[0][0])  # TODO to check

    dic['source'] = source
    dic['target'] = target
    dic['distance'] = total_distance
    dic['green_area'] = total_green_area
    dic['pm10'] = avg_pm10

    greeter.drop_all_projections()

    return dic


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
    parser.add_argument('--beta', '-b', dest='beta', type=float,
                        help="""Insert the beta parameter between 0 and 1. The value represent the importance of travel time on the final cost.""",
                        required=False, default=0.5)
    parser.add_argument('--destination', '-d', dest='dest', type=str,
                        help="""Insert the osm identifier of your destination""",
                        required=False)
    parser.add_argument('--source', '-s', dest='source', type=str,
                        help="""Insert the osm identifier of your source""",
                        required=False)
    parser.add_argument('--mapName', '-mn', dest='mapName', type=str,
                        help="""Insert the name of the file containing the map with the computed path.""",
                        required=True,)
    #parser.add_argument('--latitude', '-x', dest='lat', type=float,
    #                    help="""Insert latitude of your starting location""",
    #                    required=False)
    #parser.add_argument('--longitude', '-y', dest='lon', type=float,
    #                    help="""Insert longitude of your starting location""",
     #                   required=False)
    #parser.add_argument('--latitude_dest', '-x_dest', dest='lat_dest', type=float,
      #                  help="""Insert latitude of your destination location""",
     #                   required=False)
    #parser.add_argument('--longitude_dest', '-y_dest', dest='lon_dest', type=float,
      #                  help="""Insert longitude of your destination location""",
     #                   required=False)
    #parser.add_argument('--weight', '-w', dest='weight', type=str,help="""Insert the weight to use in order to perform the routing : travel_time, cost or both.""",
    #                    required=False, default = 'both')

    return parser


def main(args=None):
    """Parsing input parameters"""
    arg_parser = add_options()
    options = arg_parser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    if options.beta > 1 or options.beta < 0:
        print("The beta parameter value is not valid, 0.5 will be used")
        options.beta = 0.5

    for w in ["distance", "green_area", "pm10"]:
        result = routing_shortest_path(greeter, options.source, options.dest, w, True, options.mapName)

        print("Weight: " + w)
        print("execution time:" + str(result['exec_time']))
        print("number of hops:" + str(result['hops']))
        print("total distance:" + str(result['distance']))
        print("total green area:" + str(result['green_area']))
        print("average pm10:" + str(result['pm10']))

    return 0


if __name__ == "__main__":
    main()

# python routing.py -n neo4j://localhost:7687 -u neo4j -p password -mn prova -s 1314391413 -d 277291137