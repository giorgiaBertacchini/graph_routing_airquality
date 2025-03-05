import time
import json
import numpy as np
from graph_bridge import App


def coordinates_to_geojson(coordinates, weight, value, tot_distance, tot_green_area, avg_pm10, total_pm10_metre,
                           total_inv_ga_metre, total_green_area_distance, index):
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
                "properties": {
                    "total_cost": value,
                    "total_distance": tot_distance,
                    "total_green_area": tot_green_area,
                    "avg_pm10": avg_pm10,
                    "total_pm10_metre": total_pm10_metre,
                    "total_inv_ga_metre": total_inv_ga_metre,
                    "avg_pm10_metre": total_pm10_metre / tot_distance,
                    "total_ga_distance": total_green_area_distance
                }
            }
        ]
    }

    # save the GeoJSON file
    file_name = f"output/routing/path_{weight}_{index}_{routing_query['path_file_suffix']}.geojson"
    with open(file_name, "w") as f:
        json.dump(geojson_data, f)
        print(f"GeoJSON file saved at {file_name}")


def create_multiple_weights_propriety(greeter, combined_weight_config):
    """
    Create a combined weight property for the edges of the footway graph
    """

    # Get parameters for the combined weight
    parameters = {
        'pm10_ratio': combined_weight_config['eff_pm10']['ratio'],
        'inv_green_area_ratio': combined_weight_config['inv_green_area']['ratio']
    }

    greeter.add_combined_property(parameters)


def routing_path(greeter, source, target, weight, algorithm, k=2, bool_map=True):
    """
    Find the path(s) between two nodes in the footway graph
    """
    start_time = time.time()

    greeter.drop_all_projections()

    paths = []
    if algorithm == 'dijkstra':
        paths = greeter.dijkstra_path(source, target, weight)
    elif algorithm == 'a_star':
        paths = greeter.a_star_path(source, target, weight)
    elif algorithm == 'top_k':
        paths = greeter.top_k_paths(source, target, weight, k)

    greeter.drop_all_projections()

    if len(paths) == 0:
        return {'error': 'No path found'}

    path_data = [time.time() - start_time]

    for index, r in enumerate(paths):
        path, totalCost, total_distance, total_green_area, avg_pm10, total_pm10_metre, total_inv_ga_metre, total_green_area_distance = r

        # Remove duplicates from the path
        final_path = [path[0]] + [p for prev, p in zip(path, path[1:]) if p != prev]

        if bool_map:
            # Node coordinates of the path
            coordinates = greeter.get_coordinates(final_path=final_path)
            if len(coordinates[0][0]) == 0:
                print('\nNo result for query')
            else:
                # Save the path in a GeoJSON file
                coordinates_to_geojson(
                    coordinates[0][0], weight, totalCost, total_distance, total_green_area, avg_pm10, total_pm10_metre,
                    total_inv_ga_metre, total_green_area_distance, index)

        path_data.append({'hops': len(final_path), 'source': source, 'target': target, 'cost': totalCost,
                          'distance': total_distance, 'pm10': avg_pm10, 'green_area': total_green_area,
                          'pm10_metre': total_pm10_metre, 'inv_ga_metre': total_inv_ga_metre,
                          'green_area_distance': total_green_area_distance})

    return path_data


def main():
    greeter = App(config['neo4j_URL'], config['neo4j_user'], config['neo4j_pwd'])

    if routing_query['update_graph_properties']:
        print("Updating graph properties as weights for path finding algorithm...")
        greeter.add_inv_green_area_metre()
        greeter.add_pm10_metre()
        #greeter.add_green_area_distance()  # only for results visualization

    w = routing_query['weight']  # "distance", "pm10_metre, "inv_ga_metre", "combined_weight"

    if w == 'combined_weight':
        create_multiple_weights_propriety(greeter, routing_query['combined_weight'])

    result = routing_path(
        greeter, routing_query['source_id'], routing_query['destination_id'],
        w, routing_query['algorithm'], routing_query['top_k'], True)

    print("\n-- Routing results --")
    print("execution time: " + str(result[0]))
    print("source: " + str(routing_query['source_id']))
    print("destination: " + str(routing_query['destination_id']))

    for r in result[1:]:
        print("\n|| Weight: " + w + " ||")
        if 'error' in r:
            print("No path found")
            return 1
        else:
            print("number of hops: " + str(r['hops']))
            print("total cost: " + str(r['cost']))
            print("total distance: " + str(r['distance']))
            print("average pm10: " + str(r['pm10']))
            print("total green area: " + str(r['green_area']))
            print("total pm10 per metre: " + str(r['pm10_metre']))
            print("total inverse green area per metre: " + str(r['inv_ga_metre']))
            print("avg pm10 per metre: " + str(r['pm10_metre']/r['distance']))
            print("total green area distance: " + str(r['green_area_distance']))

    greeter.close()
    return 0


if __name__ == "__main__":
    with open("data/config.json", "r") as file:
        config = json.load(file)
    with open("data/routing_query.json", "r") as file:
        routing_query = json.load(file)
    main()
