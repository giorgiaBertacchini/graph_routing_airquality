import time
import json
import numpy as np
from graph_bridge import App


def coordinates_to_geojson(coordinates, weight, value, total_distance, total_green_area, avg_pm10, index):
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
                    "weight": value,
                    "total_distance": total_distance,
                    "total_green_area": total_green_area,
                    "avg_pm10": avg_pm10
                }
            }
        ]
    }

    # save the GeoJSON file
    file_name = f"output/routing/path_{weight}_{index}_{routing_query['path_file_suffix']}.geojson"
    with open(file_name, "w") as f:
        json.dump(geojson_data, f)
        print(f"GeoJSON file saved at {file_name}")


def calculate_iqr_bounds(values):
    Q1 = np.percentile(values, 25)
    Q3 = np.percentile(values, 75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return lower_bound, upper_bound


def get_edge_props_bounds(greeter):
    result = greeter.get_distances_and_effective_pm10()
    distance_values = []
    pm10_values = []

    for record in result:
        distance_values.append(record[0])
        pm10_values.append(record[1])

    distance_lower_bound, distance_upper_bound = calculate_iqr_bounds(distance_values)
    pm10_lower_bound, pm10_upper_bound = calculate_iqr_bounds(pm10_values)

    return distance_lower_bound, distance_upper_bound, pm10_lower_bound, pm10_upper_bound


def create_multiple_weights_propriety(greeter, combined_weight_config):
    # TODO to delete
    #distance_lower_bound, distance_upper_bound, pm10_lower_bound, pm10_upper_bound = get_edge_props_bounds(greeter)

    # TODO test meglio in IQ o senza?

    # Create a unique property for each edge with all the weights
    parameters = {
        'distance_power': combined_weight_config['distance']['power'],
        'pm10_power': combined_weight_config['eff_pm10']['power'],
        'inv_green_area_power': combined_weight_config['inv_green_area']['power'],
        'distance_ratio': combined_weight_config['distance']['ratio'],
        'pm10_ratio': combined_weight_config['eff_pm10']['ratio'],
        'inv_green_area_ratio': combined_weight_config['inv_green_area']['ratio'],
        'min_distance': 0, #distance_lower_bound,
        #'max_distance': distance_upper_bound,  # TODO delete
        'min_pm10': 0, #pm10_lower_bound,
        #'max_pm10': pm10_upper_bound,  # TODO delete
        'min_inv_green_area': 0, #min_inv_green_area,
    }

    result = greeter.add_combined_property(parameters)
    #res = greeter.add_new_prop(parameters)


def routing_single_weight_path(greeter, source, target, weight, algorithm, k=2, bool_map=False):
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
        path, totalCost, total_distance, total_green_area, avg_pm10 = r

        # Remove duplicates from the path
        final_path = [path[0]] + [p for prev, p in zip(path, path[1:]) if p != prev]

        if bool_map:
            # Node coordinates of the path
            coordinates = greeter.get_coordinates(final_path=final_path)
            if len(coordinates[0][0]) == 0:
                print('\nNo result for query')
            else:
                # Save the path in a GeoJSON file
                coordinates_to_geojson(coordinates[0][0], weight, totalCost, total_distance, total_green_area, avg_pm10, index)

        path_data.append({'hops': len(final_path), 'source': source, 'target': target, 'cost': totalCost,
                          'distance': total_distance, 'green_area': total_green_area, 'pm10': avg_pm10})

    return path_data


def main():
    greeter = App(config['neo4j_URL'], config['neo4j_user'], config['neo4j_pwd'])

    w = routing_query['weight']  # ["distance", "green_area", "pm10", 'combined_property']

    if w == 'combined_weight' or w == 'green_area':
        greeter.add_inverse_green_area()
    if w == 'combined_weight' or w == 'effective_pm10':
        greeter.add_effective_pm10(routing_query['effective_pm10']['c1'], routing_query['effective_pm10']['c2'])
    if w == 'combined_weight':
        create_multiple_weights_propriety(greeter, routing_query['combined_weight'])

    result = routing_single_weight_path(
        greeter, routing_query['source_id'], routing_query['destination_id'], w, routing_query['algorithm'], routing_query['top_k'], True)

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
            print("total green area: " + str(r['green_area']))
            print("average pm10: " + str(r['pm10']))

    greeter.close()
    return 0


if __name__ == "__main__":
    with open("data/config.json", "r") as file:
        config = json.load(file)
    with open("data/routing_query.json", "r") as file:
        routing_query = json.load(file)
    main()
