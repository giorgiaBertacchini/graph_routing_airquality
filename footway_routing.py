import time
import json
import numpy as np
from graph_bridge import App


def coordinates_to_geojson(coordinates, weight, value, total_distance, total_green_area, avg_pm10):
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
    file_name = f"output/path_{weight}_{data['graph_db_name']}.geojson"  # TODO attenzione cambiare il path anche in qgis
    with open(file_name, "w") as f:
        json.dump(geojson_data, f)


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


def create_multiple_weights_propriety(greeter, powers, rations):
    # TODO to delete
    distance_lower_bound, distance_upper_bound, pm10_lower_bound, pm10_upper_bound = get_edge_props_bounds(greeter)

    # Create a unique property for each edge with all the weights
    parameters = {
        'distance_power': powers['distance_power'],
        'pm10_power': powers['pm10_power'],
        'distance_ratio': rations['distance_ratio'],
        'pm10_ratio': rations['pm10_ratio'],
        'min_distance': distance_lower_bound,
        'max_distance': distance_upper_bound,
        'min_pm10': pm10_lower_bound,
        'max_pm10': pm10_upper_bound
    }

    result = greeter.add_combined_property(parameters)
    #res = greeter.add_new_prop(parameters)

    r = result

    # TODO check result?


def routing_single_weight_path(greeter, source, target, weight, algorithm, k=2, bool_map=False):
    start_time = time.time()

    greeter.drop_all_projections()

    if algorithm == 'dijkstra':
        result = greeter.dijkstra_path(source, target, weight)
    elif algorithm == 'a_star':
        result = greeter.a_star_path(source, target, weight)
    elif algorithm == 'top_k':
        result = greeter.top_k_paths(source, target, weight, k)

    if len(result) == 0:
        return {'error': 'No path found'}

    path, totalCost, total_distance, total_green_area, avg_pm10 = result

    # Remove duplicates from the path
    final_path = [path[0]] + [p for prev, p in zip(path, path[1:]) if p != prev]

    if bool_map:
        # Node coordinates of the path
        coordinates = greeter.get_coordinates(final_path=final_path)
        if len(coordinates[0][0]) == 0:
            print('\nNo result for query')
        else:
            # Save the path in a GeoJSON file
            coordinates_to_geojson(coordinates[0][0], weight, totalCost, total_distance, total_green_area, avg_pm10)  # TODO to check

    greeter.drop_all_projections()

    return {'exec_time': time.time() - start_time, 'hops': len(final_path), 'source': source, 'target': target,
            'cost': totalCost, 'distance': total_distance, 'green_area': total_green_area, 'pm10': avg_pm10}


def main():
    greeter = App(data['neo4j_URL'], data['neo4j_user'], data['neo4j_pwd'])

    w = data['weight']  # ["distance", "green_area", "pm10", 'combined_property']

    if w == 'combined_weight' or w == 'effective_pm10':
        greeter.add_effective_pm10()
    if w == 'combined_weight':
        create_multiple_weights_propriety(greeter, data['powers'], data['rations'])

    result = routing_single_weight_path(
        greeter, data['source_id'], data['destination_id'], w, data['algorithm'], 2, True)

    print("\n|| Weight: " + w + " ||")
    if 'error' in result:
        print("No path found")
        return 1
    else:
        print("source: " + str(data['source_id']))
        print("destination: " + str(data['destination_id']))
        print("execution time: " + str(result['exec_time']))
        print("number of hops: " + str(result['hops']))
        print("total cost: " + str(result['cost']))
        print("total distance: " + str(result['distance']))
        print("total green area: " + str(result['green_area']))
        print("average pm10: " + str(result['pm10']))

    return 0


if __name__ == "__main__":
    with open("data/config.json", "r") as file:
        data = json.load(file)
    main()

# python routing.py -n neo4j://localhost:7687 -u neo4j -p password -mn prova -s 240 -d 29920