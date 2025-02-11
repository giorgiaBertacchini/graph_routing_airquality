import csv
from graph_bridge import App


def export_edges_to_csv():
    result = greeter.get_road_edges()

    with open("output/edges.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["source", "target", "source_lon", "source_lat", "target_lon", "target_lat",
                         "name", "highway", "distance", "green_area", "pm10"])
        for record in result:
            writer.writerow(list(record))


def export_road_junctions_to_csv():
    result = greeter.get_road_junction_nodes()

    with open("output/road_junctions.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["id", "lon", "lat"])
        for record in result:
            writer.writerow(list(record))


if __name__ == "__main__":
    greeter = App("neo4j://localhost:7687", "neo4j", "password")

    export_edges_to_csv()
    #export_road_junctions_to_csv()
