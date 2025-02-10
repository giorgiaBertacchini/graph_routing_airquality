import csv
from graph_bridge import App


def export_edges_to_csv():
    result = greeter.get_road_edges()

    with open("output/edges.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["source", "target", "source_lon", "source_lat", "target_lon", "target_lat",
                         "distance", "green_area", "pm10"])
        for record in result:
            writer.writerow([
                record["source"], record["target"],
                record["source_lon"], record["source_lat"],
                record["target_lon"], record["target_lat"],
                record["distance"], record["green_area"], record["pm10"]
            ])


greeter = App("neo4j://localhost:7687", "neo4j", "password")

export_edges_to_csv()
