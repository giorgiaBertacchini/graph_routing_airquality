<div align="center">
  <h1 align="center">Air-Aware Walking Routes </h1>
  <h3 align="center">Air Quality Analysis and Path Finding on Modena City</h3>
  <p align="center">
  A smarter, healthier way  to navigate the city!
  </p>
</div>

## Introduction
This project aims to find the best walking route within the city of Modena, considering not only the distance but also air quality. Air quality is calculated based on the PM10 values detected by sensors scattered throughout the city.

## Project Steps:
1. **Data Collection**: Data related to the position of sensors (latitude and longitude coordinates) and the PM10 values are provided through CSV files:
   * One CSV file containing the geographical coordinates of the sensors.
   * Another CSV file containing the current PM10 values recorded by the sensors.
2. **Air Quality Interpolation**: To have a city-wide map of air quality (rather than just point data from the sensors), the PM10 data is used to create a raster through the IDW (Inverse Distance Weighting) interpolation.
3. **Graph Construction in Neo4j**: 
   * The city is represented as a graph in Neo4j, where the nodes are the intersections of the streets and the edges are the streets themselves.
   * Each edge in the graph is associated with an average PM10 value, calculated from the interpolated data.
4. **Path Finding Algorithm**: 
   * Neo4j's pathfinding algorithms (Dijkstra, A* and Yen) are used to find the best walking route, minimizing the average PM10 value along the path.
   * It is possible to calculate the best route by considering either air quality (PM10) alone or a combination of distance and PM10. In the latter case, a weighted average of distance and PM10 is calculated, and the path search is performed based on this value.

## Interactions with Neo4j and QGIS
To better manage the communication with Neo4j, the `graph_bridge.py` file contains the functions to interact with the database.

To have a visual representation of the graph and the outputs, this project create also files designated for QGIS application.

## Installation 
For this project is used `Python 3.11`.

For the graph database is used `Neo4j`. 
Remember to configure the connection to the database in the `config.json` file, in the `neo4j_URL`, `neo4j_user`, and `neo4j_pwd` fields.

The required packages are listed in the `requirements.txt` file.

``` bash
pip install -r requirements.txt
```

## Usage
### Prepare the Data
Make sure you have two CSV files:
* A file with the sensors' geographical coordinates, with columns `id_station`, `name`, `latitude`, and `longitude`.
* A file with the PM10 values recorded by the sensors, with at least the columns `id_station`, `value`.
In the `data` folder, you can find two example files that can be used to test the project.

You can set the paths to the CSV files in the `config.json` file, in the `sensor_coords_path` and `measures_path` fields.

These files should be used to generate the air quality raster.

### Create the Air Quality Raster
`interpolation.py` is used to create the air quality raster that covers the entire city of Modena, through the IDW interpolation method.
You can personalize the interpolation parameters in the `config.json` file, in the `idw` field, with the `power`, `radius1` and `radius2` fields.

The script will generate a GeoTIFF file with the interpolated data, and you can change also the output file path in the `raster_path` field. 

The raster is saved in the `interpolations` folder and can be loaded in QGIS to visualize the air quality map.

### Export the Graph
`export_graph.py` is used to export the graph in csv, in particular retrieve from Neo4j the road junctions and the roads in two CSV files, easy to load in QGIS.

### Populate the Graph in Neo4j
`merge_airquality_footpath.py` is used to populate the graph in Neo4j on the streets with their average PM10 values.

The python file retrieves the coordinates of road junctions from Neo4j and, for each corresponding edge, extracts the values from the raster. 
To obtain a more aggregated result, it calculates the average PM10 values within a buffer of configurable width, as specified in the `config.json` file in the `buffer_size` field.

## Search for the Path
`footway_routing.py` is the script that allows you to search for the best walking route in the city of Modena.
It responds to the parameters set in the `routing_config.json` file, containing the routing parameters, such as:
* `source_id` and `destination_id`: the IDs of the starting and ending route junctions.
* `algorithm`: the pathfinding algorithm to use (Dijkstra, A*, or Yen).
* `weight`: the weight to minimize in the path search (PM10, distance, effective PM10 or a combination of both).
* `top_k`: the number of paths to return in the case of the Yen algorithm.
* `combined_weight`: the parameter to balance the weight between PM10 and distance in the case of the combined weight.

The script generates for each path found a GeoJSON file that can be loaded in QGIS to visualize the path on the map, and save the path in the `routing` folder.

### Weights
The `weight` parameter can be set to:
* `PM10`: to minimize the average PM10 value along the path.
* `distance`: to minimize the distance.
* `effective_PM10`: to minimize the effective PM10 value, a value more representative of the air quality along the path, that considers the distance and the PM10 value.
* `combined_weight`: to minimize a weighted normalized average of distance and PM10, where the weight is set in the `combined_weight` parameter.


## Conclusion
This project is designed to be highly flexible and customizable, allowing it to adapt to any use case or future needs. 
Key parameters can be easily configured through the `config.json` and `routing_config.json` files.
