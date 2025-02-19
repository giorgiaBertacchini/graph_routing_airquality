import sys
from neo4j import GraphDatabase


class App:
    """
    Class that contains the methods to interact with the neo4j database
    """
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

        # Check if the connection is successful
        try:
            with self.driver:
                self.driver.verify_connectivity()
        except Exception as e:
            print(f"Connection failed: {e}")
            sys.exit(1)

    def close(self):
        self.driver.close()

    def shorter_path_metrics(self, pairs):
        with self.driver.session() as session:
            result = session.write_transaction(self._shorter_path_metrics, pairs)
            return result

    @staticmethod
    def _shorter_path_metrics(tx, pairs):
        """
        Query to get the cost, danger and distance of the shortest path between the pairs of nodes
        """
        query = """ UNWIND %s as pairs
                    CALL {
                        MATCH (n:RoadJunction {id: pairs[0]})-[r:ROUTE]->(m:RoadJunction {id: pairs[1]})
                        RETURN r ORDER BY r.distance ASC LIMIT 1
                    }
                    RETURN SUM(r.green_area) AS green_area, SUM(r.distance) AS distance""" % pairs
        result = tx.run(query)
        return result.values()

    def get_coordinates(self, final_path):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_coordinates, final_path)
            return result

    @staticmethod
    def _get_coordinates(tx, final_path):
        """
        Query to get the list of coordinates of the path nodes
        """
        query = """
        UNWIND %s as p
        MATCH (n:RoadJunction {id: p})
        RETURN collect([n.lon, n.lat])""" % final_path
        result = tx.run(query)
        return result.values()

    def drop_all_projections(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._drop_all_projections)
            return result

    @staticmethod
    def _drop_all_projections(tx):
        """
        Query to take the list of all the graphs and drop them
        """
        result = tx.run("""CALL gds.graph.list() YIELD graphName
                    CALL gds.graph.drop(graphName)
                    YIELD database
                    RETURN 'dropped ' + graphName""")
        return result.values()

    def dijkstra_path(self, source, target, weight_property):
        with self.driver.session() as session:
            result = session.write_transaction(self._dijkstra_path, source, target, weight_property)
            return result

    @staticmethod
    def _dijkstra_path(tx, source, target, weight_property):
        sub_graph_query = ("""
            CALL gds.graph.project('subgraph_routing', 
                ['RoadJunction'], ['ROUTE'], 
                {nodeProperties: ['lat', 'lon'], 
                relationshipProperties: ['%s']});
        """) % weight_property

        tx.run(sub_graph_query)

        query = """
        MATCH (s:RoadJunction {id: $source})
        MATCH (t:RoadJunction {id: $target})
        CALL gds.shortestPath.dijkstra.stream('subgraph_routing', {
            sourceNode: s, 
            targetNode: t,
            relationshipWeightProperty: $weight_property
        })
        YIELD index, sourceNode, targetNode, totalCost, nodeIds, path
        with  [nodeId IN nodeIds | gds.util.asNode(nodeId).id] AS nodes_path, totalCost,path as p
        unwind relationships(p) as n with startNode(n).id as start_node,endNode(n).id as end_node,nodes_path,totalCost
        match (fn:RoadJunction{id:start_node})-[r:ROUTE]->(fn2:RoadJunction{id:end_node})
        return nodes_path, totalCost, sum(r.distance) as total_distance, sum(r.green_area) as total_green_area, avg(r.pm10)
        """

        result = tx.run(query, source=source, target=target, weight_property=weight_property)

        tx.run("""call gds.graph.drop('subgraph_routing')""")

        return result.values()

    def a_star_path(self, source, target, weight_property):
        with self.driver.session() as session:
            result = session.write_transaction(self._a_star_path, source, target, weight_property)
            return result

    @staticmethod
    def _a_star_path(tx, source, target, weight_property):
        sub_graph_query = ("""
            CALL gds.graph.project('subgraph_routing', 
                ['RoadJunction'], ['ROUTE'], 
                {nodeProperties: ['lat', 'lon'], 
                relationshipProperties: ['%s']});
        """) % weight_property

        tx.run(sub_graph_query)

        query = """        
        MATCH (s:RoadJunction {id: $source})
        MATCH (t:RoadJunction {id: $target})
        CALL gds.shortestPath.astar.stream('subgraph_routing', {
            sourceNode: s,
            targetNode: t,
            latitudeProperty: 'lat',
            longitudeProperty: 'lon',
            relationshipWeightProperty: $weight_property
        })
        YIELD index, sourceNode, targetNode, totalCost, nodeIds, path
        with  [nodeId IN nodeIds | gds.util.asNode(nodeId).id] AS nodes_path, totalCost,path as p
        unwind relationships(p) as n with startNode(n).id as start_node,endNode(n).id as end_node,nodes_path,totalCost
        match (fn:RoadJunction{id:start_node})-[r:ROUTE]->(fn2:RoadJunction{id:end_node})
        return nodes_path, totalCost, sum(r.distance) as total_distance, sum(r.green_area) as total_green_area, avg(r.pm10)
        """

        result = tx.run(query, source=source, target=target, weight_property=weight_property)

        tx.run("""call gds.graph.drop('subgraph_routing')""")

        return result.values()

    def top_k_paths(self, source, target, weight_property, k):
        with self.driver.session() as session:
            result = session.write_transaction(self._top_k_paths, source, target, weight_property, k)
            return result

    @staticmethod
    def _top_k_paths(tx, source, target, weight_property, k):
        sub_graph_query = ("""
            CALL gds.graph.project('subgraph_routing', 
                ['RoadJunction'], ['ROUTE'], 
                {nodeProperties: ['lat', 'lon'], 
                relationshipProperties: ['%s']});
        """) % weight_property

        tx.run(sub_graph_query)

        query = """
        MATCH (s:RoadJunction {id: $source})
        MATCH (t:RoadJunction {id: $target})
        CALL gds.shortestPath.yens.stream('subgraph_routing', {
            sourceNode: s, 
            targetNode: t,
            k: $k,
            relationshipWeightProperty: $weight_property
        })
        YIELD index, sourceNode, targetNode, totalCost, nodeIds, path
        with  [nodeId IN nodeIds | gds.util.asNode(nodeId).id] AS nodes_path, totalCost,path as p
        unwind relationships(p) as n with startNode(n).id as start_node,endNode(n).id as end_node,nodes_path,totalCost
        match (fn:RoadJunction{id:start_node})-[r:ROUTE]->(fn2:RoadJunction{id:end_node})
        return nodes_path, totalCost, sum(r.distance) as total_distance, sum(r.green_area) as total_green_area, avg(r.pm10)
        """

        result = tx.run(query, source=source, target=target, weight_property=weight_property, k=k)

        tx.run("""call gds.graph.drop('subgraph_routing')""")

        return result.values()

    def get_edges_endpoints(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_edges_endpoints)
            return result

    @staticmethod
    def _get_edges_endpoints(tx):
        query = """
        MATCH (s:RoadJunction)-[r:ROUTE]->(d:RoadJunction)
        WHERE s.id < d.id
        RETURN s.id AS source, d.id AS destination, 
        s.lon AS source_lon, s.lat AS source_lat, 
        d.lon AS destination_lon, d.lat AS destination_lat
        """
        result = tx.run(query)
        return result.values()

    def add_edge_air_quality_in_bulk(self, id_pairs, mean_air_quality_values):
        with self.driver.session() as session:
            result = session.write_transaction(self._add_edge_air_quality_in_bulk, id_pairs, mean_air_quality_values)
            return result

    @staticmethod
    def _add_edge_air_quality_in_bulk(tx, id_pairs, mean_air_quality_values):
        query = """
        UNWIND $pairs AS pair
        MATCH (s:RoadJunction)-[r:ROUTE]->(d:RoadJunction)
        WHERE s.id = pair.source AND d.id = pair.destination
        SET r.pm10 = pair.mean_air_quality
        WITH r, d
        MATCH (d)-[r2:ROUTE]->(s)
        SET r2.pm10 = r.pm10
        RETURN r.pm10
        """
        result = tx.run(query, pairs=[{'source': pair[0], 'destination': pair[1], 'mean_air_quality': mean_air_quality}
                                      for pair, mean_air_quality in zip(id_pairs, mean_air_quality_values)])
        return result.values()

    def get_extreme_lon_lat(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_extreme_lon_lat)
            return result

    @staticmethod
    def _get_extreme_lon_lat(tx):
        query = """
        MATCH (n:RoadJunction)
        RETURN min(n.lon) as min_lon, max(n.lon) as max_lon, min(n.lat) as min_lat, max(n.lat) as max_lat
        """
        result = tx.run(query)
        return result.values()[0]

    def get_road_junction_nodes(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_road_junction_nodes)
            return result

    @staticmethod
    def _get_road_junction_nodes(tx):
        query = """
        MATCH (n:RoadJunction)
        RETURN n.id as id, n.lon as lon, n.lat as lat
        """
        result = tx.run(query)
        return result.values()

    def get_road_edges(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_road_edges)
            return result

    @staticmethod
    def _get_road_edges(tx):
        query = """
        MATCH (s:RoadJunction)-[r:ROUTE]->(d:RoadJunction)
        RETURN s.id AS source, d.id AS target, 
        s.lon AS source_lon, s.lat AS source_lat, 
            d.lon AS target_lon, d.lat AS target_lat, 
            r.name AS name, r.highway AS highway,
            r.distance AS distance, r.green_area AS green_area, r.pm10 AS pm10
        """
        result = tx.run(query)
        return result.values()

    def get_extreme_edge_properties(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_extreme_edge_properties)
            return result

    @staticmethod
    def _get_extreme_edge_properties(tx):
        query = """
        MATCH (s:RoadJunction)-[r:ROUTE]->(d:RoadJunction)
        RETURN min(r.distance) as min_distance, max(r.distance) as max_distance, 
        min(r.green_area) as min_green_area, max(r.green_area) as max_green_area, 
        min(r.pm10) as min_pm10, max(r.pm10) as max_pm10
        """
        result = tx.run(query)
        return result.values()

    def add_combined_property(self, weight):
        with self.driver.session() as session:
            result = session.write_transaction(self._add_combined_property, weight)
            return result

    @staticmethod
    def _add_combined_property(tx, parameters):
        query = """
        CALL {
            MATCH (s:RoadJunction)-[r:ROUTE]->(d:RoadJunction)
            RETURN 
                max(r.distance) AS max_distance, max(r.effective_pm10) AS max_effective_pm10
        }
        MATCH (s:RoadJunction)-[r:ROUTE]->(d:RoadJunction)
        WHERE s.id < d.id
        WITH 
            r, s, d, max_distance, max_effective_pm10,
            (r.distance - $min_distance) / (max_distance - $min_distance) AS normalized_distance,
            (r.effective_pm10 - $min_pm10) / (max_effective_pm10 - $min_pm10) AS normalized_pm10
        WITH r, s, d, 
            normalized_distance^$distance_power AS powered_distance,
            normalized_pm10^$pm10_power AS powered_pm10
        WITH r, s, d,
            ($distance_ratio * powered_distance + $pm10_ratio * powered_pm10) AS weighted_average
        SET r.combined_weight = weighted_average
        
        WITH r, s, d, weighted_average
        MATCH (d)-[r2:ROUTE]->(s)  
        SET r2.combined_weight = weighted_average

        RETURN r, r.combined_property
        """

        result = tx.run(query, parameters=parameters)
        return result.values()

    def get_distances_and_effective_pm10(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_distances_and_effective_pm10)
            return result

    @staticmethod
    def _get_distances_and_effective_pm10(tx):
        query = """
        MATCH (s:RoadJunction)-[r:ROUTE]->(d:RoadJunction)
        RETURN r.distance AS distance, r.effective_pm10 AS effective_pm10
        """
        result = tx.run(query)
        return result.values()

    # TODO to delete
    def add_new_prop(self, weight):
        with self.driver.session() as session:
            result = session.write_transaction(self._add_new_prop, weight)
            return result

    @staticmethod
    def _add_new_prop(tx, parameters):
        query = """
        MATCH (s:RoadJunction)-[r:ROUTE]->(d:RoadJunction)
        WHERE s.id < d.id
        WITH r, s, d,
            CASE 
                WHEN r.distance <= $min_distance THEN 0
                WHEN r.distance >= $max_distance THEN 1
                ELSE (r.distance - $min_distance) / ($max_distance - $min_distance)
            END AS normalized_distance,             
            CASE 
                WHEN r.effective_pm10 <= $min_pm10 THEN 0
                WHEN r.effective_pm10 >= $max_pm10 THEN 1
                ELSE (r.effective_pm10 - $min_pm10) / ($max_pm10 - $min_pm10)
            END AS normalized_eff_pm10
        WITH r, s, d, 
            normalized_distance^$distance_power AS powered_distance,
            normalized_eff_pm10^$pm10_power AS powered_pm10
        WITH r, s, d,
            ($distance_ratio * powered_distance + $pm10_ratio * powered_pm10) AS weighted_average
        SET r.combined_weight = weighted_average
    
        WITH r, s, d, weighted_average
        MATCH (d)-[r2:ROUTE]->(s)  
        SET r2.combined_weight = weighted_average
    
        RETURN r, r.combined_property
        """

        result = tx.run(query, parameters=parameters)
        return result.values()

    def add_effective_pm10(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._add_effective_pm10)
            return result

    @staticmethod
    def _add_effective_pm10(tx):
        query = """
        MATCH (s:RoadJunction)-[r:ROUTE]->(d:RoadJunction)
        WHERE s.id < d.id
        WITH r, s, d,
            (r.pm10 * 2000) / (r.distance + 200) AS effective_pm10
        
        SET r.effective_pm10 = effective_pm10
    
        WITH r, s, d, effective_pm10
        MATCH (d)-[r2:ROUTE]->(s)  
        SET r2.effective_pm10 = effective_pm10
    
        RETURN r, r.effective_pm10
        """

        result = tx.run(query)
        return result.values()
