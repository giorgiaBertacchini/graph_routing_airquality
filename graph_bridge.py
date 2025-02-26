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
        return nodes_path, totalCost, sum(r.distance) as total_distance, sum(r.green_area) as total_green_area, 
        avg(r.pm10), sum(r.pm10_metre) as total_pm10_metre, sum(r.inv_ga_metre) as total_inv_ga_metre
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
        return nodes_path, totalCost, sum(r.distance) as total_distance, sum(r.green_area) as total_green_area, 
        avg(r.pm10), sum(r.abs_altitude_diff) as total_altitude_diff
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
        return nodes_path, totalCost, sum(r.distance) as total_distance, sum(r.green_area) as total_green_area, 
        avg(r.pm10), sum(r.abs_altitude_diff) as total_altitude_diff
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
        WHERE s.id < d.id AND s.id = pair.source AND d.id = pair.destination
        SET r.pm10 = pair.mean_air_quality
        WITH s, d, pair
        MATCH (d)-[r2:ROUTE]->(s)
        SET r2.pm10 = pair.mean_air_quality
        RETURN pair.mean_air_quality
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
            r.name AS name,
            r.distance AS distance, r.green_area AS green_area, r.pm10 AS pm10,
            r.pm10_metre AS pm10_metre, r.inv_ga_metre AS inv_ga_metre, r.combined_weight AS combined_weight
        """
        result = tx.run(query)
        return result.values()

    def get_distances(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_distances)
            return result

    @staticmethod
    def _get_distances(tx):
        query = """
        MATCH (s:RoadJunction)-[r:ROUTE]->(d:RoadJunction)
        RETURN r.distance
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
                max(r.pm10_metre) AS max_pm10_metre, 
                max(r.inv_ga_metre) AS max_inv_ga_metre,
                min(r.pm10_metre) AS min_pm10,
                min(r.inv_ga_metre) AS min_inv_ga
        }
        
        MATCH (s:RoadJunction)-[r:ROUTE]->(d:RoadJunction)
        WHERE s.id < d.id        
        WITH 
            r, s, d, max_pm10_metre, max_inv_ga_metre, min_pm10, min_inv_ga,
            (r.pm10_metre - min_pm10) / (max_pm10_metre - min_pm10) AS normalized_pm10,
            (r.inv_ga_metre - min_inv_ga) / (max_inv_ga_metre - min_inv_ga) AS normalized_inv_ga
                        
        WITH 
            r, s, d,
            (($pm10_ratio * normalized_pm10) + ($inv_green_area_ratio * normalized_inv_ga)) AS weighted_average
        
        SET r.combined_weight = weighted_average
        
        WITH r, s, d, weighted_average
        MATCH (d)-[r2:ROUTE]->(s)  
        SET r2.combined_weight = weighted_average
        
        RETURN r, r.combined_weight
        """

        result = tx.run(query, parameters=parameters)
        return result.values()

    def add_pm10_metre(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._add_pm10_metre)
            return result

    @staticmethod
    def _add_pm10_metre(tx):
        query = """
        MATCH (s:RoadJunction)-[r:ROUTE]->(d:RoadJunction)
        WHERE s.id < d.id
        WITH r, s, d,
            (r.pm10) / (r.distance) AS pm10_per_metre
        
        SET r.pm10_metre = pm10_per_metre
    
        WITH r, s, d, pm10_per_metre
        MATCH (d)-[r2:ROUTE]->(s)  
        SET r2.pm10_metre = pm10_per_metre
    
        RETURN r, r.pm10_metre
        """

        result = tx.run(query)
        return result.values()

    def add_inv_green_area_metre(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._add_inv_green_area_metre)
            return result

    @staticmethod
    def _add_inv_green_area_metre(tx):
        query = """
        MATCH (s:RoadJunction)-[r:ROUTE]->(d:RoadJunction)
        WHERE s.id < d.id
        WITH r, s, d,
            1 / ((r.green_area + 0.0000001) * r.distance) AS inverse_green_area_metre
        
        SET r.inv_ga_metre = inverse_green_area_metre
    
        WITH r, s, d, inverse_green_area_metre
        MATCH (d)-[r2:ROUTE]->(s)  
        SET r2.inv_ga_metre = inverse_green_area_metre
    
        RETURN r, r.inv_ga_metre
        """

        result = tx.run(query)
        return result.values()
