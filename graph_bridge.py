from neo4j import GraphDatabase


class App:
    """
    Class that contains the methods to interact with the neo4j database
    """
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

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
        MATCH (n:RoadJunction{id: p}) 
        RETURN collect([n.lat,n.lon])""" % final_path
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

    def shortest_path(self, source, target):
        with self.driver.session() as session:
            result = session.write_transaction(self._shortest_path, source, target)
            return result

    @staticmethod
    def _shortest_path(tx, source, target):
        """
        Create the subgraph with the nodes and the relationships needed to find the shortest path
        and return the shortest path between the source and the target
        """
        # Create the subgraph with:
        # node FootNode with the properties lat and lon and relationship FOOT_ROUTE with the properties comfort_cost
        tx.run("""CALL gds.graph.project('subgraph_routing', 
        ['RoadJunction'], ['ROUTE'], 
        {nodeProperties: ['lat', 'lon'], relationshipProperties: ['distance']});""")

        # Query to find the shortest path between the source and the target.
        #   YIELD index: index of the path,
        #   sourceNode, targetNode: source and target nodes,
        #   totalCost: total cost of the path,
        #   nodeIds: list of nodes in the path,
        #   path: path as a list of relationships
        query = """
        match (s:RoadJunction {id: '%s'})
        match (t:RoadJunction {id: '%s'})
        CALL gds.shortestPath.dijkstra.stream('subgraph_routing', {
                                                sourceNode: s, targetNode: t,
                                                relationshipWeightProperty: 'distance' })
                                            YIELD index, sourceNode, targetNode, totalCost, nodeIds, path
        with  [nodeId IN nodeIds | gds.util.asNode(nodeId).id] AS nodes_path, totalCost as total_distance
        UNWIND range(0, size(nodes_path) - 2) AS i
        MATCH (fn:RoadJunction {id: nodes_path[i]})-[r:ROUTE]->(fn2:RoadJunction {id: nodes_path[i+1]})
        return nodes_path, total_distance, sum(r.green_area) as total_green_area, avg(r.pm10)""" % (source, target)
        result = tx.run(query)

        tx.run("""call gds.graph.drop('subgraph_routing')""")

        return result.values()[0]

    # TODO to delete
    def single_weight_path(self, source, target, weight_property):
        with self.driver.session() as session:
            result = session.write_transaction(self._single_weight_path, source, target, weight_property)
            return result

    # TODO to delete
    @staticmethod
    def _single_weight_path(tx, source, target, weight_property):
        sub_graph_query = ("""
            CALL gds.graph.project('subgraph_routing', 
                ['RoadJunction'], ['ROUTE'], 
                {nodeProperties: ['lat', 'lon'], 
                relationshipProperties: ['%s']});
        """) % weight_property

        tx.run(sub_graph_query)

        query = """
        MATCH (s:RoadJunction {id: '%s'})
        MATCH (t:RoadJunction {id: '%s'})
        CALL gds.shortestPath.dijkstra.stream('subgraph_routing', {
            sourceNode: s, 
            targetNode: t,
            relationshipWeightProperty: '%s' 
        })
        YIELD nodeIds, totalCost, path
        WITH [nodeId IN nodeIds | gds.util.asNode(nodeId).id] AS nodes_path, totalCost
        UNWIND range(0, size(nodes_path) - 2) AS i
        MATCH (fn:FootNode {id: nodes_path[i]})-[r:FOOT_ROUTE]->(fn2:FootNode {id: nodes_path[i+1]})
        return nodes_path, total_distance, sum(r.green_area) as total_green_area, avg(r.pm10)
        """ % (source, target, weight_property)

        result = tx.run(query)

        tx.run("""call gds.graph.drop('subgraph_routing')""")

        return result.values()[0]

    def get_edges_endpoints(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_edges_endpoints)
            return result

    @staticmethod
    def _get_edges_endpoints(tx):
        query = """
        MATCH (s:RoadJunction)-[r:ROUTE]->(d:RoadJunction)
        RETURN id(s) AS source, id(d) AS destination, s.lon AS source_lon, s.lat AS source_lat, d.lon AS destination_lon, d.lat AS destination_lat
        """
        result = tx.run(query)
        return result.values()

    def add_edge_air_quality(self, id_pair, mean_air_quality):
        with self.driver.session() as session:
            result = session.write_transaction(self._add_edge_air_quality, id_pair, mean_air_quality)
            return result

    @staticmethod
    def _add_edge_air_quality(tx, id_pair, mean_air_quality):
        query = """
        MATCH (s:RoadJunction)-[r:ROUTE]->(d:RoadJunction)
        WHERE ID(s) = $source and ID(d) = $destination
        SET r.pm10 = $mean_air_quality
        RETURN r.pm10
        """
        result = tx.run(query, source=id_pair[0], destination=id_pair[1], mean_air_quality=mean_air_quality)

        return result.values()[0]

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


    # TODO serve?
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
        RETURN s.id AS source, d.id AS target, s.lon AS source_lon, s.lat AS source_lat, 
            d.lon AS target_lon, d.lat AS target_lat, r.distance AS distance, r.green_area AS green_area, r.pm10 AS pm10
        """
        result = tx.run(query)
        return result.values()
