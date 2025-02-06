from neo4j import GraphDatabase


class App:
    """
    Class that contains the methods to interact with the neo4j database
    """
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def drop_all_projections(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._drop_all_projections)
            return result

    @staticmethod
    def _drop_all_projections(tx):
        """
        Take the list of all the graphs and drop them
        """
        result = tx.run("""CALL gds.graph.list() YIELD graphName
                    CALL gds.graph.drop(graphName)
                    YIELD database
                    RETURN 'dropped ' + graphName""")
        return result.values()

    def get_edges(self):
        with self.driver.session() as session:
            # write_transaction: to execute a write query on the database
            result = session.write_transaction(self._get_edges)
            return result

    @staticmethod
    def _get_edges(tx):
        query = """
        MATCH (s:FootNode)-[r:FOOT_ROUTE]->(d:FootNode)
        RETURN id(s) AS source, id(d) AS destination;
        """
        result = tx.run(query)
        return result.values()

    def add_edge_air_quality(self, coordinate_pair, mean_air_quality):
        with self.driver.session() as session:
            result = session.write_transaction(self._add_edge_air_quality, coordinate_pair, mean_air_quality)
            return result

    @staticmethod
    def _add_edge_air_quality(tx, coordinate_pair, mean_air_quality):
        query = """
        MATCH (s:FootNode {id: %s})-[r:FOOT_ROUTE]->(d:FootNode {id: %d})
        SET r.air_quality = $mean_air_quality
        """ % (coordinate_pair[0], coordinate_pair[1])
        tx.run(query, source=coordinate_pair[0], destination=coordinate_pair[1], mean_air_quality=mean_air_quality)
        return True
