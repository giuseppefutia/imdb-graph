import configparser
import logging

import pandas as pd

from util.graphdb_base import GraphDBBase

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

def execute_query(query, parameters, gd):
    with gd._driver.session(database=gd._database) as session:
        r = session.run(query, parameters)
        return r.data()

class DataQualityAnalyzer():

    def __init__(self):
        config_file = "config.ini"
        config = configparser.ConfigParser()
        config.read(config_file)
        neo4j_config = config["neo4j"]
        database = neo4j_config.get("database")
        self.gd = GraphDBBase()
        self.gd._database = database
    
    def prepare_data_to_check_data_quality(self):
        query = """
        CALL apoc.periodic.iterate(
            "MATCH (n:Title) WHERE n.sources IS NOT NULL RETURN n",
            "UNWIND n.sources AS source 
            CALL apoc.create.setProperty(n, replace(source, '.', '_'), true) YIELD node RETURN node",
            {batchSize: 1000, parallel: true});
        """
        execute_query(query, {}, self.gd)
    
    def check_title_basics_consistency(self):
        df = pd.read_csv("data/title.basics.tsv", sep='\t')
        unique_tconst_count = df["tconst"].nunique()
        print("Number of unique title ids in title.basics.tsv", unique_tconst_count)
        
        query_01 = """MATCH (n:Title) RETURN COUNT (DISTINCT n) as count"""
        unique_title_in_graph = execute_query(query_01, {}, self.gd)[0]["count"]
        print("Number of unique titles in the graph", unique_title_in_graph)

        query_02 = """MATCH (n:Title) WHERE n.title_basics_tsv IS NULL RETURN n"""
        nodes_from_other_sources = execute_query(query_02, {}, self.gd)
        print("Nodes generated from sources other than title.basics.tsv", nodes_from_other_sources)

if __name__ == '__main__':
    analyzer = DataQualityAnalyzer()
    logging.info("Preparing graph to check consistency...")
    analyzer.prepare_data_to_check_data_quality()
    logging.info("Checking title basic consistency...")
    analyzer.check_title_basics_consistency()
