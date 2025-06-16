import kuzu
import pandas as pd
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class NameImporter():
    
    def __init__(self):
        self.file_name = "../data/name.basics.tsv"
        self.db = kuzu.Database('./imdb')
        self.conn = kuzu.Connection(self.db)
    
    def import_people(self):
        logging.info("Loading dataframe...")
        df = pd.read_csv(self.file_name, sep='\t', on_bad_lines='skip')

        print()
        logging.info("Dataframe size...")
        print(len(df))
        
        print()
        logging.info("Data sample...")
        print(df.head(3))
        
        query = """
        LOAD FROM df
        MERGE (person:Person {id: nconst})
        SET person.name = primaryName
        """
        
        print()
        logging.info("Merging graph...")
        start_time = time.time()
        self.conn.execute(query)
        duration = time.time() - start_time
        logging.info(f"Merge completed in {duration:.2f} seconds.")

if __name__ == '__main__':
    people_importer = NameImporter()
    people_importer.import_people()
    response = people_importer.conn.execute(
        """
        MATCH (n)
        RETURN COUNT(n)
        """
    )
    while response.has_next():
        print("Graph status:")
        print(response.get_next())
