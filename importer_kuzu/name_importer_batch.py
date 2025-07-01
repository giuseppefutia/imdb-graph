import kuzu
import pandas as pd
import logging
import time
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class NameImporter():
    
    def __init__(self):
        self.file_name = "data/name.basics.tsv"
        self.db = kuzu.Database('./imdb')
        self.conn = kuzu.Connection(self.db)

    def import_people(self, batch_size=10000):
        logging.info("Loading dataframe...")
        df = pd.read_csv(self.file_name, sep='\t', on_bad_lines='skip')

        print()
        logging.info("Dataframe size...")
        print(len(df))

        print()
        logging.info("Data sample...")
        print(df.head(3))

        logging.info("Starting batched import...")
        start_time = time.time()

        num_batches = (len(df) + batch_size - 1) // batch_size

        for i in tqdm(range(num_batches), desc="Importing People"):
            batch_df = df.iloc[i * batch_size:(i + 1) * batch_size].copy()

            query = """
            LOAD FROM batch_df
            MERGE (person:Person {id: nconst})
            SET person.name = primaryName
            """
            self.conn.execute(query)

        duration = time.time() - start_time
        logging.info(f"Batched merge completed in {duration:.2f} seconds.")

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
