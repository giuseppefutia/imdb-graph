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

def parse_year(year_str):
    try:
        return int(year_str)
    except:
        return None

def split_list(val):
    if pd.isna(val) or val == '\\N':
        return []
    return val.split(',')

class NameImporter():
    
    def __init__(self):
        self.file_name = "data/name.basics.tsv"
        self.db = kuzu.Database('./imdb2')
        self.conn = kuzu.Connection(self.db)

    def import_people(self, batch_size=1000):
        logging.info("Loading dataframe...")
        df = pd.read_csv(self.file_name, sep='\t', on_bad_lines='skip')

        logging.info("Transforming data...")
        df['birthYear'] = df['birthYear'].apply(parse_year)
        df['deathYear'] = df['deathYear'].apply(parse_year)
        df['primaryProfession'] = df['primaryProfession'].apply(split_list)
        df['knownForTitles'] = df['knownForTitles'].apply(split_list)

        logging.info("Starting batched import...")
        start_time = time.time()

        num_batches = (len(df) + batch_size - 1) // batch_size
        for i in tqdm(range(num_batches), desc="Importing People"):
            batch_df = df.iloc[i * batch_size:(i + 1) * batch_size].copy()
            
            query_person = """
            LOAD FROM batch_df
            MERGE (p:Person {id: nconst})
            SET p.name = primaryName,
                p.birthYear = birthYear,
                p.deathYear = deathYear
            """

            query_profession = """
            LOAD FROM batch_df
            UNWIND primaryProfession as profession
            MERGE (p:Person {id: nconst})
            MERGE (pr:Profession {name: profession})
            MERGE (p)-[:HAS_PROFESSION]->(pr)
            """

            query_titles = """
            LOAD FROM batch_df
            UNWIND knownForTitles as title
            MERGE (p:Person {id: nconst})
            MERGE (t:Title {id: title})
            MERGE (p)-[:KNOWN_FOR]->(t)
            """

            #self.conn.execute(query_person)
            #self.conn.execute(query_profession)
            self.conn.execute(query_titles)

        duration = time.time() - start_time
        logging.info(f"Merge completed in {duration:.2f} seconds.")

if __name__ == '__main__':
    people_importer = NameImporter()
    people_importer.import_people()

    response = people_importer.conn.execute("MATCH (n) RETURN COUNT(n)")
    while response.has_next():
        print("Graph status:")
        print(response.get_next())
    
    response = people_importer.conn.execute("MATCH ()-[r]->() RETURN COUNT(r)")
    while response.has_next():
        print("Total relationships:", response.get_next()[0])
