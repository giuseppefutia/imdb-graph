import logging

import pandas as pd
import kuzu

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class PersonImporter():
    
    def __init__(self):
        self.db = kuzu.Database("./imdb-full")
        self.conn = kuzu.Connection(self.db)
        self.create_person_table()
    
    def create_person_table(self):
        table_query = """
        CREATE NODE TABLE IF NOT EXISTS Person(id STRING, name STRING, birthYear INT64, deathYear INT64, PRIMARY KEY (id))
        """
        self.conn.execute(table_query)
    
    def process_name_basics(self):
        name_basics = pd.read_csv("data/name.basics.tsv", sep="\t")
        name_basics = name_basics[['nconst', 'primaryName', 'birthYear', 'deathYear']]
        name_basics.replace('\\N', pd.NA, inplace=True)
        name_basics['primaryName'] = name_basics['primaryName'].str.replace(",", '', regex=False)
        name_basics.to_csv("data/kuzu/name.basics.csv", index=False)

    def import_from_name_basics(self):
        name_basics_query = """COPY Person FROM "data/kuzu/name.basics.csv" (header=true);"""
        self.conn.execute(name_basics_query)
    
    def process_title_crew(self):
        title_crew = pd.read_csv("data/title.crew.tsv", sep="\t")
        title_crew.replace('\\N', pd.NA, inplace=True)
        combined = pd.concat([
            title_crew['directors'].dropna().str.split(',').explode(),
            title_crew['writers'].dropna().str.split(',').explode()
        ])
        # Drop duplicates and reset index
        nconst_df = combined.dropna().drop_duplicates().reset_index(drop=True).to_frame(name='nconst')
        
        nconst_df['primaryName'] = pd.NA
        nconst_df['birthYear'] = pd.NA
        nconst_df['deathYear'] = pd.NA
        nconst_df.to_csv("data/kuzu/title.crew.csv", index=False)

    def import_from_title_crew(self):
        title_crew_query = """COPY Person FROM "data/kuzu/title.crew.csv" (header=true);"""
        self.conn.execute(title_crew_query)

    def process_title_principals(self):
        title_principals = pd.read_csv("data/title.principals.tsv", sep="\t")
        title_principals = title_principals[['nconst']]
        title_principals['primaryName'] = pd.NA
        title_principals['birthYear'] = pd.NA
        title_principals['deathYear'] = pd.NA
        title_principals.to_csv("data/kuzu/title.principals.csv", index=False)

    def import_from_title_principals(self):
        title_principals_query = """COPY Person FROM "data/kuzu/title.principals.csv" (header=true);"""
        self.conn.execute(title_principals_query)
    
    def check_name_basics(self):
        res_1 = self.conn.execute(
        """
        MATCH (n:Person)
        RETURN COUNT(n)
        """
    )
        while res_1.has_next():
            print("Number of Person nodes:", res_1.get_next())

    def import_data(self):
        logging.info("Processing name.basics.tsv...")
        self.process_name_basics()

        logging.info("Loading Person data from name.basics.csv...")
        self.import_from_name_basics()

        self.check_name_basics()

        """
        logging.info("Processing title.crew.tsv...")
        self.process_title_crew()

        logging.info("Loading Person data from title.crew.csv...")
        self.import_from_title_crew()

        self.check_name_basics()

        logging.info("Processing title.principals.tsv...")
        self.process_title_principals()

        logging.info("Loading Person data from title.principals.csv...")
        self.import_from_title_crew()

        self.check_name_basics()
        """

def transform_data(dataframe, transformations):
    for column, func in transformations.items():
        if column in dataframe.columns:
            dataframe[column] = func(dataframe[column])
    return dataframe

if __name__ == '__main__':
    person_importer = PersonImporter()
    person_importer.import_data()
