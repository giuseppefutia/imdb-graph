import logging

import pandas as pd
from functools import reduce
import kuzu

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class TitleImporter():
    
    def __init__(self):
        self.db = kuzu.Database("./imdb-full")
        self.conn = kuzu.Connection(self.db)
        self.create_title_table()
    
    def create_title_table(self):
        table_query = """
        CREATE NODE TABLE IF NOT EXISTS Title(id STRING, 
                                              name STRING, 
                                              originalName STRING,
                                              startYear INT64,
                                              endYear INT64,
                                              duration INT64,
                                              averageRating FLOAT8,
                                              numVotes INT64,
                                              ordering STRING[],
                                              alternateNames STRING[],
                                              regions STRING[],
                                              languages STRING[],
                                              types STRING[],
                                              attributes STRING[],
                                              areOriginalNames STRING[],
                                              PRIMARY KEY (id))
        """
        self.conn.execute(table_query)
        res = self.conn.execute("""CALL TABLE_INFO('Title') RETURN *""")
        while res.has_next():
            print("Title nodes metadata:", res.get_next())
    
    def process_title_files(self):
        logging.info("Loading data...")
        basics = pd.read_csv("data/title.basics.tsv", sep="\t", na_values="\\N")
        akas = pd.read_csv("data/title.akas.tsv", sep="\t", na_values="\\N")
        ratings = pd.read_csv("data/title.ratings.tsv", sep="\t", na_values="\\N")

        logging.info("Renaming columns...")
        basics.rename(columns={
            "tconst": "id",
            "primaryTitle": "name",
            "originalTitle": "originalName",
            "runtimeMinutes": "duration"
        }, inplace=True)

        akas.rename(columns={
            "titleId": "id",
            "title": "alternateNames",
            "language": "languages",
            "region": "regions",
            "isOriginalTitle": "areOriginalNames"
        }, inplace=True)

        ratings.rename(columns={"tconst": "id"}, inplace=True)

        logging.info("Dropping unused columns...")
        basics.drop(columns=["titleType", "isAdult", "genres"], inplace=True, errors="ignore")

        logging.info("Preprocessing list fields...")
        list_fields = ['ordering', 'alternateNames', 'regions', 'languages', 'types', 'attributes', 'areOriginalNames']
        for df in [akas]:
            for col in list_fields:
                if col in df.columns:
                    df[col] = df[col].astype(str).replace("nan", pd.NA)

        logging.info("Merging dataframes...")
        merged_df = reduce(lambda left, right: pd.merge(left, right, on="id", how="outer"), [basics, akas, ratings])

        def listify(series):
            return series.dropna().tolist()

        logging.info("Aggregating...")
        agg_funcs = {
            'name': 'first',
            'originalName': 'first',
            'startYear': 'first',
            'endYear': 'first',
            'duration': 'first',
            'averageRating': 'first',
            'numVotes': 'first'
        }

        for col in list_fields:
            if col in merged_df.columns:
                agg_funcs[col] = listify

        grouped = merged_df.groupby("id", sort=False).agg(agg_funcs).reset_index()

        logging.info("Saving output...")
        grouped.to_csv("data/kuzu/title.csv", index=False)
    
    def post_process(self):
        input_path="data/kuzu/title.csv"
        output_path="data/kuzu/title_clean.csv"
        df = pd.read_csv(input_path)

        # Fix integer-looking float columns
        int_fields = ['startYear', 'endYear', 'duration', 'numVotes']
        for col in int_fields:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        # Fix rating column to be float with 1 decimal
        if 'averageRating' in df.columns:
            df['averageRating'] = pd.to_numeric(df['averageRating'], errors='coerce').round(1)

        # Optional: fix stringified lists (if they aren't parsed correctly)
        list_fields = ['ordering', 'alternateNames', 'regions', 'languages', 'types', 'attributes', 'areOriginalNames']
        for col in list_fields:
            if col in df.columns and df[col].dtype == object:
                df[col] = df[col].fillna("[]")

        # Save the cleaned version
        df.to_csv(output_path, index=False)

    
    def import_from_title_files(self):
        title_principals_query = """COPY Title FROM "data/kuzu/title_clean.csv" (header=true, ignore_errors=true);"""
        self.conn.execute(title_principals_query)
    
    def check_graph(self):
        res_1 = self.conn.execute(
            """
            MATCH (n:Title)
            RETURN COUNT(n)
            """
        )
        while res_1.has_next():
            print("Number of Title nodes:", res_1.get_next())
        
        res_2 = self.conn.execute(
            """
            MATCH (n:Title)
            RETURN n LIMIT 1
            """
        )
        while res_2.has_next():
            print("Sample output:", res_2.get_next())
    
    def import_data(self):
        # logging.info("Processing title.csv...")
        # self.process_title_files()
        #logging.info("Post processing title.csv...")
        #self.post_process()

        logging.info("Loading Title data from title.csv...")
        self.import_from_title_files()

        self.check_graph()

if __name__ == '__main__':
    person_importer = TitleImporter()
    person_importer.import_data()
