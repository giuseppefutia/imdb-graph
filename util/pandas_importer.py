import logging

import pandas as pd

from util.base_importer import BaseImporter

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class PandasImporter(BaseImporter):
    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)
    
    @staticmethod
    def get_rows(file, transformations):
        logging.info(f"File loading...")
        dataframe = pd.read_csv(file, sep='\t')
        logging.info(f"Number of rows: {len(dataframe)}")

        logging.info("Applying transformations...")
        if transformations:
            for column, func in transformations.items():
                if column in dataframe.columns:
                    dataframe[column] = func(dataframe[column])
        
        logging.info(f"Start processing...")
        for row in dataframe.itertuples(index=False, name=None):
            yield dict(zip(dataframe.columns, row))
