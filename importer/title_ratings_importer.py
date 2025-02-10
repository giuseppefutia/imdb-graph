import sys
import logging

from util.pandas_importer import PandasImporter
from util.file_utils import get_validated_file

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class TitleRatingsImporter(PandasImporter):
    def __init__(self, argv):
        super().__init__(argv=argv)    
    
    def set_constraints(self):
        queries = [
            "CREATE CONSTRAINT titleId IF NOT EXISTS FOR (node:Title) REQUIRE node.id IS UNIQUE"
        ]
        for q in queries:
            with self._driver.session(database=self.database) as session:
                session.run(q)
    
    def import_ratings(self, ratings_file):
        query = """
        UNWIND $batch as item
        MERGE (title:Title {id: item.tconst})
        SET title.sources = apoc.coll.toSet(coalesce(title.sources, []) + "title.ratings.tsv"), 
        title.averageRating = toInteger(item.averageRating),
            title.numVotes = toInteger(item.numVotes)
        """
        size = self.get_csv_size(ratings_file)
        self.batch_store(query, self.get_rows(ratings_file, transformations={}), size=size)

if __name__ == '__main__':
    ratings_importer = TitleRatingsImporter(argv=sys.argv[1:])
    ratings_file = get_validated_file(ratings_importer, "title.ratings.tsv")

    logging.info("Setting constraints...")
    ratings_importer.set_constraints()

    logging.info("Importing title ratings records...")
    ratings_importer.import_ratings(ratings_file)
