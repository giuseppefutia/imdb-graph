import sys
import logging

from util.pandas_importer import PandasImporter
from util.data_transformer import split_list_with_nan
from util.file_utils import get_validated_file

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class TitleCrewImporter(PandasImporter):
    def __init__(self, argv):
        super().__init__(argv=argv)
    
    def set_transformations(self):
        transformations = {
            "directors": split_list_with_nan,
            "writers": split_list_with_nan
        }

        return transformations
    
    def set_constraints(self):
        queries = [
            "CREATE CONSTRAINT titleId IF NOT EXISTS FOR (node:Title) REQUIRE node.id IS UNIQUE",
            "CREATE CONSTRAINT personId IF NOT EXISTS FOR (node:Person) REQUIRE node.id IS UNIQUE"
        ]
        for q in queries:
            with self._driver.session(database=self.database) as session:
                session.run(q)
    
    def import_crew(self, crew_file):
        query = """
        UNWIND $batch as item
        MERGE (title:Title {id: item.tconst})
        SET title.sources = apoc.coll.toSet(coalesce(title.sources, []) + "title.crew.tsv")

        WITH item, title
        WHERE size(item.directors) > 0
        UNWIND item.directors as directorId
        MERGE (director:Person {id: directorId})
        SET director.sources = apoc.coll.toSet(coalesce(director.sources, []) + "title.crew.tsv")
        MERGE (director)-[:DIRECTED]->(title)
        
        WITH item, title
        WHERE size(item.writers) > 0
        UNWIND item.writers as writerId
        MERGE (writer:Person {id: writerId})
        SET writer.sources = apoc.coll.toSet(coalesce(writer.sources, []) + "title.crew.tsv")
        MERGE (writer)-[:WROTE]->(title)
        """
        size = self.get_csv_size(crew_file)
        self.batch_store(query, self.get_rows(crew_file, self.set_transformations()), size=size)

if __name__ == '__main__':
    crew_importer = TitleCrewImporter(argv=sys.argv[1:])
    crew_file = get_validated_file(crew_importer, "title.crew.tsv")
    
    logging.info("Setting constraints...")
    crew_importer.set_constraints()

    logging.info("Importing title crew records...")
    crew_importer.import_crew(crew_file)
    crew_importer.close()
