import sys
import logging

from util.pandas_importer import PandasImporter
from util.data_transformer import split_list
from util.file_utils import get_validated_file

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
) 

class TitleImporter(PandasImporter):
    def __init__(self, argv):
        super().__init__(argv=argv)
    
    def set_transformations(self):
        transformations = {
            "genres": split_list
        }

        return transformations
    
    def set_constraints(self):
        queries = [
            "CREATE CONSTRAINT titleId IF NOT EXISTS FOR (node:Title) REQUIRE node.id IS UNIQUE",
            "CREATE CONSTRAINT genreName IF NOT EXISTS FOR (node:Genre) REQUIRE node.name IS UNIQUE"
        ]

        for q in queries:
            with self._driver.session(database=self.database) as session:
                session.run(q)
    
    def import_titles(self, titles_file):
        query = """
        UNWIND $batch as item
        
        MERGE (title:Title {id: item.tconst})
        SET title.sources = apoc.coll.toSet(coalesce(title.sources, []) + "title.basics.tsv"),
            title.name = item.primaryTitle,
            title.originalName = item.originalTitle,
            title.duration = 
                CASE WHEN item.runtimeMinutes =~ '\\d+' THEN toInteger(item.runtimeMinutes)
                ELSE NULL END,
            title.startYear = 
                CASE WHEN item.startYear =~ '\\d+' THEN toInteger(item.startYear)
                ELSE NULL END,
            title.endYear =
                CASE 
                    WHEN item.endYear =~ '\\d+' THEN toInteger(item.endYear)
                    WHEN item.startYear =~ '\\d+' THEN toInteger(item.startYear)
                ELSE NULL END
        WITH title, item
        
        UNWIND item.genres as genreName
        MERGE (genre:Genre {name: genreName})
        MERGE (title)-[:HAS_GENRE]->(genre)
        """
        size = self.get_csv_size(titles_file)
        self.batch_store(query, self.get_rows(titles_file, self.set_transformations()), size=size)

if __name__ == '__main__':
    title_importer = TitleImporter(argv=sys.argv[1:])
    titles_file = get_validated_file(title_importer, "title.basics.tsv")

    logging.info("Setting constraints...")
    title_importer.set_constraints()

    logging.info("Importing title records...")
    title_importer.import_titles(titles_file)
    title_importer.close()
