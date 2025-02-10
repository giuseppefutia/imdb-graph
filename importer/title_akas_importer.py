import sys
import logging

from util.pandas_importer import PandasImporter
from util.data_transformer import replace_na
from util.file_utils import get_validated_file

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class TitleAkasImporter(PandasImporter):
    def __init__(self, argv):
        super().__init__(argv=argv)
    
    def set_transformations(self):
        transformations = {
            "ordering": replace_na,
            "title": replace_na,
            "region": replace_na,
            "language": replace_na,
            "types": replace_na,
            "attributes": replace_na,
            "isOriginalTitle": replace_na
        }

        return transformations
    
    def set_constraints(self):
        queries = [
            "CREATE CONSTRAINT titleId IF NOT EXISTS FOR (node:Title) REQUIRE node.id IS UNIQUE"
        ]

        for q in queries:
            with self._driver.session(database=self.database) as session:
                session.run(q)
    
    def clean_title_akas(self):
        query = """
        CALL apoc.periodic.iterate(
            "MATCH (title:Title) WHERE 'title.akas.tsv' in title.sources RETURN id(title) as id",
            "MATCH (title:Title) WHERE id(title)=id
             REMOVE title.ordering, 
                    title.alternateNames, 
                    title.regions, 
                    title.languages, 
                    title.types, 
                    title.attributes, 
                    title.areOriginalNames",
                {batchSize:10000})
        YIELD batches, total return batches, total
        """

        with self._driver.session(database=self.database) as session:
            session.run(query)
    
    def import_title_akas(self, titles_akas_file):
        query = """
        UNWIND $batch as item
        MERGE (title:Title {id: item.titleId})
        SET title.sources = apoc.coll.toSet(coalesce(title.sources, []) + "title.akas.tsv"),
            title.ordering = coalesce(title.ordering, []) + item.ordering,
            title.alternateNames = coalesce(title.alternateNames, []) + [item.title],
            title.regions = coalesce(title.regions, []) + item.region,
            title.languages = coalesce(title.languages, []) + item.language,
            title.types = coalesce(title.types, []) + item.types,
            title.attributes = coalesce(title.attributes, []) + item.attributes,
            title.areOriginalNames = coalesce(title.areOriginalNames, []) + item.isOriginalTitle
        """

        size = self.get_csv_size(titles_akas_file)
        self.batch_store(query, self.get_rows(titles_akas_file, self.set_transformations()), size=size)

if __name__ == '__main__':
    title_akas_importer = TitleAkasImporter(argv=sys.argv[1:])
    titles_akas_file = get_validated_file(title_akas_importer, "title.akas.tsv")

    logging.info("Setting constraints...")
    title_akas_importer.set_constraints()

    logging.info("Clean AKAs information...")
    title_akas_importer.clean_title_akas()

    logging.info("Importing title AKAs records...")
    title_akas_importer.import_title_akas(titles_akas_file)
    title_akas_importer.close()
