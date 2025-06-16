import sys
import logging

from util.pandas_importer import PandasImporter
from util.data_transformer import from_str_list_to_json
from util.file_utils import get_validated_file

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)


class TitlePrincipalsImporter(PandasImporter):
    def __init__(self, argv):
        super().__init__(argv=argv)    
    
    def set_transformations(self):
        transformations = {
            "characters": from_str_list_to_json
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
    
    def import_principals(self, principals_file):
        query = """
        UNWIND $batch as item
        MERGE (title:Title {id: item.tconst})
        SET title.sources = apoc.coll.toSet(coalesce(title.sources, []) + "title.principals.tsv")
        
        MERGE (person:Person {id: item.nconst})
        SET person.sources = apoc.coll.toSet(coalesce(person.sources, []) + "title.principals.tsv")
        
        MERGE (person)-[worked_in:WORKED_IN]->(title)
        SET worked_in.ordering = toInteger(item.ordering),
            worked_in.category = item.category,
            worked_in.job = 
                CASE WHEN item.job <> '\\N' THEN item.job
                ELSE NULL END,
            worked_in.characters =
                CASE WHEN item.characters <> '\\N' THEN item.characters
                ELSE NULL END
        """
        size = self.get_csv_size(principals_file)
        self.batch_store(query, self.get_rows(principals_file, self.set_transformations()), size=size)

if __name__ == '__main__':
    principals_importer = TitlePrincipalsImporter(argv=sys.argv[1:])
    principals_file = get_validated_file(principals_importer, "title.principals.tsv")
    
    logging.info("Setting constraints...")
    principals_importer.set_constraints()
    
    logging.info("Importing title principals records...")
    principals_importer.import_principals(principals_file)
    principals_importer.close()
