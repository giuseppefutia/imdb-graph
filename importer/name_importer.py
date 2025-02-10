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

class NameImporter(PandasImporter):
    def __init__(self, argv):
        super().__init__(argv=argv)
    
    def set_transformations(self):
        transformations = {
            "primaryProfession": split_list,
            "knownForTitles": split_list
        }

        return transformations
    
    def set_constraints(self):
        queries = ["CREATE CONSTRAINT personId IF NOT EXISTS FOR (node:Person) REQUIRE node.id IS UNIQUE",
                   "CREATE CONSTRAINT titleId IF NOT EXISTS FOR (node:Title) REQUIRE node.id IS UNIQUE",
                   "CREATE CONSTRAINT professionName IF NOT EXISTS FOR (node:Profession) REQUIRE node.name IS UNIQUE"]

        for q in queries:
            with self._driver.session(database=self.database) as session:
                session.run(q)
    
    def import_people(self, people_file):
        query = """
        UNWIND $batch as item
        MERGE (person:Person {id: item.nconst})
        SET person.sources = apoc.coll.toSet(coalesce(person.sources, []) + "name.basics.tsv"),
            person.name = item.primaryName,
            person.birthYear = 
                CASE WHEN item.birthYear =~ '\\d+' THEN toInteger(item.birthYear)
                ELSE NULL END,
            person.endYear =
                CASE WHEN item.deathYear =~ '\\d+' THEN toInteger(item.deathYear)
                ELSE NULL END
        WITH person, item
        UNWIND item.primaryProfession as professionName
        MERGE (profession:Profession {name: professionName})
        MERGE (person)-[:HAS_PROFESSION]->(profession)
        WITH person, item
        UNWIND item.knownForTitles as titleId
        MERGE (title:Title {id: titleId})
        SET title.sources = apoc.coll.toSet(coalesce(title.sources, []) + "name.basics.tsv")
        MERGE (person)-[:KNOWN_FOR]->(title)
        """
        size = self.get_csv_size(people_file)
        self.batch_store(query, self.get_rows(people_file, self.set_transformations()), size=size)

if __name__ == '__main__':
    people_importer = NameImporter(argv=sys.argv[1:])
    people_file = get_validated_file(people_importer, "name.basics.tsv")

    logging.info("Setting constraints...")
    people_importer.set_constraints()

    logging.info("Importing name records...")
    people_importer.import_people(people_file)
    people_importer.close()
