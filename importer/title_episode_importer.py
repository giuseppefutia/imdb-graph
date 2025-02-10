import sys
import logging

from util.pandas_importer import PandasImporter
from util.file_utils import get_validated_file

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class TitleEpisodeImporter(PandasImporter):
    def __init__(self, argv):
        super().__init__(argv=argv)
    
    def set_constraints(self):
        queries = [
            "CREATE CONSTRAINT titleId IF NOT EXISTS FOR (node:Title) REQUIRE node.id IS UNIQUE"
        ]
        for q in queries:
            with self._driver.session(database=self.database) as session:
                session.run(q)
    
    def import_episodes(self, episodes_file):
        query = """
        UNWIND $batch as item
        MERGE (episode:Title {id: item.tconst})
        SET episode.sources = apoc.coll.toSet(coalesce(episode.sources, []) + "title.episode.tsv")
        MERGE (parent:Title {id: item.parentTconst})
        SET parent.sources = apoc.coll.toSet(coalesce(parent.sources, []) + "title.episode.tsv")
        MERGE (episode)-[r:PART_OF]->(parent)
        SET r.seasonNumber = 
            CASE WHEN item.seasonNumber <> '\\N' THEN toInteger(item.seasonNumber)
            ELSE NULL END,
            r.episodeNumber = CASE WHEN item.episodeNumber <> '\\N' THEN toInteger(item.episodeNumber)
            ELSE NULL END
        """
        size = self.get_csv_size(episodes_file)
        self.batch_store(query, self.get_rows(episodes_file, transformations={}), size=size)

if __name__ == '__main__':
    episode_importer = TitleEpisodeImporter(argv=sys.argv[1:])
    episodes_file = get_validated_file(episode_importer, "title.episode.tsv")
    
    logging.info("Setting constraints...")
    episode_importer.set_constraints()

    logging.info("Importing episode records...")
    episode_importer.import_episodes(episodes_file)
