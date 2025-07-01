import kuzu
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class PersonSearcher:
    def __init__(self, db_path: str = './imdb2', index_name: str = 'person_name_idx'):
        self.db = kuzu.Database(db_path)
        self.conn = kuzu.Connection(self.db)
        self.index_name = index_name

    def enable_fts_extension(self):
        logging.info("Installing and loading full-text (fts) extension...")
        self.conn.execute("INSTALL fts;")
        self.conn.execute("LOAD EXTENSION fts;")
        logging.info("✅ FTS extension enabled.")

    def create_full_text_index(self):
        logging.info(f"Building FTS index '{self.index_name}' on Person(name)...")
        # follows the documented usage CALL CREATE_FTS_INDEX('Person', 'indexName', ['name'])
        self.conn.execute(
            f"CALL CREATE_FTS_INDEX('Person', '{self.index_name}', ['name']);"
        )
        logging.info("✅ FTS index created successfully.")

    def search_by_name(self, keyword: str, limit: int = 10):
        #logging.info(f"Executing FTS query for '{keyword}'...")
        query = f"""
        CALL QUERY_FTS_INDEX(
            'Person',
            '{self.index_name}',
            '{keyword}'
        )
        YIELD node AS person, score
        RETURN person.id, person.name, score
        ORDER BY score DESC
        LIMIT {limit};
        """
        result = self.conn.execute(query)
        """
        print(f"\nFull-text search results for '{keyword}':")
        while result.has_next():
            row = result.get_next()
            # row is a list: [person.id, person.name, score]
            print(f"• {row[1]} (id: {row[0]}), score = {row[2]:.4f}")
        """

if __name__ == '__main__':
    searcher = PersonSearcher()

    # Step 1: Enable FTS
    searcher.enable_fts_extension()

    # Step 2: Create the full-text index (run once unless re-building)
    searcher.create_full_text_index()

    # Step 3: Search by name
    searcher.search_by_name("Tom", limit=5)
