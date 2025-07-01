import sys
import logging
import time
from util.pandas_importer import PandasImporter
import kuzu
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class NameImporter(PandasImporter):

    def __init__(self, argv):
        super().__init__(argv=argv)

    def set_constraints(self):
        pass

    def get_rows(self, batch_size=100):
        last_id = ""
        while True:
            query = """
            MATCH (p:Person)
            WHERE p.embedding IS NOT NULL AND p.id > $last_id
            RETURN p.id AS id, p.embedding AS embedding, p.name AS name
            ORDER BY p.id ASC
            LIMIT $limit
            """
            try:
                with self._driver.session(database=self.database) as session:
                    result = session.run(query, last_id=last_id, limit=batch_size)
                    batch = list(result)
            except Exception as e:
                logging.error(f"Neo4j read failed: {e}")
                time.sleep(2)
                continue

            if not batch:
                break

            for row in batch:
                yield {
                    "id": row['id'],
                    "embedding": row['embedding'],
                    "name": row['name']
                }

            last_id = batch[-1]['id']

    def insert_batch_kuzu(self, conn, batch):
        for row in batch:
            id = row["id"]
            embedding = row["embedding"]
            name = row["name"]
            emb_str = ','.join(map(str, embedding))
            query = f"""
            MERGE (p:Person {{id: '{id}'}})
            SET p.embedding = [{emb_str}],
                p.name = "{name}"
            """
            try:
                conn.execute(query)
            except Exception as e:
                logging.error(f"Failed to insert {id} into Kùzu: {e}")

    def import_data(self):
        logging.info("Connecting to KùzuDB...")
        db = kuzu.Database("imdb3")
        conn = kuzu.Connection(db)
        conn.execute("INSTALL vector; LOAD vector;")

        try:
            conn.execute("CREATE NODE TABLE IF NOT EXISTS Person(id STRING, name STRING, embedding FLOAT[768], PRIMARY KEY (id))")
        except Exception as e:
            logging.warning(f"Table creation failed (maybe already exists): {e}")

        logging.info("Importing data into KùzuDB...")
        batch = []
        progress = tqdm(desc="Importing", unit="rows")
        for row in self.get_rows():
            batch.append(row)
            progress.update(1)
            if len(batch) >= 1000:
                self.insert_batch_kuzu(conn, batch)
                batch = []

        if batch:
            self.insert_batch_kuzu(conn, batch)

        progress.close()
        logging.info("Import into KùzuDB completed.")

if __name__ == '__main__':
    importer = NameImporter(argv=sys.argv[1:])

    logging.info("Setting constraints...")
    importer.set_constraints()

    logging.info("Importing data...")
    importer.import_data()

    importer.close()
