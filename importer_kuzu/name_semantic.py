import kuzu
from sentence_transformers import SentenceTransformer
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class PersonSearcher:
    def __init__(self, db_path: str = './imdb3', index_name="person_name_embedding"):
        self.db = kuzu.Database(db_path)
        self.conn = kuzu.Connection(self.db)
        self.index_name = index_name
        self.model = SentenceTransformer("all-mpnet-base-v2")
    
    def install_extension(self):
        logging.info(f"Creating vector extension...")
        self.conn.execute("INSTALL VECTOR;")
        self.conn.execute("LOAD VECTOR;")
        logging.info("✅ Vector extension installed.")

    def create_vector_index(self):
        logging.info("Creating the vector index...")
        query = f"""
        CALL CREATE_VECTOR_INDEX(
            'Person',
            '{self.index_name}',
            'embedding'
        );
        """
        self.conn.execute(query)
        logging.info("✅ Vector index created enabled.")

    def search_by_name_semantically(self, keyword, limit=5):
        # Encode the keyword using the embedding model
        embedding = self.model.encode(keyword, show_progress_bar=False).tolist()

        query = f"""
        CALL QUERY_VECTOR_INDEX(
            Person,
            {self.index_name},
            {embedding},
            {limit},
            [option_name := option_value]
        ) RETURN node.id ORDER BY distance;
        """
        
        print(f"\nVetor search results for '{keyword}':")
        result = self.conn.execute(query)
        while result.has_next():
            row = result.get_next()
            # row is a list: [person.id, person.name, score]
            print(f"• {row[1]} (id: {row[0]}), score = {row[2]:.4f}")

if __name__ == '__main__':
    searcher = PersonSearcher()

    searcher.install_extension()

    # Only needed once
    searcher.create_vector_index()

    searcher.search_by_name_semantically("Tom Cruise", limit=5)
