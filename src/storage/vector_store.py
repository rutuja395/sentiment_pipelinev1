import numpy as np
from sentence_transformers import SentenceTransformer
from storage.db import Database
from typing import List, Dict

class VectorStore:
    def __init__(self, db: Database = None, model_name: str = "all-MiniLM-L6-v2"):
        self.db = db or Database()
        self.model = SentenceTransformer(model_name)
    
    def generate_embeddings(self, location_id: str = None):
        reviews = self.db.get_reviews(location_id=location_id, limit=10000)
        count = 0
        for review in reviews:
            text = review.get('review_text', '')
            if text:
                embedding = self.model.encode(text)
                self._store_embedding(review['review_id'], embedding)
                count += 1
                if count % 50 == 0:
                    print(f"Generated {count} embeddings...")
        return count
    
    def _store_embedding(self, review_id: str, embedding: np.ndarray):
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO embeddings (review_id, embedding) VALUES (?, ?)",
                      (review_id, embedding.tobytes()))
        conn.commit()
        conn.close()
    
    def search_similar(self, query: str, limit: int = 5) -> List[Dict]:
        query_embedding = self.model.encode(query)
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT review_id, embedding FROM embeddings")
        results = []
        for row in cursor.fetchall():
            review_id, embedding_bytes = row
            embedding = np.frombuffer(embedding_bytes, dtype=np.float32)
            similarity = np.dot(query_embedding, embedding) / (np.linalg.norm(query_embedding) * np.linalg.norm(embedding))
            results.append((review_id, similarity))
        conn.close()
        results.sort(key=lambda x: x[1], reverse=True)
        top_results = []
        for review_id, score in results[:limit]:
            review = self.db.get_review_with_enrichment(review_id)
            if review:
                review['similarity_score'] = float(score)
                top_results.append(review)
        return top_results
