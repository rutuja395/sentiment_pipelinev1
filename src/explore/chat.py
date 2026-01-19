from utils.bedrock import BedrockClient
from storage.db import Database
from storage.vector_store import VectorStore
from typing import Dict, List

class ChatEngine:
    def __init__(self, db: Database = None):
        self.db = db or Database()
        self.bedrock = BedrockClient()
        self.vector_store = VectorStore(db=self.db)
    
    def chat(self, query: str, location_id: str = None, use_semantic: bool = True) -> Dict:
        if use_semantic:
            reviews = self.vector_store.search_similar(query, limit=5)
        else:
            reviews = self.db.get_reviews(location_id=location_id, limit=10)
        
        context = self._build_context(reviews)
        prompt = f"""You are analyzing car rental reviews. Answer based on the provided reviews.

Reviews:
{context}

User Question: {query}

Provide a concise answer with specific examples from the reviews."""
        
        response = self.bedrock.invoke(prompt, max_tokens=500)
        return {
            'answer': response,
            'citations': [{'review_id': r['review_id'], 'text': r['review_text'][:150]} for r in reviews[:3]],
            'review_count': len(reviews)
        }
    
    def _build_context(self, reviews: List[Dict]) -> str:
        context_parts = []
        for i, review in enumerate(reviews[:5], 1):
            context_parts.append(f"Review {i} (ID: {review['review_id']}, Rating: {review['rating']}):\n{review['review_text'][:300]}\n")
        return "\n".join(context_parts)
