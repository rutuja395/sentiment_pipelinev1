from storage.db import Database
from typing import Dict, List, Optional

class FilterEngine:
    def __init__(self, db: Database = None):
        self.db = db or Database()
    
    def apply_filters(self, location_id: Optional[str] = None, min_rating: Optional[float] = None,
                     max_rating: Optional[float] = None, start_date: Optional[str] = None,
                     end_date: Optional[str] = None, topics: Optional[List[str]] = None,
                     sentiment: Optional[str] = None, limit: int = 100) -> List[Dict]:
        reviews = self.db.get_reviews(location_id=location_id, min_rating=min_rating, max_rating=max_rating, limit=limit)
        filtered = reviews
        
        if start_date:
            filtered = [r for r in filtered if r.get('review_date', '') >= start_date]
        if end_date:
            filtered = [r for r in filtered if r.get('review_date', '') <= end_date]
        if topics or sentiment:
            filtered = self._filter_by_enrichment(filtered, topics, sentiment)
        
        return filtered
    
    def _filter_by_enrichment(self, reviews: List[Dict], topics: Optional[List[str]], sentiment: Optional[str]) -> List[Dict]:
        import json
        result = []
        for review in reviews:
            enrichment = self.db.get_review_with_enrichment(review['review_id'])
            if not enrichment:
                continue
            if sentiment and enrichment.get('sentiment') != sentiment:
                continue
            if topics:
                review_topics = json.loads(enrichment['topics']) if isinstance(enrichment.get('topics'), str) else enrichment.get('topics', [])
                if not any(t in review_topics for t in topics):
                    continue
            result.append(enrichment)
        return result
