import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from storage.db import Database
from utils.bedrock import BedrockClient
from typing import List, Dict

class ReviewEnricher:
    def __init__(self, db: Database = None, batch_size: int = 20):
        self.db = db or Database()
        self.bedrock = BedrockClient()
        self.batch_size = batch_size
    
    def enrich_batch(self, reviews: List[Dict]) -> List[Dict]:
        """Enrich a batch of reviews using LLM"""
        print(f"  -> Sending batch of {len(reviews)} reviews to LLM...")
        enrichments = self.bedrock.enrich_reviews_batch(reviews)
        print(f"  -> Received {len(enrichments)} enrichments")
        return enrichments
    
    def enrich_all_reviews(self, location_id: str = None, limit: int = None):
        """Enrich all reviews in database using batch processing"""
        reviews = self.db.get_reviews(location_id=location_id, limit=limit or 10000)
        total = len(reviews)
        count = 0
        
        print(f"Processing {total} reviews in batches of {self.batch_size}...")
        
        # Process in batches
        for i in range(0, len(reviews), self.batch_size):
            batch_num = (i // self.batch_size) + 1
            total_batches = (total + self.batch_size - 1) // self.batch_size
            batch = reviews[i:i + self.batch_size]
            
            print(f"\nBatch {batch_num}/{total_batches}:")
            try:
                enrichments = self.enrich_batch(batch)
                for enrichment in enrichments:
                    self.db.insert_enrichment(enrichment)
                    count += 1
                print(f"  -> Saved to database. Progress: {count}/{total} reviews ({int(count/total*100)}%)")
            except Exception as e:
                print(f"  -> ERROR: {e}")
        
        return count
