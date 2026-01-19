"""
Ingestion Pipeline - Run this to process review files
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from storage.db import Database
from ingestion.parser import ReviewParser
from ingestion.enricher import ReviewEnricher

def run_ingestion(file_path: str, location_id: str = None, batch_size: int = 20):
    """Run full ingestion pipeline with batch LLM processing"""
    print(f"Starting ingestion for {file_path}...")
    
    # Step 1: Parse and insert reviews
    print("Step 1: Parsing reviews...")
    parser = ReviewParser()
    count = parser.ingest_file(file_path, location_id)
    print(f"[OK] Inserted {count} reviews")
    
    # Step 2: Enrich reviews in batches
    print(f"Step 2: Enriching reviews (batch size: {batch_size})...")
    enricher = ReviewEnricher(batch_size=batch_size)
    enriched = enricher.enrich_all_reviews(location_id=location_id)
    print(f"[OK] Enriched {enriched} reviews")
    
    print(f"\n[COMPLETE] Ingestion complete! Processed {count} reviews.")

if __name__ == "__main__":
    # Process JFK reviews with batch LLM enrichment
    run_ingestion("data/raw/JFK_reviews_10_01_2026.json", "JFK", batch_size=20)
