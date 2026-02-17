"""
Reddit Data Ingestion Script
Ingests Reddit posts/comments about car rental brands into the review database.
"""
import json
import sys
from pathlib import Path
from datetime import datetime

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))

from storage.db import Database
from ingestion.reddit_parser import RedditParser
from ingestion.enricher import ReviewEnricher
from utils.logger import get_logger

logger = get_logger(__name__)


def ingest_reddit_file(file_path: str, enrich: bool = True) -> dict:
    """
    Ingest a Reddit JSON file into the database.
    
    Args:
        file_path: Path to the Reddit JSON file
        enrich: Whether to run LLM enrichment on the reviews
        
    Returns:
        Summary dict with counts
    """
    db = Database()
    parser = RedditParser()
    
    logger.start(f"Ingesting Reddit data from: {file_path}")
    
    # Load JSON file
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Parse scrape date from data or filename
    scrape_date_str = data.get("scrape_date")
    if scrape_date_str:
        scrape_date = datetime.strptime(scrape_date_str, "%Y-%m-%d")
    else:
        scrape_date = datetime.now()
    
    # Parse Reddit data into reviews
    brand = data.get("brand", "avis")
    reviews = parser.parse_reddit_data(data, brand=brand, scrape_date=scrape_date)
    
    logger.info(f"Parsed {len(reviews)} reviews from Reddit data")
    
    # Insert reviews into database
    inserted = 0
    for review in reviews:
        try:
            db.insert_review(review)
            inserted += 1
        except Exception as e:
            logger.error(f"Error inserting review {review.get('review_id')}: {e}")
    
    logger.success(f"Inserted {inserted} Reddit reviews into database")
    
    # Enrich reviews if requested
    enriched = 0
    if enrich and inserted > 0:
        logger.info("Starting LLM enrichment for Reddit reviews...")
        enricher = ReviewEnricher(db)
        
        # Get unenriched Reddit reviews
        unenriched = db.get_reviews(unenriched_only=True, limit=1000)
        reddit_reviews = [r for r in unenriched if r.get('source') == 'reddit']
        
        if reddit_reviews:
            enriched = enricher.enrich_all_reviews()
            logger.success(f"Enriched {enriched} Reddit reviews")
    
    return {
        "file": file_path,
        "brand": brand,
        "parsed": len(reviews),
        "inserted": inserted,
        "enriched": enriched
    }


def ingest_all_reddit_files(reddit_dir: str = "data/raw/reddit", enrich: bool = True):
    """Ingest all Reddit JSON files from a directory"""
    reddit_path = Path(reddit_dir)
    
    if not reddit_path.exists():
        logger.error(f"Reddit data directory not found: {reddit_dir}")
        return []
    
    results = []
    json_files = list(reddit_path.glob("*.json"))
    
    logger.start(f"Found {len(json_files)} Reddit JSON files to process")
    
    for file_path in json_files:
        try:
            result = ingest_reddit_file(str(file_path), enrich=enrich)
            results.append(result)
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            results.append({"file": str(file_path), "error": str(e)})
    
    return results


if __name__ == "__main__":
    import argparse
    
    arg_parser = argparse.ArgumentParser(description="Ingest Reddit data into review database")
    arg_parser.add_argument("--file", "-f", help="Specific Reddit JSON file to ingest")
    arg_parser.add_argument("--dir", "-d", default="data/raw/reddit", help="Directory with Reddit JSON files")
    arg_parser.add_argument("--no-enrich", action="store_true", help="Skip LLM enrichment")
    
    args = arg_parser.parse_args()
    
    enrich = not args.no_enrich
    
    if args.file:
        result = ingest_reddit_file(args.file, enrich=enrich)
        print(f"\nResult: {json.dumps(result, indent=2)}")
    else:
        results = ingest_all_reddit_files(args.dir, enrich=enrich)
        print(f"\nResults: {json.dumps(results, indent=2)}")
