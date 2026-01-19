import json
from pathlib import Path
from typing import List, Dict
from datetime import datetime
import sys
sys.path.append(str(Path(__file__).parent.parent))

from storage.db import Database
from ingestion.date_parser import parse_relative_date

class ReviewParser:
    def __init__(self, db: Database = None):
        self.db = db or Database()
    
    def parse_json_file(self, file_path: str, location_id: str = None) -> List[Dict]:
        """Parse a review JSON file and normalize the data"""
        # Try multiple encodings
        for encoding in ['utf-16-le', 'utf-16', 'utf-8', 'utf-8-sig', 'latin-1']:
            try:
                with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
                    data = json.load(f)
                    break
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                if encoding == 'latin-1':
                    raise Exception(f"Could not parse JSON file: {e}")
                continue
        
        # Extract location and date from filename: location_dd_mm_yyyy.json
        filename = Path(file_path).stem
        if not location_id:
            location_id = filename.split('_')[0]
        
        # Extract scrape date from filename
        scrape_date = self._extract_date_from_filename(filename)
        
        reviews = []
        
        # Handle the nested structure
        review_list = data.get('data', {}).get('reviews', [])
        
        for review in review_list:
            normalized = {
                'location_id': location_id,
                'source': 'google',
                'review_id': review.get('review_id'),
                'rating': review.get('rating'),
                'review_text': review.get('text', ''),
                'reviewer_name': review.get('reviewer'),
                'reviewer_type': self._extract_reviewer_type(review.get('reviewer', '')),
                'relative_date': review.get('relative_date'),
                'review_date': parse_relative_date(review.get('relative_date', ''), scrape_date),
                'language': 'en'  # Default, can be enhanced
            }
            reviews.append(normalized)
        
        return reviews
    
    def _extract_date_from_filename(self, filename: str) -> datetime:
        """Extract date from filename pattern: location_dd_mm_yyyy"""
        try:
            parts = filename.split('_')
            if len(parts) >= 4:
                day, month, year = int(parts[1]), int(parts[2]), int(parts[3])
                return datetime(year, month, day)
        except (ValueError, IndexError):
            pass
        return datetime.now()
    
    def _extract_reviewer_type(self, reviewer_info: str) -> str:
        """Extract reviewer type from reviewer string"""
        if 'Local Guide' in reviewer_info:
            return 'local_guide'
        return 'standard'
    
    def ingest_file(self, file_path: str, location_id: str = None) -> int:
        """Parse and insert reviews into database"""
        reviews = self.parse_json_file(file_path, location_id)
        count = 0
        
        for review in reviews:
            try:
                self.db.insert_review(review)
                count += 1
            except Exception as e:
                print(f"Error inserting review {review.get('review_id')}: {e}")
        
        return count

if __name__ == "__main__":
    # Test with JFK data
    parser = ReviewParser()
    count = parser.ingest_file("data/raw/JFK_reviews.json", "JFK")
    print(f"Ingested {count} reviews")
