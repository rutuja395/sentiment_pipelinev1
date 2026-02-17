"""
Sync and process brand data from S3
Downloads raw review files from S3 and processes them into the database
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import boto3
import json

sys.path.insert(0, str(Path(__file__).parent))

from storage.db import Database
from ingestion.parser import ReviewParser
from ingestion.enricher import ReviewEnricher

load_dotenv()

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION', 'us-east-1')
    )

def list_brand_files(s3, bucket: str, brand: str) -> list:
    """List all review files for a brand in S3"""
    prefix = f"google/{brand}/"
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = []
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('.json') and key != prefix:
            files.append(key)
    return files

def download_file(s3, bucket: str, key: str, local_path: str):
    """Download a file from S3"""
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, local_path)
    print(f"  Downloaded: {key} -> {local_path}")

def extract_location_from_key(key: str) -> str:
    """Extract location ID from S3 key like google/hertz/ATL_2026-02-12_33.6407_-84.4277.json"""
    filename = Path(key).stem
    return filename.split('_')[0]

def sync_brand(brand: str, enrich: bool = False, batch_size: int = 20):
    """Sync all data for a brand from S3 and process it"""
    s3 = get_s3_client()
    bucket = os.getenv('REVIEWS_S3_BUCKET', 'google-reviews-extract')
    
    print(f"\n{'='*50}")
    print(f"Syncing {brand.upper()} data from S3")
    print(f"{'='*50}")
    
    # List files for brand
    files = list_brand_files(s3, bucket, brand)
    print(f"Found {len(files)} files for {brand}")
    
    if not files:
        print(f"No files found for {brand}")
        return
    
    # Download and process each file
    db = Database()
    parser = ReviewParser(db)
    
    for key in files:
        location_id = extract_location_from_key(key)
        local_path = f"data/raw/{brand}/{Path(key).name}"
        
        print(f"\nProcessing {location_id} ({brand})...")
        
        # Download
        download_file(s3, bucket, key, local_path)
        
        # Parse with brand info
        reviews = parser.parse_json_file(local_path, location_id)
        
        # Add brand to each review
        count = 0
        for review in reviews:
            review['brand'] = brand
            try:
                db.insert_review(review)
                count += 1
            except Exception as e:
                print(f"  Error inserting review: {e}")
        
        print(f"  Inserted {count} reviews for {location_id} ({brand})")
    
    # Optionally enrich
    if enrich:
        print(f"\nEnriching {brand} reviews...")
        enricher = ReviewEnricher(db, batch_size=batch_size)
        # Get all reviews for this brand that don't have enrichments
        enricher.enrich_all_reviews()

def sync_all_brands(enrich: bool = False):
    """Sync all brands from S3"""
    brands = ['avis', 'hertz', 'enterprise']
    for brand in brands:
        try:
            sync_brand(brand, enrich=enrich)
        except Exception as e:
            print(f"Error syncing {brand}: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Sync brand data from S3')
    parser.add_argument('--brand', type=str, help='Specific brand to sync (avis, hertz, enterprise)')
    parser.add_argument('--enrich', action='store_true', help='Run LLM enrichment after sync')
    parser.add_argument('--all', action='store_true', help='Sync all brands')
    
    args = parser.parse_args()
    
    if args.all:
        sync_all_brands(enrich=args.enrich)
    elif args.brand:
        sync_brand(args.brand, enrich=args.enrich)
    else:
        # Default: sync hertz
        sync_brand('hertz', enrich=args.enrich)
