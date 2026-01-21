"""
Export reviews database to JSON and upload to S3 for Knowledge Base ingestion.

Usage:
    python src/export_to_s3.py <bucket-name> [--prefix path/to/folder] [--by-location]
    
Example:
    python src/export_to_s3.py my-kb-bucket --prefix reviews/
    python src/export_to_s3.py my-kb-bucket --prefix reviews/ --by-location
"""
import sqlite3
import json
import argparse
from pathlib import Path
from typing import Dict, List

import boto3


def get_all_locations(db_path: str) -> List[str]:
    """Get all unique location IDs from the database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT location_id FROM reviews")
    locations = [row[0] for row in cursor.fetchall()]
    conn.close()
    return locations


def export_reviews_to_json(db_path: str, output_path: str, location_id: str = None) -> int:
    """Export reviews with enrichments to JSON format, optionally filtered by location."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = """
        SELECT 
            r.review_id, r.location_id, r.rating, r.reviewer_name,
            r.review_date, r.review_text,
            e.sentiment, e.sentiment_score, e.topics, e.entities
        FROM reviews r
        LEFT JOIN enrichments e ON r.review_id = e.review_id
    """
    params = []
    if location_id:
        query += " WHERE r.location_id = ?"
        params.append(location_id)

    cursor.execute(query, params)

    reviews = []
    for row in cursor.fetchall():
        reviews.append({
            "review_id": row["review_id"],
            "location_id": row["location_id"],
            "rating": row["rating"],
            "reviewer_name": row["reviewer_name"],
            "review_date": row["review_date"],
            "review_text": row["review_text"],
            "sentiment": row["sentiment"],
            "sentiment_score": row["sentiment_score"],
            "topics": json.loads(row["topics"]) if row["topics"] else [],
            "entities": json.loads(row["entities"]) if row["entities"] else []
        })

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(reviews, f, indent=2)

    conn.close()
    return len(reviews)


def upload_to_s3(file_path: str, bucket: str, key: str):
    """Upload file to S3."""
    s3 = boto3.client("s3")
    s3.upload_file(file_path, bucket, key)
    print(f"[OK] Uploaded to s3://{bucket}/{key}")


def main():
    parser = argparse.ArgumentParser(description="Export reviews to S3 for Knowledge Base")
    parser.add_argument("bucket", help="S3 bucket name")
    parser.add_argument("--prefix", default="", help="S3 key prefix (folder path)")
    parser.add_argument("--db", default="data/reviews.db", help="Path to SQLite database")
    parser.add_argument("--by-location", action="store_true", 
                        help="Export separate JSON files per location")
    args = parser.parse_args()

    if args.by_location:
        print("Step 1: Getting all locations...")
        locations = get_all_locations(args.db)
        print(f"[OK] Found {len(locations)} locations: {', '.join(locations)}")

        print("\nStep 2: Exporting reviews by location...")
        total_count = 0
        exported_files = []
        
        for location_id in locations:
            output_file = f"data/processed/reviews_{location_id}.json"
            count = export_reviews_to_json(args.db, output_file, location_id)
            total_count += count
            exported_files.append((output_file, location_id, count))
            print(f"  [OK] {location_id}: {count} reviews -> {output_file}")

        print("\nStep 3: Uploading to S3...")
        for output_file, location_id, count in exported_files:
            s3_key = f"{args.prefix}reviews_{location_id}.json".lstrip("/")
            upload_to_s3(output_file, args.bucket, s3_key)

        print(f"\n[COMPLETE] {total_count} reviews across {len(locations)} locations ready for Knowledge Base ingestion")
    else:
        output_file = "data/processed/reviews_export.json"
        
        print("Step 1: Exporting reviews to JSON...")
        count = export_reviews_to_json(args.db, output_file)
        print(f"[OK] Exported {count} reviews to {output_file}")

        print("Step 2: Uploading to S3...")
        s3_key = f"{args.prefix}reviews_export.json".lstrip("/")
        upload_to_s3(output_file, args.bucket, s3_key)

        print(f"\n[COMPLETE] {count} reviews ready for Knowledge Base ingestion")


if __name__ == "__main__":
    main()
