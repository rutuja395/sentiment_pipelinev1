import sys
import os
from dotenv import load_dotenv
load_dotenv()

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')
os.environ['AWS_REGION'] = os.getenv('AWS_REGION', 'us-east-1')

sys.path.insert(0, 'src')
from ingestion.pipeline import IngestionPipeline

pipeline = IngestionPipeline(
    bucket_name='google-reviews-extract',
    prefix='google/',
    batch_size=20
)

print("Processing all pending files (without enrichment for speed)...")
results = pipeline.process_all_pending(enrich=False)

print(f"\n=== SUMMARY ===")
print(f"Processed {len(results)} files")
for r in results:
    status = r['status']
    reviews = r['reviews_count']
    enriched = r['enriched_count']
    key = r['s3_key']
    print(f"  {key}: {status} - {reviews} reviews, {enriched} enriched")
