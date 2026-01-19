import sqlite3
import json

conn = sqlite3.connect('data/reviews.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Get all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()
print("=== DATABASE TABLES ===")
for table in tables:
    print(f"\n📊 Table: {table['name']}")

# Show schema for each table
for table in tables:
    table_name = table['name']
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    print(f"\n--- Schema for '{table_name}' ---")
    for col in columns:
        print(f"  {col['name']:20} {col['type']:15} {'NOT NULL' if col['notnull'] else 'NULL':10} {'PK' if col['pk'] else ''}")

# Show sample data from each table
print("\n\n=== SAMPLE DATA ===")

# Reviews table
print("\n--- REVIEWS (1 sample) ---")
cursor.execute("SELECT * FROM reviews LIMIT 1")
review = cursor.fetchone()
if review:
    for key in review.keys():
        value = review[key]
        if key == 'review_text' and value and len(value) > 150:
            value = value[:150] + '...'
        print(f"  {key:20} {value}")

# Enrichments table
print("\n--- ENRICHMENTS (1 sample) ---")
cursor.execute("SELECT * FROM enrichments LIMIT 1")
enrichment = cursor.fetchone()
if enrichment:
    for key in enrichment.keys():
        value = enrichment[key]
        if key in ['topics', 'entities'] and value:
            value = json.loads(value)
        print(f"  {key:20} {value}")

# Embeddings table
print("\n--- EMBEDDINGS ---")
cursor.execute("SELECT COUNT(*) as count FROM embeddings")
emb_count = cursor.fetchone()['count']
print(f"  Total embeddings: {emb_count}")
if emb_count > 0:
    cursor.execute("SELECT id, review_id, LENGTH(embedding) as emb_size, created_at FROM embeddings LIMIT 1")
    emb = cursor.fetchone()
    print(f"  Sample: review_id={emb['review_id']}, embedding_size={emb['emb_size']} bytes")

# Insights cache table
print("\n--- INSIGHTS_CACHE ---")
cursor.execute("SELECT COUNT(*) as count FROM insights_cache")
insights_count = cursor.fetchone()['count']
print(f"  Total cached insights: {insights_count}")

# Show joined data
print("\n\n=== JOINED DATA (Review + Enrichment) ===")
cursor.execute("""
    SELECT 
        r.review_id,
        r.rating,
        r.reviewer_name,
        r.review_date,
        SUBSTR(r.review_text, 1, 100) as review_snippet,
        e.sentiment,
        e.sentiment_score,
        e.topics,
        e.entities
    FROM reviews r
    LEFT JOIN enrichments e ON r.review_id = e.review_id
    LIMIT 2
""")
print("\nShowing 2 complete records:")
for i, row in enumerate(cursor.fetchall(), 1):
    print(f"\n  Record {i}:")
    print(f"    Review ID: {row['review_id']}")
    print(f"    Rating: {row['rating']} stars")
    print(f"    Reviewer: {row['reviewer_name']}")
    print(f"    Date: {row['review_date']}")
    print(f"    Text: {row['review_snippet']}...")
    print(f"    Sentiment: {row['sentiment']} (score: {row['sentiment_score']})")
    topics = json.loads(row['topics']) if row['topics'] else []
    print(f"    Topics: {topics}")
    entities = json.loads(row['entities']) if row['entities'] else []
    print(f"    Entities: {entities}")

conn.close()
