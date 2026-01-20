import sqlite3
from datetime import datetime
from typing import List, Dict, Optional
import json

class Database:
    def __init__(self, db_path: str = "data/reviews.db"):
        self.db_path = db_path
        self.init_db()
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def init_db(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Reviews table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                location_id TEXT NOT NULL,
                source TEXT DEFAULT 'google',
                review_id TEXT UNIQUE NOT NULL,
                rating REAL NOT NULL,
                review_text TEXT,
                reviewer_name TEXT,
                reviewer_type TEXT,
                review_date TEXT,
                relative_date TEXT,
                language TEXT,
                ingested_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Enrichments table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS enrichments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id TEXT UNIQUE NOT NULL,
                topics TEXT,
                sentiment TEXT,
                sentiment_score REAL,
                entities TEXT,
                processed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (review_id) REFERENCES reviews(review_id)
            )
        """)
        
        # Insights cache table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS insights_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                location_id TEXT NOT NULL,
                time_window TEXT NOT NULL,
                top_topics TEXT,
                key_drivers TEXT,
                representative_quotes TEXT,
                anomalies TEXT,
                generated_summary TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Embeddings table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS embeddings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id TEXT UNIQUE NOT NULL,
                embedding BLOB,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (review_id) REFERENCES reviews(review_id)
            )
        """)
        
        # Locations table for storing location metadata including coordinates
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                location_id TEXT UNIQUE NOT NULL,
                name TEXT,
                latitude REAL,
                longitude REAL,
                address TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
    
    def insert_review(self, review: Dict) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO reviews 
            (location_id, source, review_id, rating, review_text, reviewer_name, 
             reviewer_type, review_date, relative_date, language)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            review.get('location_id'),
            review.get('source', 'google'),
            review.get('review_id'),
            review.get('rating'),
            review.get('review_text'),
            review.get('reviewer_name'),
            review.get('reviewer_type'),
            review.get('review_date'),
            review.get('relative_date'),
            review.get('language', 'en')
        ))
        conn.commit()
        row_id = cursor.lastrowid
        conn.close()
        return row_id
    
    def insert_enrichment(self, enrichment: Dict):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO enrichments 
            (review_id, topics, sentiment, sentiment_score, entities)
            VALUES (?, ?, ?, ?, ?)
        """, (
            enrichment.get('review_id'),
            json.dumps(enrichment.get('topics', [])),
            enrichment.get('sentiment'),
            enrichment.get('sentiment_score'),
            json.dumps(enrichment.get('entities', []))
        ))
        conn.commit()
        conn.close()
    
    def get_reviews(self, location_id: Optional[str] = None, 
                    min_rating: Optional[float] = None,
                    max_rating: Optional[float] = None,
                    limit: int = 100) -> List[Dict]:
        conn = self.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = "SELECT * FROM reviews WHERE 1=1"
        params = []
        
        if location_id:
            query += " AND location_id = ?"
            params.append(location_id)
        if min_rating:
            query += " AND rating >= ?"
            params.append(min_rating)
        if max_rating:
            query += " AND rating <= ?"
            params.append(max_rating)
        
        query += f" ORDER BY review_date DESC LIMIT {limit}"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def get_review_with_enrichment(self, review_id: str) -> Optional[Dict]:
        conn = self.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT r.*, e.topics, e.sentiment, e.sentiment_score, e.entities
            FROM reviews r
            LEFT JOIN enrichments e ON r.review_id = e.review_id
            WHERE r.review_id = ?
        """, (review_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        return dict(row) if row else None
    
    def upsert_location(self, location_id: str, name: str = None, 
                        latitude: float = None, longitude: float = None, 
                        address: str = None):
        """Insert or update location metadata"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO locations (location_id, name, latitude, longitude, address)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(location_id) DO UPDATE SET
                name = COALESCE(excluded.name, locations.name),
                latitude = COALESCE(excluded.latitude, locations.latitude),
                longitude = COALESCE(excluded.longitude, locations.longitude),
                address = COALESCE(excluded.address, locations.address)
        """, (location_id, name, latitude, longitude, address))
        conn.commit()
        conn.close()
    
    def get_locations_with_coords(self) -> List[Dict]:
        """Get all locations with their coordinates"""
        conn = self.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT r.location_id, l.name, l.latitude, l.longitude, l.address
            FROM reviews r
            LEFT JOIN locations l ON r.location_id = l.location_id
        """)
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
