from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict
from collections import Counter
from datetime import datetime
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from storage.db import Database
from monitor.insights import InsightGenerator
from explore.chat import ChatEngine
from explore.filters import FilterEngine

app = FastAPI(title="Review Intelligence API", description="Dashboard APIs for review sentiment analysis")

# Enable CORS for dashboard frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="web/static"), name="static")

db = Database()
insights_gen = InsightGenerator(db)
chat_engine = ChatEngine(db)
filter_engine = FilterEngine(db)

class ChatRequest(BaseModel):
    query: str
    location_id: Optional[str] = None
    use_semantic: bool = True

@app.get("/", response_class=HTMLResponse)
async def home():
    return FileResponse("web/templates/index.html")

@app.get("/api/locations")
async def get_locations():
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT location_id FROM reviews")
    locations = [row[0] for row in cursor.fetchall()]
    conn.close()
    return {"locations": locations}

@app.get("/api/insights/{location_id}")
async def get_insights(location_id: str, regenerate: bool = False):
    if regenerate:
        insights = insights_gen.generate_insights(location_id)
    else:
        insights = insights_gen.get_cached_insights(location_id)
        if not insights:
            insights = insights_gen.generate_insights(location_id)
    return insights

@app.post("/api/chat")
async def chat(request: ChatRequest):
    try:
        response = chat_engine.chat(request.query, request.location_id, request.use_semantic)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/reviews")
async def get_reviews(
    location_id: Optional[str] = None,
    min_rating: Optional[float] = Query(None, ge=1, le=5),
    max_rating: Optional[float] = Query(None, ge=1, le=5),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    topics: Optional[str] = None,
    sentiment: Optional[str] = None,
    limit: int = Query(100, le=1000)
):
    topic_list = topics.split(',') if topics else None
    reviews = filter_engine.apply_filters(
        location_id=location_id, min_rating=min_rating, max_rating=max_rating,
        start_date=start_date, end_date=end_date, topics=topic_list,
        sentiment=sentiment, limit=limit
    )
    return {"reviews": reviews, "count": len(reviews)}

@app.get("/api/stats/{location_id}")
async def get_stats(location_id: str):
    reviews = db.get_reviews(location_id=location_id, limit=10000)
    if not reviews:
        return {"error": "No reviews found"}
    ratings = [r['rating'] for r in reviews if r.get('rating')]
    avg_rating = sum(ratings) / len(ratings) if ratings else 0
    return {
        "total_reviews": len(reviews),
        "average_rating": round(avg_rating, 2),
        "rating_distribution": {
            "1": len([r for r in ratings if r == 1]),
            "2": len([r for r in ratings if r == 2]),
            "3": len([r for r in ratings if r == 3]),
            "4": len([r for r in ratings if r == 4]),
            "5": len([r for r in ratings if r == 5])
        }
    }


# ============ DASHBOARD APIs ============

@app.get("/api/dashboard/summary")
async def get_dashboard_summary(location_id: Optional[str] = None):
    """Get complete dashboard summary with all key metrics"""
    conn = db.get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    
    # Base query filter
    location_filter = "WHERE r.location_id = ?" if location_id else ""
    params = [location_id] if location_id else []
    
    # Total reviews and average rating
    cursor.execute(f"""
        SELECT COUNT(*) as total, AVG(rating) as avg_rating 
        FROM reviews r {location_filter}
    """, params)
    stats = cursor.fetchone()
    
    # Sentiment breakdown
    cursor.execute(f"""
        SELECT e.sentiment, COUNT(*) as count
        FROM reviews r
        JOIN enrichments e ON r.review_id = e.review_id
        {location_filter}
        GROUP BY e.sentiment
    """, params)
    sentiment_data = {row['sentiment']: row['count'] for row in cursor.fetchall()}
    
    # Rating distribution
    cursor.execute(f"""
        SELECT rating, COUNT(*) as count
        FROM reviews r {location_filter}
        GROUP BY rating ORDER BY rating
    """, params)
    rating_dist = {int(row['rating']): row['count'] for row in cursor.fetchall()}
    
    # Top topics
    cursor.execute(f"""
        SELECT e.topics FROM reviews r
        JOIN enrichments e ON r.review_id = e.review_id
        {location_filter}
    """, params)
    topic_counter = Counter()
    for row in cursor.fetchall():
        if row['topics']:
            topics = json.loads(row['topics'])
            topic_counter.update(topics)
    
    conn.close()
    
    return {
        "total_reviews": stats['total'],
        "average_rating": round(stats['avg_rating'], 2) if stats['avg_rating'] else 0,
        "sentiment_breakdown": sentiment_data,
        "rating_distribution": rating_dist,
        "top_topics": [{"topic": t, "count": c} for t, c in topic_counter.most_common(10)],
        "generated_at": datetime.now().isoformat()
    }


@app.get("/api/dashboard/trends")
async def get_trends(location_id: Optional[str] = None, period: str = "month"):
    """Get rating and sentiment trends over time"""
    conn = db.get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    
    # Date grouping based on period
    if period == "day":
        date_format = "DATE(r.review_date)"
    elif period == "week":
        date_format = "strftime('%Y-W%W', r.review_date)"
    else:  # month
        date_format = "strftime('%Y-%m', r.review_date)"
    
    location_filter = "WHERE r.location_id = ?" if location_id else ""
    params = [location_id] if location_id else []
    
    # Rating trends
    cursor.execute(f"""
        SELECT {date_format} as period, 
               AVG(r.rating) as avg_rating,
               COUNT(*) as review_count
        FROM reviews r {location_filter}
        GROUP BY {date_format}
        ORDER BY period
    """, params)
    
    rating_trends = [
        {"period": row['period'], "avg_rating": round(row['avg_rating'], 2), "count": row['review_count']}
        for row in cursor.fetchall() if row['period']
    ]
    
    # Sentiment trends
    cursor.execute(f"""
        SELECT {date_format} as period,
               e.sentiment,
               COUNT(*) as count
        FROM reviews r
        JOIN enrichments e ON r.review_id = e.review_id
        {location_filter}
        GROUP BY {date_format}, e.sentiment
        ORDER BY period
    """, params)
    
    sentiment_by_period = {}
    for row in cursor.fetchall():
        if row['period']:
            if row['period'] not in sentiment_by_period:
                sentiment_by_period[row['period']] = {}
            sentiment_by_period[row['period']][row['sentiment']] = row['count']
    
    conn.close()
    
    return {
        "rating_trends": rating_trends,
        "sentiment_trends": [{"period": p, **s} for p, s in sentiment_by_period.items()]
    }


@app.get("/api/dashboard/topics")
async def get_topic_analysis(location_id: Optional[str] = None):
    """Get detailed topic analysis with sentiment correlation"""
    conn = db.get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    
    location_filter = "WHERE r.location_id = ?" if location_id else ""
    params = [location_id] if location_id else []
    
    cursor.execute(f"""
        SELECT e.topics, e.sentiment, e.sentiment_score, r.rating
        FROM reviews r
        JOIN enrichments e ON r.review_id = e.review_id
        {location_filter}
    """, params)
    
    topic_stats = {}
    for row in cursor.fetchall():
        if row['topics']:
            topics = json.loads(row['topics'])
            for topic in topics:
                if topic not in topic_stats:
                    topic_stats[topic] = {
                        "count": 0, "ratings": [], "sentiments": [],
                        "positive": 0, "negative": 0, "neutral": 0
                    }
                topic_stats[topic]["count"] += 1
                topic_stats[topic]["ratings"].append(row['rating'])
                topic_stats[topic][row['sentiment']] += 1
    
    conn.close()
    
    # Calculate averages and format response
    result = []
    for topic, stats in topic_stats.items():
        avg_rating = sum(stats["ratings"]) / len(stats["ratings"]) if stats["ratings"] else 0
        result.append({
            "topic": topic,
            "count": stats["count"],
            "avg_rating": round(avg_rating, 2),
            "sentiment_split": {
                "positive": stats["positive"],
                "negative": stats["negative"],
                "neutral": stats["neutral"]
            }
        })
    
    # Sort by count descending
    result.sort(key=lambda x: x["count"], reverse=True)
    return {"topics": result}


@app.get("/api/dashboard/reviews-by-topic/{topic}")
async def get_reviews_by_topic(topic: str, location_id: Optional[str] = None, limit: int = 20):
    """Get reviews filtered by a specific topic"""
    conn = db.get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    
    location_filter = "AND r.location_id = ?" if location_id else ""
    params = [f'%"{topic}"%'] + ([location_id] if location_id else [])
    
    cursor.execute(f"""
        SELECT r.*, e.sentiment, e.sentiment_score, e.topics, e.entities
        FROM reviews r
        JOIN enrichments e ON r.review_id = e.review_id
        WHERE e.topics LIKE ? {location_filter}
        ORDER BY r.review_date DESC
        LIMIT ?
    """, params + [limit])
    
    reviews = []
    for row in cursor.fetchall():
        review = dict(row)
        review['topics'] = json.loads(review['topics']) if review['topics'] else []
        review['entities'] = json.loads(review['entities']) if review['entities'] else []
        reviews.append(review)
    
    conn.close()
    return {"topic": topic, "reviews": reviews, "count": len(reviews)}


@app.get("/api/dashboard/sentiment-details")
async def get_sentiment_details(location_id: Optional[str] = None):
    """Get detailed sentiment analysis with score distribution"""
    conn = db.get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    
    location_filter = "WHERE r.location_id = ?" if location_id else ""
    params = [location_id] if location_id else []
    
    cursor.execute(f"""
        SELECT e.sentiment, e.sentiment_score, r.rating, r.review_text, r.review_id
        FROM reviews r
        JOIN enrichments e ON r.review_id = e.review_id
        {location_filter}
    """, params)
    
    sentiment_groups = {"positive": [], "negative": [], "neutral": []}
    score_distribution = []
    
    for row in cursor.fetchall():
        sentiment = row['sentiment']
        score_distribution.append(row['sentiment_score'])
        if sentiment in sentiment_groups:
            sentiment_groups[sentiment].append({
                "review_id": row['review_id'],
                "score": row['sentiment_score'],
                "rating": row['rating'],
                "text_preview": row['review_text'][:150] + "..." if len(row['review_text']) > 150 else row['review_text']
            })
    
    conn.close()
    
    # Get most extreme examples
    for sentiment in sentiment_groups:
        sentiment_groups[sentiment].sort(key=lambda x: abs(x['score']), reverse=True)
        sentiment_groups[sentiment] = sentiment_groups[sentiment][:5]  # Top 5 examples
    
    return {
        "summary": {
            "positive_count": len([s for s in score_distribution if s > 0.3]),
            "negative_count": len([s for s in score_distribution if s < -0.3]),
            "neutral_count": len([s for s in score_distribution if -0.3 <= s <= 0.3]),
            "avg_score": round(sum(score_distribution) / len(score_distribution), 2) if score_distribution else 0
        },
        "examples": sentiment_groups
    }


@app.get("/api/dashboard/recent-reviews")
async def get_recent_reviews(location_id: Optional[str] = None, limit: int = 10):
    """Get most recent reviews with enrichments"""
    conn = db.get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    
    location_filter = "WHERE r.location_id = ?" if location_id else ""
    params = ([location_id] if location_id else []) + [limit]
    
    cursor.execute(f"""
        SELECT r.*, e.sentiment, e.sentiment_score, e.topics, e.entities
        FROM reviews r
        JOIN enrichments e ON r.review_id = e.review_id
        {location_filter}
        ORDER BY r.review_date DESC
        LIMIT ?
    """, params)
    
    reviews = []
    for row in cursor.fetchall():
        review = dict(row)
        review['topics'] = json.loads(review['topics']) if review['topics'] else []
        review['entities'] = json.loads(review['entities']) if review['entities'] else []
        reviews.append(review)
    
    conn.close()
    return {"reviews": reviews}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
