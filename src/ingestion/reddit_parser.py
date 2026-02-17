"""
Reddit Review Parser - Handles parsing Reddit posts and comments into review format
Converts Reddit discussions about car rental brands into the standard review schema.
"""
import json
import hashlib
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path


class RedditParser:
    """Parser for Reddit posts and comments about car rental brands"""
    
    # Map subreddits to general topics
    SUBREDDIT_TOPICS = {
        "travel": ["travel", "vacation"],
        "travelhacks": ["travel", "tips"],
        "scams": ["scams", "fraud"],
        "cars": ["vehicle", "car_quality"],
        "iama": ["customer_service", "insider"],
        "unitedairlines": ["travel", "partnerships"],
    }
    
    def __init__(self):
        pass
    
    def parse_reddit_data(self, data: Dict, brand: str = None, 
                          scrape_date: datetime = None) -> List[Dict]:
        """
        Parse Reddit data structure into normalized reviews.
        
        Supports two formats:
        1. Single subreddit:
        {
            "subreddit": "travel",
            "posts": [...]
        }
        
        2. Multiple subreddits (from JSON file):
        {
            "brand": "avis",
            "subreddits": [
                {"subreddit": "travel", "posts": [...]},
                {"subreddit": "cars", "posts": [...]}
            ]
        }
        """
        if scrape_date is None:
            scrape_date = datetime.now()
        
        # Get brand from data if not provided
        if brand is None:
            brand = data.get("brand")
            
        reviews = []
        
        # Handle multi-subreddit format
        if "subreddits" in data:
            for sub_data in data["subreddits"]:
                subreddit = sub_data.get("subreddit", "unknown")
                posts = sub_data.get("posts", [])
                for post in posts:
                    post_reviews = self._parse_post(post, subreddit, brand, scrape_date)
                    reviews.extend(post_reviews)
        else:
            # Single subreddit format
            subreddit = data.get("subreddit", "unknown")
            posts = data.get("posts", [])
            for post in posts:
                post_reviews = self._parse_post(post, subreddit, brand, scrape_date)
                reviews.extend(post_reviews)
        
        return reviews
    
    def _parse_post(self, post: Dict, subreddit: str, brand: str,
                    scrape_date: datetime) -> List[Dict]:
        """Parse a single Reddit post into reviews"""
        reviews = []
        title = post.get("title", "")
        votes = post.get("votes", 0)
        comments = post.get("comments", [])
        
        # Generate unique ID from title
        post_id = hashlib.md5(title.encode()).hexdigest()[:12]
        
        # Determine sentiment from votes and title keywords
        sentiment_hints = self._extract_sentiment_hints(title, comments)
        
        # Create a review for each substantive comment
        for i, comment in enumerate(comments):
            if len(comment) < 20:  # Skip very short comments
                continue
                
            review_id = f"reddit_{post_id}_{i}"
            
            # Estimate rating from comment sentiment (1-5 scale)
            rating = self._estimate_rating(comment, sentiment_hints)
            
            review = {
                "location_id": "ALL",  # Reddit posts are typically not location-specific
                "source": "reddit",
                "brand": brand,
                "is_competitor": False,
                "review_id": review_id,
                "rating": rating,
                "review_text": comment,
                "reviewer_name": f"r/{subreddit} user",
                "reviewer_type": "reddit_user",
                "review_date": scrape_date.strftime("%Y-%m-%d"),
                "relative_date": "from reddit",
                "language": "en",
                "raw_json": json.dumps({
                    "post_title": title,
                    "subreddit": subreddit,
                    "votes": votes,
                    "comment_index": i
                })
            }
            reviews.append(review)
        
        return reviews
    
    def _extract_sentiment_hints(self, title: str, comments: List[str]) -> Dict:
        """Extract sentiment hints from title and comments"""
        title_lower = title.lower()
        all_text = title_lower + " " + " ".join(c.lower() for c in comments)
        
        negative_keywords = [
            "bad", "worst", "terrible", "awful", "scam", "avoid", "never",
            "horrible", "rude", "hidden fees", "overcharge", "complaint",
            "warning", "beware", "don't", "do not", "annoyed", "frustrated"
        ]
        
        positive_keywords = [
            "great", "excellent", "best", "recommend", "good", "helpful",
            "smooth", "easy", "bargain", "deal", "satisfied", "happy"
        ]
        
        neg_count = sum(1 for kw in negative_keywords if kw in all_text)
        pos_count = sum(1 for kw in positive_keywords if kw in all_text)
        
        return {
            "negative_signals": neg_count,
            "positive_signals": pos_count,
            "overall": "negative" if neg_count > pos_count else "positive" if pos_count > neg_count else "neutral"
        }
    
    def _estimate_rating(self, comment: str, sentiment_hints: Dict) -> float:
        """Estimate a 1-5 rating based on comment content"""
        comment_lower = comment.lower()
        
        # Strong negative indicators
        if any(kw in comment_lower for kw in ["scam", "worst", "terrible", "never again", "avoid"]):
            return 1.0
        
        # Moderate negative
        if any(kw in comment_lower for kw in ["bad", "annoyed", "frustrated", "hidden fees", "overcharge"]):
            return 2.0
        
        # Strong positive
        if any(kw in comment_lower for kw in ["excellent", "best", "highly recommend", "great deal"]):
            return 5.0
        
        # Moderate positive
        if any(kw in comment_lower for kw in ["good", "decent", "satisfied", "helpful"]):
            return 4.0
        
        # Use overall sentiment as fallback
        if sentiment_hints["overall"] == "negative":
            return 2.5
        elif sentiment_hints["overall"] == "positive":
            return 3.5
        
        return 3.0  # Neutral default


def parse_reddit_text_block(text_block: str, brand: str = "avis") -> List[Dict]:
    """
    Parse a text block of Reddit data (like the user provided) into structured format.
    
    This handles the raw text format:
    r/subreddit
    Post Title
    Votes: X | Comments: Y
    Comments:
    "comment1"
    "comment2"
    """
    parser = RedditParser()
    all_reviews = []
    
    lines = text_block.strip().split('\n')
    current_subreddit = None
    current_post = None
    posts_by_subreddit = {}
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        # Detect subreddit header (r/something)
        if line.startswith('r/') and not line.startswith('r/'):
            current_subreddit = line[2:].lower()
            if current_subreddit not in posts_by_subreddit:
                posts_by_subreddit[current_subreddit] = []
            i += 1
            continue
        
        # Detect subreddit in format "r/SubredditName"
        if line.startswith('r/'):
            current_subreddit = line[2:].split()[0].lower()
            if current_subreddit not in posts_by_subreddit:
                posts_by_subreddit[current_subreddit] = []
            i += 1
            continue
        
        # Detect votes line
        if 'Votes:' in line and '|' in line:
            # Previous line was the title
            if i > 0 and current_subreddit:
                title = lines[i-1].strip()
                votes_part = line.split('|')[0]
                votes = int(''.join(filter(str.isdigit, votes_part)) or '0')
                
                current_post = {
                    "title": title,
                    "votes": votes,
                    "comments": []
                }
                posts_by_subreddit[current_subreddit].append(current_post)
            i += 1
            continue
        
        # Detect comment line (starts with quote)
        if line.startswith('"') and current_post is not None:
            # Extract comment text between quotes
            comment = line.strip('"')
            if comment:
                current_post["comments"].append(comment)
            i += 1
            continue
        
        i += 1
    
    # Convert to reviews
    scrape_date = datetime.now()
    for subreddit, posts in posts_by_subreddit.items():
        data = {"subreddit": subreddit, "posts": posts}
        reviews = parser.parse_reddit_data(data, brand=brand, scrape_date=scrape_date)
        all_reviews.extend(reviews)
    
    return all_reviews


if __name__ == "__main__":
    # Test with sample data
    sample = {
        "subreddit": "travel",
        "posts": [
            {
                "title": "Avis car rental question",
                "votes": 12,
                "comments": [
                    "I'm planning a trip soon. What are the most important things to consider?",
                    "Make sure to check their insurance policies!"
                ]
            }
        ]
    }
    
    parser = RedditParser()
    reviews = parser.parse_reddit_data(sample, brand="avis")
    print(f"Parsed {len(reviews)} reviews")
    for r in reviews:
        print(f"  - {r['review_id']}: {r['review_text'][:50]}...")
