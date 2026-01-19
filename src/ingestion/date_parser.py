from datetime import datetime, timedelta
import re

def parse_relative_date(relative_date: str, scrape_date: datetime = None) -> str:
    """Convert relative date strings to ISO format dates"""
    if not scrape_date:
        scrape_date = datetime.now()
    
    relative_date = relative_date.lower().strip()
    
    # Patterns
    if 'today' in relative_date or 'just now' in relative_date:
        return scrape_date.isoformat()
    
    if 'yesterday' in relative_date:
        return (scrape_date - timedelta(days=1)).isoformat()
    
    # Extract number and unit
    match = re.search(r'(\d+)\s*(day|week|month|year)s?\s*ago', relative_date)
    if match:
        num = int(match.group(1))
        unit = match.group(2)
        
        if unit == 'day':
            return (scrape_date - timedelta(days=num)).isoformat()
        elif unit == 'week':
            return (scrape_date - timedelta(weeks=num)).isoformat()
        elif unit == 'month':
            return (scrape_date - timedelta(days=num * 30)).isoformat()
        elif unit == 'year':
            return (scrape_date - timedelta(days=num * 365)).isoformat()
    
    # Match "a week ago", "a month ago"
    match = re.search(r'a\s+(day|week|month|year)\s*ago', relative_date)
    if match:
        unit = match.group(1)
        if unit == 'day':
            return (scrape_date - timedelta(days=1)).isoformat()
        elif unit == 'week':
            return (scrape_date - timedelta(weeks=1)).isoformat()
        elif unit == 'month':
            return (scrape_date - timedelta(days=30)).isoformat()
        elif unit == 'year':
            return (scrape_date - timedelta(days=365)).isoformat()
    
    # Default to scrape date if can't parse
    return scrape_date.isoformat()
