"""
Seed location coordinates for map display.
Run this after ingestion to populate location metadata.
"""
import sys
sys.path.insert(0, 'src')
from storage.db import Database

# Known airport rental locations with coordinates
LOCATION_COORDS = {
    "LAX": {
        "name": "Avis Car Rental - LAX Airport",
        "latitude": 33.9499276,
        "longitude": -118.3760274,
        "address": "9217 Airport Blvd, Los Angeles, CA 90045"
    },
    "JFK": {
        "name": "Avis Car Rental - JFK Airport", 
        "latitude": 40.6413111,
        "longitude": -73.7781391,
        "address": "JFK International Airport, Queens, NY 11430"
    },
    # Add more locations as needed
}

def seed_locations():
    db = Database()
    
    # Get existing location_ids from reviews using SQLAlchemy
    from sqlalchemy import text
    conn = db.get_connection()
    result = conn.execute(text("SELECT DISTINCT location_id FROM reviews"))
    existing_locations = [row[0] for row in result.fetchall()]
    conn.close()
    
    print(f"Found {len(existing_locations)} locations in database: {existing_locations}")
    
    for loc_id in existing_locations:
        if loc_id in LOCATION_COORDS:
            coords = LOCATION_COORDS[loc_id]
            db.upsert_location(
                location_id=loc_id,
                name=coords["name"],
                latitude=coords["latitude"],
                longitude=coords["longitude"],
                address=coords["address"]
            )
            print(f"✓ Seeded coordinates for {loc_id}: ({coords['latitude']}, {coords['longitude']})")
        else:
            print(f"⚠ No coordinates found for {loc_id} - add to LOCATION_COORDS dict")
    
    print("\nDone! Locations table updated.")

if __name__ == "__main__":
    seed_locations()
