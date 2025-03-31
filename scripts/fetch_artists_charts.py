from db.connection import get_session
from db.models import Artist, Country, Artist_charts
import pandas as pd
import requests
import time
from dotenv import load_dotenv
import os
import pylast
import json
from datetime import datetime
from sqlalchemy import func

load_dotenv()
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
LASTFM_API_SECRET = os.getenv("LASTFM_API_SECRET")
LASTFM_USERNAME = os.getenv("LASTFM_USERNAME")
LASTFM_PASSWORD = pylast.md5(os.getenv("LASTFM_PASSWORD"))

def fetch_mbid(artist, retries=3):
    """
    Fetches the MBID for a given artist from the MusicBrainz API.
    Returns a tuple (artist.spotify_id, mbid) on success,
    ("404") if not found, or None if not retrievable.
    Retries when a 503 error is encountered.
    """
    url = f"https://musicbrainz.org/ws/2/url?resource=https://open.spotify.com/artist/{artist.spotify_id}&fmt=json&inc=artist-rels"
    headers = {
        "User-Agent": "SAEDataVis/1.0 (mailto:malcolm.aridory@etudiant.univ-reims.fr)"
    }
    
    try:
        print(f"Fetching MBID for artist {artist.spotify_id}")
        response = requests.get(url, headers=headers, timeout=10, verify=False)
        
        if response.status_code == 404:
            print(f"404 error for {artist.spotify_id}")
            return artist.spotify_id, "404"
        
        if response.status_code == 503:
            if retries > 0:
                print(f"503 error for {artist.spotify_id}, retrying ({retries} left)...")
                time.sleep(5)
                return fetch_mbid(artist, retries - 1)
            else:
                print(f"503 error for {artist.spotify_id} after retries")
                return None
        
        response.raise_for_status()
        data = response.json()
        
        if not data.get("relations"):
            print(f"No relations found for {artist.spotify_id}")
            return None
            
        for relation in data.get("relations", []):
            if "artist" in relation and "id" in relation["artist"]:
                artist_mbid = relation["artist"]["id"]
                return artist.spotify_id, artist_mbid
                
        print(f"No artist relation found for {artist.spotify_id}")
        return None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"404 error for {artist.spotify_id}")
            return artist.spotify_id, "404"
        print(f"HTTP error for {artist.spotify_id}: {e}")
        return None
    except Exception as e:
        print(f"Error for {artist.spotify_id}: {e}")
        return None


def update_artist_mbids(skip_404=True):
    """
    Fetches and updates the MBIDs for all artists in the database where MBID is NULL.

    Args:
        skip_404 (bool): if True, skips artists that returned a 404 error on previous try
    """
    session = get_session()
    
    # Ensure data directory exists
    os.makedirs("db/data", exist_ok=True)
    
    # Load problematic artists or initialize with default permanent problematic artist
    error_file = "db/data/artist_errors.json"
    try:
        with open(error_file, "r") as f:
            problematic_artists = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        problematic_artists = {
            "2qk9voo8llSGYcZ6xrBzKx": {
                "error_type": "other",
                "error_message": "Permanently problematic artist",
                "timestamp": datetime.now().isoformat()
            }
        }
        with open(error_file, "w") as f:
            json.dump(problematic_artists, f, indent=2)
    
    artists_404 = {art_id for art_id, data in problematic_artists.items() if data.get("error_type") == "404"}
    
    artists = session.query(Artist).filter(Artist.mbid.is_(None)).all()
    if not artists:
        print("No artists with missing MBIDs found.")
        return

    artists_count = len(artists)
    print(f"Found {artists_count} artists with missing MBIDs. Updating...")

    updated_count = 0
    new_problematic = {}
    
    # Preserve permanent problematic artists
    for art_id, data in problematic_artists.items():
        if data.get("error_type") == "other":
            new_problematic[art_id] = data

    for artist in artists:
        # skip permanently problematic artists
        if artist.spotify_id in problematic_artists and problematic_artists[artist.spotify_id].get("error_type") == "other":
            print(f"Skipping permanently problematic artist {artist.spotify_id}")
            continue
            
        # skip 404 artists if skip_404 is True
        if skip_404 and artist.spotify_id in artists_404:
            print(f"Skipping 404 artist {artist.spotify_id}")
            new_problematic[artist.spotify_id] = problematic_artists[artist.spotify_id]
            continue
                
        try:
            result = fetch_mbid(artist)
            if not result:
                new_problematic[artist.spotify_id] = {
                    "error_type": "other",
                    "error_message": "No relation found or server error",
                    "timestamp": datetime.now().isoformat()
                }
                continue
                
            spotify_id, mbid = result
            
            if mbid == "404":
                new_problematic[spotify_id] = {
                    "error_type": "404",
                    "error_message": "Artist not found in MusicBrainz",
                    "timestamp": datetime.now().isoformat()
                }
                continue
   
            artist.mbid = mbid
            session.commit()
            updated_count += 1
            print(f"Updated {updated_count}/{artists_count} artists ({round(updated_count/artists_count*100, 2)}%).")
            
        except Exception as e:
            new_problematic[artist.spotify_id] = {
                "error_type": "other",
                "error_message": str(e),
                "timestamp": datetime.now().isoformat()
            }

        time.sleep(1)
    
    # update problematic artists file
    with open(error_file, "w") as f:
        json.dump(new_problematic, f, indent=2)
    recovered = sum(1 for art_id in problematic_artists if art_id not in new_problematic)
    
    session.close()
    print(f"\nFinal results:")
    print(f"- Updated MBIDs for {updated_count} artists")
    print(f"- Recovered {recovered} previously problematic artists!")


def insert_countries():
    """
    Inserts countries into the database from a CSV file.
    """
    session = get_session()
    countries = pd.read_csv("db/data/countries.csv")[["name", "alpha-2", "region"]]
    for _, row in countries.iterrows():
        country = Country(
            country_code=row["alpha-2"],
            country_name=row["name"],
            region=row["region"]
        )
        session.add(country)
    
    session.commit()
    session.close()
    print("\nSuccessfully inserted countries")
    
    
def fetch_artists_charts():
    """
    Fetches artists charts per country from last.fm's API.
    """
    session = get_session()
    countries = session.query(Country).all()
    today = datetime.today().date()
    
    for country in countries:
        print(f"Fetching artists charts for {country.country_name}")
        
        # Check if we already have data for this country today
        existing = session.query(Artist_charts).filter(
            Artist_charts.country_code == country.country_code,
            func.date(Artist_charts.date) == today
        ).first()
        
        if existing:
            print(f"Skipping {country.country_name}: already fetched today")
            continue
            
        top_artists = requests.get(f"http://ws.audioscrobbler.com/2.0/?method=geo.gettopartists&country={country.country_name.split(',')[0]}&api_key={LASTFM_API_KEY}&format=json&limit=100").json()
        
        # join artists by mbid and insert into artist_charts
        rank = 0
        added_artists = set()  # Keep track of artists we've already added
        if top_artists.get("topartists") and top_artists["topartists"].get("artist"):
            for artist in top_artists["topartists"]["artist"]:
                rank += 1
                mbid = artist["mbid"]
                if mbid:
                    artist_obj = session.query(Artist).filter(Artist.mbid == mbid).first()
                    if artist_obj and artist_obj.spotify_id not in added_artists:
                        artist_chart = Artist_charts(
                            artist_id=artist_obj.spotify_id,
                            country_code=country.country_code,
                            rank=rank,
                            date=datetime.today()
                        )
                        session.add(artist_chart)
                        added_artists.add(artist_obj.spotify_id)

        session.commit()
        time.sleep(1)

    session.close()
    
if __name__ == "__main__":
    #update_artist_mbids()
    #insert_countries()
    fetch_artists_charts()