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

load_dotenv()
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
LASTFM_API_SECRET = os.getenv("LASTFM_API_SECRET")
LASTFM_USERNAME = os.getenv("LASTFM_USERNAME")
LASTFM_PASSWORD = pylast.md5(os.getenv("LASTFM_PASSWORD"))

# fetch MBID from MusicBrainz API ----- apparently fetches the wrong mbid (resource instead of artist) -> needs fixing
def fetch_mbid(artist):
    """
    Fetches the MBID for a given artist from the MusicBrainz API.
    :param artist: An Artist object from the database.
    :return: A tuple of (artist.spotify_id, mbid) if successful, otherwise None.
    """
    url = f"https://musicbrainz.org/ws/2/url?resource=https://open.spotify.com/artist/{artist.spotify_id}&fmt=json"
    headers = {
        "User-Agent": "SAEDataVis/1.0 (mailto:your_email@example.com)"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        mbid = data.get("id")
        return artist.spotify_id, mbid
    except Exception as e:
        print(f"Error fetching MBID for artist {artist.spotify_id}: {e}")
        return None


def update_artist_mbids():
    """
    Fetches and updates the MBIDs for all artists in the database where MBID is NULL.
    """
    session = get_session()
    
    artists = session.query(Artist).filter(Artist.mbid.is_(None)).all()
    if not artists:
        print("No artists with missing MBIDs found.")
        return

    artists_count = len(artists)
    print(f"Found {artists_count} artists with missing MBIDs. Updating...")

    updated_count = 0
    for artist in artists:
        result = fetch_mbid(artist)
        if result:
            _, mbid = result
            artist.mbid = mbid
            session.commit()
            updated_count += 1
            print(f"Updated {round(updated_count/artists_count*100, 2)}% of artists with MBIDs.")
        time.sleep(1)
            
    session.close()
    print(f"Updated MBIDs for {updated_count} artists.")

def insert_countries():
    """
    Inserts countries into the database from a CSV file
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
    Fetches artists charts per country from last.fm's api
    """
    
    session = get_session()
    
    countries = session.query(Country).all()
    
    for country in countries:
        print(f"Fetching artists charts for {country.country_name}")
        top_artists = requests.get(f"http://ws.audioscrobbler.com/2.0/?method=geo.gettopartists&country={country.country_name}&api_key={LASTFM_API_KEY}&format=json&limit=100").json()
        
        # join artists by mbid and insert into artist_charts
        for artist in top_artists["topartists"]["artist"]:
            mbid = artist["mbid"]
            if mbid:
                artist_obj = session.query(Artist).filter(Artist.mbid == mbid).first()
                if artist_obj:
                    artist_chart = Artist_charts(
                        artist_id=artist_obj.spotify_id,
                        country_code=country.country_code,
                        rank=artist["rank"],
                        date = datetime.today()
                    )
                    session.add(artist_chart)

        session.commit()
        time.sleep(1)

    session.close()
    
if __name__ == "__main__":
    #update_artist_mbids()
    #insert_countries()
    fetch_artists_charts()