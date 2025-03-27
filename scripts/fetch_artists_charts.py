from db.connection import get_session
from db.models import Artist, Country
import pandas as pd
import requests
import time

# Function to fetch MBID from MusicBrainz API
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

# Main function to update MBIDs in the database
def update_artist_mbids():
    """
    Fetches and updates the MBIDs for all artists in the database where MBID is NULL.
    """
    session = get_session()
    
    # Query artists where MBID is NULL
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
            spotify_id, mbid = result
            # Update the artist's MBID in the database
            artist.mbid = mbid
            session.commit()
            updated_count += 1
            print(f"Updated {round(updated_count/artists_count*100, 2)}% of artists with MBIDs.")
        time.sleep(1)
            
    session.close()
    print(f"Updated MBIDs for {updated_count} artists.")

def insert_countries():
    """
    Inserts countries into the database.
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
    
# Run the update process
if __name__ == "__main__":
    #update_artist_mbids()
    insert_countries()