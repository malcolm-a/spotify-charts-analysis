from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
from dateutil.rrule import rrule, DAILY
import db.connection
from db.models import Artist, Song, artist_song
from db.schema import ensure_schema_exists, drop_and_create_schema
from io import StringIO
import pandas as pd
from pathlib import Path
import re
import requests
import urllib3
import concurrent.futures
import time
from tqdm import tqdm
from sqlalchemy import text, inspect, MetaData, Table, Column, String, create_engine

# Disables tls warnings in the console when fetching data
urllib3.disable_warnings()


def fetch_artists():
    # Ensure schema exists before fetching
    ensure_schema_exists()
    
    url = 'http://www.kworb.net/spotify/artists.html'
    try:
        response = requests.get(url, verify=False)
        response.encoding = 'utf-8'
        
        artist_links = BeautifulSoup(response.text, 'html.parser').select('table.addpos tbody tr td.text div a')
        
        artists_data = []
        for link in artist_links:
            artist_name = link.text.strip()
            
            spotify_id_match = re.search(r'/spotify/artist/([^_]+)_', link.get('href'))
            spotify_id = spotify_id_match.group(1) if spotify_id_match else None
            
            if spotify_id:
                artists_data.append({
                    'name': artist_name,
                    'spotify_id': spotify_id
                })
            
        # Use ORM to add artists
        session = db.connection.get_session()
        for artist_data in artists_data:
            artist = Artist(
                spotify_id=artist_data['spotify_id'],
                name=artist_data['name']
            )
            session.merge(artist)  # Use merge instead of add to handle duplicates
        
        session.commit()
        session.close()
        print(f"Fetched {len(artists_data)} artists in the database")
    
    except Exception as e:
        print(f"Error fetching artists: {e}")


def fetch_artist_songs(artist_id):
    """Fetch songs for a single artist"""
    try:
        url = f'http://www.kworb.net/spotify/artist/{artist_id}_songs.html'
        response = requests.get(url, verify=False, timeout=10)
        response.encoding = 'utf-8'
        
        soup = BeautifulSoup(response.text, 'html.parser')
        artist_songs = soup.select('table.addpos tbody tr td.text div a')
        
        songs = []
        for song in artist_songs:
            song_name = song.text.strip()
            href = song.get('href')
            spotify_id_match = re.search(r'/track/([^_]+)', href)
            spotify_id = spotify_id_match.group(1) if spotify_id_match else None
        
            if spotify_id:
                songs.append({
                    'name': song_name,
                    'song_id': spotify_id
                })
        
        return songs, artist_id
    except Exception as e:
        print(f"Error fetching songs for artist {artist_id}: {e}")
        return [], artist_id  # Return empty list on error


def fetch_artists_songs_batch(batch_size=50, max_workers=10):
    """Fetch songs for all artists in batches with concurrent requests"""
    try:
        # Do NOT drop schema here, just ensure it exists
        ensure_schema_exists()
        
        # Get all artist IDs from database
        session = db.connection.get_session()
        artists = session.query(Artist).all()
        spotify_ids = [artist.spotify_id for artist in artists]
        session.close()
        
        print(f"Found {len(spotify_ids)} artists to process")
        
        # Process in batches
        for i in range(0, len(spotify_ids), batch_size):
            batch = spotify_ids[i:i+batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(spotify_ids)+batch_size-1)//batch_size}")
            
            # Use concurrent.futures to make requests in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all requests and map them to their artist_ids
                future_to_artist = {executor.submit(fetch_artist_songs, artist_id): artist_id for artist_id in batch}
                
                # Process results as they complete
                for future in tqdm(concurrent.futures.as_completed(future_to_artist), total=len(batch), desc="Artists"):
                    artist_id = future_to_artist[future]
                    try:
                        songs, artist_id = future.result()
                        
                        # Process each batch of songs and add to database
                        if songs:
                            session = db.connection.get_session()
                            
                            # Get or create the artist
                            artist = session.query(Artist).filter_by(spotify_id=artist_id).first()
                            if not artist:
                                continue  # Skip if artist doesn't exist
                                
                            # Add songs and relationships
                            for song_data in songs:
                                # Add song if not exists
                                song = session.query(Song).filter_by(song_id=song_data['song_id']).first()
                                if not song:
                                    song = Song(
                                        song_id=song_data['song_id'],
                                        name=song_data['name']
                                    )
                                    session.add(song)
                                
                                # Add relationship if not exists
                                relationship_exists = session.query(artist_song).filter_by(
                                    artist_id=artist_id,
                                    song_id=song_data['song_id']
                                ).count() > 0
                                
                                if not relationship_exists:
                                    session.execute(
                                        artist_song.insert().values(
                                            artist_id=artist_id,
                                            song_id=song_data['song_id']
                                        )
                                    )
                                    
                            session.commit()
                            session.close()
                            
                    except Exception as e:
                        print(f"Artist {artist_id} generated an exception: {e}")
            
            # Wait a bit between batches to avoid overwhelming the server
            time.sleep(1)
        
        print(f"Successfully fetched and saved songs for {len(spotify_ids)} artists")
        
    except Exception as e:
        print(f"Error in main fetch_artists_songs function: {e}")
 
def fetch_kworb_charts(source: str, target: str = 'sql', start: date = None, end: date = None):
    """fetches chart data from kworb.net

    Args:
        source (str): kworb.net charts source (apple, itunes, or radio)
        target (str, optional): in which format to save the data. Defaults to 'sql'.
        start (date, optional): first date to get the charts from. Defaults to yesterday.
        end (date, optional): last date to get the charts from. Defaults to yesterday.

    Raises:
        ValueError: if the source is not valid
        ValueError: if the target is not valid
    """
    # start and end are yesterday by default to fetch the latest data
    start = datetime.today() - timedelta(days=1) if not start or start >= datetime.today() else start
    end = datetime.today() - timedelta(days=1) if not end or end >= datetime.today() else end
    
    # match sources to their actual paths on kworb.net
    match source:
        case 'apple':
            resource_path = 'apple_songs/archive'
        case 'itunes':
            resource_path = 'ww/archive'
        case 'radio':
            resource_path = 'radio/archives'
        case _:
            raise ValueError(f"Source {source} not found.")
    
    # matches our target parameter to either save as csv or in our db
    match target:
        case 'csv':
            # create the path if it doesn't exist and create save_data fn to save the csv there
            save_path = f"data/charts/{source}"
            Path(save_path).mkdir(parents=True, exist_ok=True)
            def save_data(dt: date, df: pd.DataFrame):
                df.to_csv(f"{save_path}/{dt}.csv")  
                print(f"Inserted data from {dt} into {save_path}/{dt}.csv")  
        case 'sql':
            # connect to the db and create save_data fn to save data in the corresponding table
            engine = db.connection.get_engine()
            def save_data(dt: date, df: pd.DataFrame):
                df.to_sql(f'chart_data_{source}', engine, if_exists="replace", index=False)
                print(f"Inserted data from {dt} into chart_data_{source}")
        case _:
            raise ValueError(f"Target {target} is invalid.")
                      
    # fetch and save each date between start date and end date
    for current_date in rrule(DAILY, dtstart=start, until=end):
        dt = current_date.strftime("%Y%m%d")
        with requests.get(f'http://www.kworb.net/{resource_path}/{dt}.html', verify=False) as response:
            response.encoding = 'utf-8'
            html_tables = pd.read_html(StringIO(response.text), encoding='utf-8')[0]
            html_tables['date'] = current_date
            save_data(dt, html_tables)


# Main execution
if __name__ == "__main__":
    # Only drop and recreate schema if explicitly requested
    # drop_and_create_schema()  # Uncomment if you want to reset database
    
    # Fetch artists first, then songs
    fetch_artists()
    fetch_artists_songs_batch()