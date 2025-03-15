from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
from dateutil.rrule import rrule, DAILY
import db.connection
from io import StringIO
import pandas as pd
from pathlib import Path
import re
import requests
import urllib3
import concurrent.futures
import time
from tqdm import tqdm
from sqlalchemy import text

# Disables tls warnings in the console when fetching data
urllib3.disable_warnings()


def fetch_artists():
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
            
            artists_data.append({
                'name': artist_name,
                'spotify_id': spotify_id
            })
        engine = db.connection.get_engine()
        
        pd.DataFrame(artists_data).to_sql('artist', engine, if_exists='replace', index=False)
        print("Fetched artists in the database")
    
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
        
            songs.append({
                'name': song_name,
                'song_id': spotify_id,
                'artist_id': artist_id
            })
        
        return songs
    except Exception as e:
        print(f"Error fetching songs for artist {artist_id}: {e}")
        return []  # Return empty list on error

def fetch_artists_songs(batch_size=50, max_workers=10):
    """Fetch songs for all artists in batches with concurrent requests"""
    try:
        # Get all artist IDs from database
        session = db.connection.get_session()
        result = session.execute(text("SELECT spotify_id FROM artist"))
        spotify_ids = [row[0] for row in result]
        session.close()
        
        print(f"Found {len(spotify_ids)} artists to process")
        engine = db.connection.get_engine()
        
        # Process in batches
        for i in range(0, len(spotify_ids), batch_size):
            batch = spotify_ids[i:i+batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(spotify_ids)+batch_size-1)//batch_size}")
            
            # Use concurrent.futures to make requests in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all requests and map them to their artist_ids
                future_to_artist = {executor.submit(fetch_artist_songs, artist_id): artist_id for artist_id in batch}
                
                # Process results as they complete
                batch_songs = []
                for future in tqdm(concurrent.futures.as_completed(future_to_artist), total=len(batch), desc="Artists"):
                    artist_id = future_to_artist[future]
                    try:
                        songs = future.result()
                        batch_songs.extend(songs)
                    except Exception as e:
                        print(f"Artist {artist_id} generated an exception: {e}")
            
            # Save this batch to the database
            if batch_songs:
                print(f"Saving batch of {len(batch_songs)} songs to database")
                df = pd.DataFrame(batch_songs)
                df.to_sql('song', engine, if_exists='append', index=False)
            
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


# example usages
fetch_kworb_charts('apple', 'csv') # saves yesterday's apple music charts to a local csv file
#fetch_kworb_charts('itunes', 'sql', start=datetime(year=2025, month=3, day=1)) # saves itunes charts from 2025/03/01 to today in the db
