from bs4 import BeautifulSoup
import db.connection
from db.models import Artist, Artist_stats
from db.schema import ensure_schema_exists
import re
import requests
import urllib3
import concurrent.futures
import time
from tqdm import tqdm
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
import pandas as pd
from io import StringIO

urllib3.disable_warnings()

def parse_number(text):
    if not text:
        return None
    return float(text.replace(",", ""))

def fetch_artist_stats(artist_id):
    try:
        with requests.get(f'https://www.kworb.net/spotify/artist/{artist_id}_songs.html', verify=False) as response:
            html_tables = pd.read_html(StringIO(response.text), encoding='utf-8', extract_links='all', header=0)[0].to_dict()
            

        streams_total = html_tables[('Total', None)][0][0]
        daily_total = html_tables[('Total', None)][1][0] 

        return {
            'total_streams': streams_total,
            'daily_streams': daily_total,  
        }
    
    except Exception as e:
        print(f"Erreur pour fecth les stats de l'artiste {artist_id}: {e}")
        return None

def fetch_listeners():
    try:
        url = "https://kworb.net/spotify/listeners.html"
        response = requests.get(url, verify=False)
        response.encoding = "utf-8"
        soup = BeautifulSoup(response.text, "html.parser")

        listeners_data = []
        rows = soup.select("table.addpos tbody tr")
        
        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 2:
                artist_name = cols[0].text.strip()
                listeners = parse_number(cols[1].text.replace(",", ""))
                
                if artist_name and listeners:
                    listeners_data.append({
                        "artist_name": artist_name,
                        "listeners": listeners,
                        "date": datetime.now().date()
                    })

        return listeners_data

    except Exception as e:
        print(f"Erreur pour fetch listeners: {e}")
        return []


def fetch_artists_stats_batch(batch_size=50, max_workers=5):
    """Fetch artist statistics in batches with concurrent requests."""
    ensure_schema_exists()
    session = db.connection.get_session()
    
    try:
        artists = session.query(Artist).all()
        artist_ids = [artist.spotify_id for artist in artists]
        
        stats_to_insert = []
        listeners_data = fetch_listeners()
        listeners_map = {item['artist_name']: item['listeners'] for item in listeners_data}
        
        for i in tqdm(range(0, len(artist_ids), batch_size), desc="Processing artists"):
            batch = artist_ids[i:i + batch_size]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_artist = {
                    executor.submit(fetch_artist_stats, artist_id): artist_id 
                    for artist_id in batch
                }
                
                for future in concurrent.futures.as_completed(future_to_artist):
                    artist_id = future_to_artist[future]
                    try:
                        stats = future.result()
                        if stats:
                            artist = session.query(Artist).filter_by(spotify_id=artist_id).first()
                            if artist:
                                listeners = listeners_map.get(artist.name, None)
                                
                                stats_to_insert.append({
                                    'artist_id': artist_id,
                                    'total_streams': parse_number(stats['total_streams']),
                                    'daily_streams': parse_number(stats['daily_streams']),
                                    'listeners': listeners,
                                    'date': datetime.now().date()
                                })
                    except Exception as e:
                        print(f"Error processing artist {artist_id}: {e}")
            
            if stats_to_insert:
                stmt = insert(Artist_stats).values(stats_to_insert)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['artist_id', 'date'],
                    set_={
                        'total_streams': stmt.excluded.total_streams,
                        'daily_streams': stmt.excluded.daily_streams,
                        'listeners': stmt.excluded.listeners
                    }
                )
                session.execute(stmt)
                session.commit()
                stats_to_insert = []
            
            
    except Exception as e:
        session.rollback()
        print(f"Error in fetch_artists_stats_batch: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    fetch_artists_stats_batch()