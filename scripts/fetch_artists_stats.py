from bs4 import BeautifulSoup
import db.connection
from db.models import Artist, Artist_stats
from db.schema import ensure_schema_exists
import requests
import urllib3
import concurrent.futures
import time
from tqdm import tqdm
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import schedule
import logging
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('artist_stats.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

urllib3.disable_warnings()

retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)

def parse_number(value):
    if value is None or value == '-':
        return None
    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value)
    if isinstance(value, str):
        return float(value.replace(",", ""))
    return None

def fetch_artist_stats(artist_id):
    url = f'https://www.kworb.net/spotify/artist/{artist_id}_songs.html'
    try:
        response = http.get(url, verify=False, timeout=10)
        response.raise_for_status()
        
        if "No data available" in response.text:
            return None
            
        tables = pd.read_html(StringIO(response.text), encoding='utf-8')
        if not tables:
            return None
            
        df = tables[0]
        if 'Total' not in df.columns:
            return None
            
        streams_total = parse_number(df.loc[0, 'Total'])
        daily_total = parse_number(df.loc[1, 'Total'])

        return {
            'total_streams': streams_total,
            'daily_streams': daily_total,  
        }
    
    except Exception as e:
        logger.debug(f"Erreur stats artiste {artist_id}: {str(e)}")
        return None

def fetch_listeners():
    try:
        base_url = "https://kworb.net/spotify/listeners{}.html"
        listeners_data = []
        stats_date = datetime.now().date() - timedelta(days=1)
        
        for page in range(1, 6):
            url = base_url.format(page if page > 1 else '')
            try:
                response = http.get(url, verify=False, timeout=10)
                response.raise_for_status()
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "html.parser")

                table = soup.find("table", class_="sortable")
                if not table:
                    continue
                    
                rows = table.find_all("tr")[1:]
                
                
                for row in rows:
                    cols = row.find_all("td")
                    if len(cols) >= 3:
                        artist_name = cols[1].get_text(strip=True)
                        listeners_text = cols[2].get_text(strip=True)
                        listeners = parse_number(listeners_text)
                        
                        if artist_name and listeners is not None:
                            listeners_data.append({
                                "artist_name": artist_name,
                                "listeners": listeners,
                                "date": stats_date
                            })

            except Exception as e:
                logger.error(f"Erreur page listeners {page}: {str(e)}")
                continue

        return listeners_data

    except Exception as e:
        logger.error(f"Erreur majeure fetch_listeners: {str(e)}")
        return []

def fetch_artists_stats_batch(batch_size=20, max_workers=5):
    logger.info("Début de la collecte des statistiques")
    start_time = time.time()
    
    ensure_schema_exists()
    session = db.connection.get_session()
    
    try:
        artists = session.query(Artist).all()
        artist_ids = [artist.spotify_id for artist in artists]
        stats_to_insert = []
        listeners_data = fetch_listeners()
        listeners_map = {item['artist_name']: item['listeners'] for item in listeners_data}
        stats_date = datetime.now().date() - timedelta(days=1)
        for i in tqdm(range(0, len(artist_ids), batch_size), desc="Artistes"):
            batch = artist_ids[i:i + batch_size]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(fetch_artist_stats, artist_id): artist_id for artist_id in batch}
                
                for future in concurrent.futures.as_completed(futures):
                    artist_id = futures[future]
                    try:
                        stats = future.result()
                        if stats:
                            artist = session.query(Artist).filter_by(spotify_id=artist_id).first()
                            if artist:
                                stats_to_insert.append({
                                    'artist_id': artist_id,
                                    'total_streams': stats['total_streams'],
                                    'daily_streams': stats['daily_streams'],
                                    'listeners': listeners_map.get(artist.name),
                                    'date': stats_date
                                })
                    except Exception as e:
                        logger.error(f"Erreur traitement artiste {artist_id}: {str(e)}")
            
            if stats_to_insert:
                try:
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
                    logger.error(f"Erreur DB: {str(e)}")
                
        logger.info(f"Collecte terminée en {time.time() - start_time:.2f}s")
            
    except Exception as e:
        session.rollback()
        logger.error(f"Erreur majeure: {str(e)}", exc_info=True)
    finally:
        session.close()

def run_scheduler():
    try:
        fetch_artists_stats_batch()
        
        schedule.every(24).hours.do(fetch_artists_stats_batch)
        logger.info("Planificateur démarré. Utilisez CTRL+C pour quitter.")
        
        while True:
            schedule.run_pending()
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("\nScript arrêté. Pour relancer :")
        logger.info("python3 fetch_artists_stats.py")
    except Exception as e:
        logger.error(f"Erreur : {str(e)}")

if __name__ == "__main__":
    run_scheduler()