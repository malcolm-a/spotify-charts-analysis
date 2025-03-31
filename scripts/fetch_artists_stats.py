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
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import schedule
import logging
import sys

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
        logger.error(f"Erreur lors de la récupération des stats pour l'artiste {artist_id}: {str(e)}")
        return None

def fetch_listeners(base_url="https://kworb.net/spotify/listeners"):
    try:
        listeners_data = []
        
        for page_num in ["", "2", "3", "4"]:
            url = f"{base_url}{page_num}.html"
            logger.info(f"Fetching from URL: {url}")
            
            response = requests.get(url, verify=False)    
            if response.status_code != 200:
                logger.warning(f"Failed to fetch from {url}: {response.status_code}")
                continue
                
            response.encoding = "utf-8"
            soup = BeautifulSoup(response.text, "html.parser")
            
            rows = soup.select("table.sortable tbody tr")
            logger.info(f"Found {len(rows)} rows in table on {url}")
            
            stats_date = datetime.now().date() - timedelta(days=1)  # default to yesterday
            date_text = soup.find(text=lambda t: t and "Last updated:" in t)
            if date_text:
                date_match = re.search(r'(\d{4}/\d{2}/\d{2})', date_text)
                if date_match:
                    try:
                        stats_date = datetime.strptime(date_match.group(1), '%Y/%m/%d').date()
                    except ValueError:
                        pass
            
            for row in rows:
                cols = row.find_all("td")
                if len(cols) >= 3:
                    artist_link = cols[1].find('a')
                    if artist_link:
                        href = artist_link.get('href', '')
                        spotify_id_match = re.search(r'artist/([^_]+)_songs\.html', href)
                        spotify_id = spotify_id_match.group(1) if spotify_id_match else None
                        
                        listeners_text = cols[2].text.strip()
                        listeners = parse_number(listeners_text)
                        
                        if spotify_id and listeners:
                            listeners_data.append({
                                "spotify_id": spotify_id,
                                "listeners": listeners,
                                "date": stats_date
                            })
        return listeners_data

    except Exception as e:
        logger.error(f"Erreur lors de la récupération des listeners: {str(e)}")
        return []

def fetch_artists_stats_batch(batch_size=50, max_workers=5):
    logger.info("Début de la collecte des statistiques")
    start_time = time.time()
    
    ensure_schema_exists()
    session = db.connection.get_session()
    
    try:
        artists = session.query(Artist).all()
        artist_ids = [artist.spotify_id for artist in artists]
        stats_to_insert = []
        listeners_data = fetch_listeners()
        listeners_map = {item['spotify_id']: item['listeners'] for item in listeners_data}
        
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
                            listeners = listeners_map.get(artist_id)
                            
                            stats_to_insert.append({
                                'artist_id': artist_id,
                                'total_streams': parse_number(stats['total_streams']),
                                'daily_streams': parse_number(stats['daily_streams']),
                                'listeners': listeners,
                                'date': datetime.now().date()
                            })
                    except Exception as e:
                        logger.error(f"Erreur lors du traitement de l'artiste {artist_id}: {str(e)}")
            
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
                
        logger.info(f"Collecte terminée avec succès. Durée: {time.time() - start_time:.2f} secondes")
            
    except Exception as e:
        session.rollback()
        logger.error(f"Erreur dans fetch_artists_stats_batch: {str(e)}", exc_info=True)
    finally:
        session.close()

def run_scheduler():
    fetch_artists_stats_batch()
    
    schedule.every(24).hours.do(fetch_artists_stats_batch)
    
    logger.info("Planificateur démarré. Prochaine exécution programmée toutes les 24 heures.")
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except KeyboardInterrupt:
            logger.info("Arrêt manuel du planificateur")
            break
        except Exception as e:
            logger.error(f"Erreur dans le planificateur: {str(e)}", exc_info=True)
            time.sleep(300)

if __name__ == "__main__":
    run_scheduler()