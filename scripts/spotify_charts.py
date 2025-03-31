import re
import requests
import urllib3
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import concurrent.futures
import time
from tqdm import tqdm
import schedule
from sqlalchemy.dialects.postgresql import insert

from db.connection import get_session
from db.models import Song, Artist, Spotify_charts, Country, artist_song
from db.schema import ensure_schema_exists

# disable ssl warnings
urllib3.disable_warnings()

def parse_number(text: str) -> int:
    """Transforms text to number

    Args:
        text (str): the text to transform

    Returns:
        int: the transformed number
    """
    if not text or text == '-':
        return 0
    return int(text.replace(",", "").replace("+", ""))

def extract_artists_and_title(text_cell: str) -> tuple:
    """Extracts artists and title from the text cell

    Args:
        text_cell (str): the cell containing the text

    Returns:
        tuple: the song id, song name, and the list of its artists
    """
    artists = []
    song_id = None
    song_name = None
    
    links = text_cell.find_all('a')
    
    for i, link in enumerate(links):
        href = link.get('href', '')
        
        # extract song details (only the first track link)
        if '/track/' in href and not song_id:
            song_match = re.search(r'/track/([^.]+)\.html', href)
            if song_match:
                song_id = song_match.group(1)
                song_name = link.text.strip()
        
        # extract artist details
        if '/artist/' in href:
            artist_match = re.search(r'/artist/([^.]+)\.html', href)
            if artist_match:
                artist_id = artist_match.group(1)
                artist_name = link.text.strip()
                artists.append({
                    'spotify_id': artist_id,
                    'name': artist_name
                })
    
    return song_id, song_name, artists

def fetch_country_charts(country_code: str) -> dict:
    """fetches the daily charts for a given country code

    Args:
        country_code (str): the alpha-2 country code

    Returns:
        dict: a dictionary containing the charts data, songs, artists, and artist-songs relationships
    """    
    try:
        url = f"https://kworb.net/spotify/country/{country_code.lower()}_daily.html"
        response = requests.get(url, verify=False, timeout=15)
        
        if response.status_code != 200:
            print(f"Failed to get charts for {country_code}: status code {response.status_code}")
            return []
            
        response.encoding = "utf-8"
        soup = BeautifulSoup(response.text, "html.parser")
        
        chart_date = (datetime.now().date() - timedelta(days=1))

        if title := soup.select_one("span.pagetitle"):
            if date_match := re.search(r'(\d{4}/\d{2}/\d{2})', title.text):
                try:
                    chart_date = datetime.strptime(date_match.group(1), '%Y/%m/%d').date()
                except ValueError:
                    pass
        
        charts_data = []
        songs_data = []
        artists_data = []
        artist_songs = []
        
        rows = soup.select("table#spotifydaily tbody tr")
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 11:
                continue
                
            # extract position and text column
            position = int(cols[0].text.strip())
            text_cell = cols[2]
            
            song_id, song_name, artists = extract_artists_and_title(text_cell)
            if not song_id or not song_name or not artists:
                continue
                
            # extract other chart data 
            days_text = cols[3].text.strip()
            if not days_text:
                continue
            else:
                days = int(days_text)
            
            streams = cols[6].text.strip()
            total_streams = cols[10].text.strip()
            if not streams or not total_streams:
                continue
            else:    
                streams = parse_number(streams)
                total_streams = parse_number(total_streams)
            
            # add chart data
            charts_data.append({
                'date': chart_date,
                'country_code': country_code,
                'song_id': song_id,
                'streams': streams,
                'total_streams': total_streams,
                'days': days,
                'rank': position
            })
            
            # add song data
            songs_data.append({
                'song_id': song_id,
                'name': song_name
            })
            
            # add artist data and relationship
            for artist in artists:
                artists_data.append(artist)
                artist_songs.append({
                    'artist_id': artist['spotify_id'],
                    'song_id': song_id
                })
        
        return {
            'charts': charts_data,
            'songs': songs_data,
            'artists': artists_data,
            'artist_songs': artist_songs
        }
            
    except Exception as e:
        print(f"Error fetching charts for {country_code}: {str(e)}")
        return []

def save_charts_data(data: dict):
    """saves the charts data to the database

    Args:
        data (dict): the data fetched with fetch_country_charts
    """    
    if not data or not data.get('charts'):
        return
        
    session = get_session()
    try:
        # insert artists
        if data['artists']:
            stmt = insert(Artist).values(data['artists'])
            stmt = stmt.on_conflict_do_nothing(index_elements=['spotify_id'])
            session.execute(stmt)
        
        # insert songs
        if data['songs']:
            stmt = insert(Song).values(data['songs'])
            stmt = stmt.on_conflict_do_nothing(index_elements=['song_id'])
            session.execute(stmt)
        
        # insert artist-song relationships
        if data['artist_songs']:
            stmt = insert(artist_song).values(data['artist_songs'])
            stmt = stmt.on_conflict_do_nothing(index_elements=['artist_id', 'song_id'])
            session.execute(stmt)
        
        # insert chart data
        if data['charts']:
            stmt = insert(Spotify_charts).values(data['charts'])
            stmt = stmt.on_conflict_do_update(
                index_elements=['song_id', 'country_code', 'date'],
                set_={
                    'streams': stmt.excluded.streams,
                    'total_streams': stmt.excluded.total_streams,
                    'days': stmt.excluded.days,
                    'rank': stmt.excluded.rank
                }
            )
            session.execute(stmt)
            
        session.commit()
        print(f"Saved {len(data['charts'])} chart entries")
        
    except Exception as e:
        session.rollback()
        print(f"Database error: {str(e)}")
    finally:
        session.close()

def fetch_all_countries_charts(max_workers: int = 10):
    """Fetches charts for all countries in batches

    Args:
        max_workers (int, optional): the number of threads to use. Defaults to 10.
    """    
    ensure_schema_exists()
    session = get_session()
    
    try:
        countries = session.query(Country).all()
        country_codes = [country.country_code for country in countries]
        print(f"Found {len(country_codes)} countries to process")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_country_charts, code): code for code in country_codes}
            
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(country_codes), desc="Fetching country charts"):
                country_code = futures[future]
                try:
                    data = future.result()
                    if data:
                        save_charts_data(data)
                except Exception as e:
                    print(f"Error processing {country_code}: {str(e)}")
                
                
    except Exception as e:
        print(f"Error in fetch_all_countries_charts: {str(e)}")
    finally:
        session.close()

def run_scheduler():
    """run the chart fetcher on a schedule"""
    fetch_all_countries_charts()
    
    schedule.every(24).hours.do(fetch_all_countries_charts)
    
    print("Scheduler started. Next run in 24 hours.")
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except KeyboardInterrupt:
            print("Scheduler manually stopped")
            break
        except Exception as e:
            print(f"Scheduler error: {str(e)}")
            time.sleep(300)  # wait 5 minutes before retrying

if __name__ == "__main__":
    run_scheduler()