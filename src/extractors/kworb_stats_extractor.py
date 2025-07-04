from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import requests
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from src.transformers.stats_transformer import parse_number

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


def fetch_artist_stats(artist_id):
    """Fetch artist statistics from kworb.net

    Args:
        artist_id (str): Spotify artist ID

    Returns:
        dict: Artist stats with total_streams and daily_streams, or None if not found
    """
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
        print(f"Error fetching stats for artist {artist_id}: {str(e)}")
        return None


def fetch_listeners():
    """Fetch listeners data from kworb.net top listeners pages

    Returns:
        list: List of dicts with artist_name, listeners, and date
    """
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
                print(f"Error fetching listeners page {page}: {str(e)}")
                continue

        return listeners_data

    except Exception as e:
        print(f"Major error in fetch_listeners: {str(e)}")
        return []
