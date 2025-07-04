import re
import requests
import urllib3
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from src.transformers.chart_transformer import parse_number, extract_artists_and_title

# disable ssl warnings
urllib3.disable_warnings()


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
            return {}

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
        return {}
