from bs4 import BeautifulSoup
import db.connection
from db.models import Artist, Song, artist_song
from db.schema import ensure_schema_exists
import re
import requests
import urllib3
import concurrent.futures
import time
from tqdm import tqdm
from sqlalchemy.dialects.postgresql import insert

# Disables tls warnings in the console when fetching data
urllib3.disable_warnings()


def fetch_artists(url="http://www.kworb.net/spotify/artists.html"):
    """Fetch all artists and bulk insert them into the database.
    
    Returns:
        None
    """

    ensure_schema_exists()

    try:
        response = requests.get(url, verify=False)
        response.encoding = "utf-8"

        artist_links = BeautifulSoup(response.text, "html.parser").select(
            "table.addpos tbody tr td.text div a"
        )

        artists_data = []
        for link in artist_links:
            artist_name = link.text.strip()
            href = link.get("href").lstrip("/spotify")
            spotify_id_match = re.search(r"artist/([^_]+)_", href)
            spotify_id = spotify_id_match.group(1) if spotify_id_match else None

            if spotify_id:
                artists_data.append({"name": artist_name, "spotify_id": spotify_id})

        # Bulk insert artists using SQLAlchemy Core
        session = db.connection.get_session()
        stmt = insert(Artist.__table__).values(artists_data)
        stmt = stmt.on_conflict_do_nothing(index_elements=["spotify_id"])
        session.execute(stmt)
        session.commit()
        session.close()

        print(f"Fetched {len(artists_data)} artists into the database")

    except Exception as e:
        print(f"Error fetching artists: {e}")


def fetch_artist_songs(artist_id):

    """Fetch songs for a given artist from kworb.net

    Args:
        artist_id (str): the spotify id of the artist
        
    Returns:
        list: a list of songs with their names and spotify ids
        str: the spotify id of the artist
    """
    
    try:
        url = f"http://www.kworb.net/spotify/artist/{artist_id}_songs.html"
        response = requests.get(url, verify=False, timeout=10)
        response.encoding = "utf-8"

        soup = BeautifulSoup(response.text, "html.parser")
        artist_songs = soup.select("table.addpos tbody tr td.text div a")

        songs = []
        for song in artist_songs:
            song_name = song.text.strip()
            href = song.get("href")
            spotify_id_match = re.search(r"/track/([^_]+)", href)
            spotify_id = spotify_id_match.group(1) if spotify_id_match else None

            if spotify_id:
                songs.append({"name": song_name, "song_id": spotify_id})

        return songs, artist_id
    except Exception as e:
        print(f"Error fetching songs for artist {artist_id}: {e}")
        return [], artist_id


def fetch_artists_songs_batch(batch_size=50, max_workers=10):
    """Fetch songs for all artists in batches and bulk insert them into the database

    Args:
        batch_size (int, optional): how many artists to treat at once. Defaults to 50.
        max_workers (int, optional): how many threads to use for fetching songs. Defaults to 10.
        
    Returns:
        None
    """ 
       
    try:
        ensure_schema_exists()

        # Get all artist ids from the db
        session = db.connection.get_session()
        artists = session.query(Artist).all()
        spotify_ids = [artist.spotify_id for artist in artists]
        session.close()

        print(f"Found {len(spotify_ids)} artists to process")

        for i in range(0, len(spotify_ids), batch_size):
            batch = spotify_ids[i : i + batch_size]
            print(
                f"Processing batch {i // batch_size + 1}/{(len(spotify_ids) + batch_size - 1) // batch_size}"
            )

            # Use concurrent.futures to make requests in parallel
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                future_to_artist = {
                    executor.submit(fetch_artist_songs, artist_id): artist_id
                    for artist_id in batch
                }

                all_songs = []
                all_relationships = []

                for future in tqdm(
                    concurrent.futures.as_completed(future_to_artist),
                    total=len(batch),
                    desc="Artists",
                ):
                    artist_id = future_to_artist[future]
                    try:
                        songs, artist_id = future.result()
                        all_songs.extend(songs)
                        all_relationships.extend(
                            [
                                {"artist_id": artist_id, "song_id": s["song_id"]}
                                for s in songs
                            ]
                        )
                    except Exception as e:
                        print(f"Error processing artist {artist_id}: {e}")
                        continue  # skip if there's an error in fetching songs

                # Remove duplicates
                unique_songs = list({s["song_id"]: s for s in all_songs}.values())

                # Bulk insert songs and relationships
                if unique_songs or all_relationships:
                    session = db.connection.get_session()
                    try:
                        # Insert songs
                        if unique_songs:
                            stmt = insert(Song.__table__).values(
                                [
                                    {"song_id": s["song_id"], "name": s["name"]}
                                    for s in unique_songs
                                ]
                            )
                            stmt = stmt.on_conflict_do_nothing(
                                index_elements=["song_id"]
                            )
                            session.execute(stmt)

                        # Insert song-artist relationships
                        if all_relationships:
                            stmt = insert(artist_song).values(all_relationships)
                            stmt = stmt.on_conflict_do_nothing(
                                index_elements=["artist_id", "song_id"]
                            )
                            session.execute(stmt)

                        session.commit()
                        print(
                            f"Inserted {len(unique_songs)} songs and {len(all_relationships)} relationships"
                        )
                    except Exception as e:
                        session.rollback()
                        print(f"Database error: {e}")
                    finally:
                        session.close()

                time.sleep(1)  # throttle requests to avoid overwhelming the server

            print(
                f"Successfully fetched and saved songs for {len(spotify_ids)} artists"
            )

    except Exception as e:
        print(f"Error in main fetch_artists_songs function: {e}")


if __name__ == "__main__":
    # drop_and_create_schema()  # RESETS THE DATABASE
    fetch_artists()
    fetch_artists("https://kworb.net/spotify/listeners.html")
    fetch_artists_songs_batch()
