import os
import time
import json
from db.connection import get_session
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import sqlalchemy as sa

def seconds_to_hms(seconds):
    """Convert seconds to hours, minutes, and seconds
    
    Args:
        seconds (int): Number of seconds to convert
    
    Returns:
        tuple: A tuple containing hours, minutes, and seconds
    """
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    return hours, minutes, seconds


def fetch_spotify_tracks_batch(ids: list):
    """
    Fetches track details from Spotify's API using a list of track IDs

    Args:
        ids (list): list of track IDs to fetch from Spotify.

    Returns:
        list: list of track details fetched from Spotify.
    """

    credentials = SpotifyClientCredentials(
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET")
    )
    sp = spotipy.Spotify(client_credentials_manager=credentials)

    results = []
    
    # Process in batches of 50 tracks (Spotify API limit)
    for i in range(0, len(ids), 50):
        batch_ids = ids[i:i+50]
        try:
            time.sleep(1)
            batch_results = sp.tracks(batch_ids)
            if 'tracks' in batch_results:
                results.extend(batch_results['tracks'])
        except Exception as e:
            print(f"Error fetching tracks batch {i//50 + 1}: {e}")
    
    # Update db
    if results:
        session = get_session()
        try:
            for track in results:
                if track:  # Some tracks might be None if they couldn't be found
                    # Mandatory conversion of dict to json for pgsql
                    track_json = json.dumps(track)
                    # Update the track in the database
                    session.execute(
                        sa.text("""
                        UPDATE song 
                        SET sp_track = CAST(:track_data AS jsonb)
                        WHERE song_id = :track_id
                        """),
                        {"track_data": track_json, "track_id": track['id']}
                    )
            session.commit()
            print(f"Updated {len(results)} tracks in the database", end=" ")
        except Exception as e:
            session.rollback()
            print(f"Database error when updating tracks: {e}")
        finally:
            session.close()
    
    return results


def fetch_all_spotify_tracks(batch_size=500):
    """
    Fetches all Spotify tracks for songs in the database that don't have track data yet
    Processes them in batches to handle API rate limits
    
    Args:
        batch_size (int): Number of songs to process in each batch
    """
    start = time.time()
    session = get_session()
    try:
        # Songs with a null sp_track field
        result = session.execute(
            sa.text("SELECT song_id FROM song WHERE sp_track IS NULL")
        )
        song_ids = [row[0] for row in result]
        session.close()
        
        total_songs = len(song_ids)
        print(f"Found {total_songs} songs without Spotify track data")
        
        total_batches = (total_songs + batch_size - 1) // batch_size
        songs_processed = 0
        
        # Songs batch processing
        for i in range(0, total_songs, batch_size):
            batch_start = time.time()
            batch = song_ids[i:i+batch_size]
            songs_processed += len(batch)
            
            batch_num = i // batch_size + 1
            print(f"Processing batch {batch_num}/{total_batches} ({len(batch)} songs)")
            
            # Fetch Spotify track data for the current batch
            fetch_spotify_tracks_batch(batch)
            
            # Time data for the current batch
            batch_elapsed = time.time() - batch_start
            songs_per_second = len(batch) / batch_elapsed if batch_elapsed > 0 else 0
            print(f"in {batch_elapsed:.2f}s ({songs_per_second:.2f} songs/sec)")
            print(f"Progress: {songs_processed}/{total_songs} songs ({songs_processed/total_songs*100:.1f}%)")
            
            time.sleep(1)
        
        # Final summary
        elapsed = time.time() - start
        hours, minutes, seconds = seconds_to_hms(elapsed)
        
        print(f"Completed fetching all Spotify tracks in {hours:02}:{minutes:02}:{seconds:02}")
        print(f"Average processing speed: {total_songs/elapsed:.2f} songs/second")
    
    except Exception as e:
        elapsed = time.time() - start
        print(f"Error in fetch_all_spotify_tracks after {elapsed:.2f} seconds: {e}")
        if session:
            session.close()

if __name__ == "__main__":
    fetch_all_spotify_tracks(batch_size=50)