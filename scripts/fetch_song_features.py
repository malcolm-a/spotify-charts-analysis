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

def print_time(seconds):
    """Print time in a human-readable format
    
    Args:
        seconds (int): number of seconds to convert
    
    Returns:
        str: formatted time string  
    """
    hours, minutes, seconds = seconds_to_hms(seconds)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

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
            print(f"Updated {len(results)} tracks in the database")
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
    session = get_session()
    try:
        # Songs with a null sp_track field
        result = session.execute(
            sa.text("SELECT song_id FROM song WHERE sp_track IS NULL")
        )
        song_ids = [row[0] for row in result]
        session.close()
        
        print(f"Found {len(song_ids)} songs without Spotify track data")
        
        for i in range(0, len(song_ids), batch_size):
            batch = song_ids[i:i+batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(song_ids) + batch_size - 1)//batch_size}")
            fetch_spotify_tracks_batch(batch)
            # Delay to respect API rate limits
            time.sleep(1)
        
        print("Completed fetching all Spotify tracks")
    
    except Exception as e:
        print(f"Error in fetch_all_spotify_tracks: {e}")
        if session:
            session.close()

if __name__ == "__main__":
    fetch_all_spotify_tracks()