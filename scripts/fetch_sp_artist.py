import os
import time
import json
from db.connection import get_session
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import sqlalchemy as sa

def fetch_spotify_artists(batch_size=50):
    """Fetch and update Spotify artist data for artists in database without it
        Args:
            batch_size (int): Number of artists to process in each batch. Should not exceed 50 until spotify's rate limiting changes. Defaults to 50. 
    """
    start_time = time.time()
    session = get_session()
    credentials = SpotifyClientCredentials(
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET")
    )
    sp = spotipy.Spotify(client_credentials_manager=credentials)
    
    # artists without Spotify data
    result = session.execute(
        sa.text("SELECT spotify_id FROM artist WHERE sp_artist IS NULL")
    )
    artist_ids = [row[0] for row in result]
    total_artists = len(artist_ids)
    print(f"Found {total_artists} artists without Spotify data")
    

    
    # Process in batches
    processed = 0
    for i in range(0, total_artists, batch_size):
        batch = artist_ids[i:i+batch_size]
        processed += len(batch)
        print(f"Processing {len(batch)} artists ({processed}/{total_artists})")
        
        # fetch artist data with spotify's 50 rate limit
        for j in range(0, len(batch), 50):
            sub_batch = batch[j:j+50]
            try:
                artists_data = sp.artists(sub_batch)
                if 'artists' in artists_data:
                    for artist in artists_data['artists']:
                        if artist:  # Skip if None (artist not found)
                            try:
                                artist_json = json.dumps(artist)
                                session.execute(
                                    sa.text("""
                                    UPDATE artist
                                    SET sp_artist = CAST(:artist_data AS jsonb)
                                    WHERE spotify_id = :artist_id
                                    """),
                                    {"artist_data": artist_json, "artist_id": artist['id']}
                                )
                                session.commit()
                            except Exception as e:
                                session.rollback()
                                print(f"Error updating artist {artist['id']}: {e}")
            except Exception as e:
                print(f"Error fetching artists: {e}")
            
            time.sleep(1)  # Rate limiting
    
    session.close()
    elapsed = time.time() - start_time
    print(f"Completed in {elapsed:.2f}s. Updated {processed} artists.")

if __name__ == "__main__":
    fetch_spotify_artists()