import os
import time
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import sqlalchemy as sa


class SpotifyAPIExtractor:
    """Extractor for Spotify API data (artists and tracks)"""

    def __init__(self):
        credentials = SpotifyClientCredentials(
            client_id=os.getenv("SPOTIFY_CLIENT_ID"),
            client_secret=os.getenv("SPOTIFY_CLIENT_SECRET")
        )
        self.sp = spotipy.Spotify(client_credentials_manager=credentials)

    def fetch_artists_batch(self, artist_ids: list) -> list:
        """Fetch artist details from Spotify API

        Args:
            artist_ids (list): List of Spotify artist IDs

        Returns:
            list: List of artist data from Spotify API
        """
        results = []

        # Process in batches of 50 (Spotify API limit)
        for i in range(0, len(artist_ids), 50):
            batch = artist_ids[i:i+50]
            try:
                time.sleep(1)  # Rate limiting
                batch_results = self.sp.artists(batch)
                if 'artists' in batch_results:
                    results.extend([artist for artist in batch_results['artists'] if artist])
            except Exception as e:
                print(f"Error fetching artists batch {i//50 + 1}: {e}")

        return results

    def fetch_tracks_batch(self, track_ids: list) -> list:
        """Fetch track details from Spotify API

        Args:
            track_ids (list): List of Spotify track IDs

        Returns:
            list: List of track data from Spotify API
        """
        results = []

        # Process in batches of 50 (Spotify API limit)
        for i in range(0, len(track_ids), 50):
            batch = track_ids[i:i+50]
            try:
                time.sleep(1)  # Rate limiting
                batch_results = self.sp.tracks(batch)
                if 'tracks' in batch_results:
                    results.extend([track for track in batch_results['tracks'] if track])
            except Exception as e:
                print(f"Error fetching tracks batch {i//50 + 1}: {e}")

        return results

    def get_missing_artist_ids(self, session) -> list:
        """Get artist IDs that don't have Spotify data yet

        Args:
            session: Database session

        Returns:
            list: List of artist IDs missing Spotify data
        """
        result = session.execute(
            sa.text("SELECT spotify_id FROM artist WHERE sp_artist IS NULL")
        )
        return [row[0] for row in result]

    def get_missing_track_ids(self, session) -> list:
        """Get track IDs that don't have Spotify data yet

        Args:
            session: Database session

        Returns:
            list: List of track IDs missing Spotify data
        """
        result = session.execute(
            sa.text("SELECT song_id FROM song WHERE sp_track IS NULL")
        )
        return [row[0] for row in result]

    def fetch_audio_features_batch(self, track_ids: list) -> list:
        """Fetch audio features for tracks from Spotify API

        Args:
            track_ids (list): List of Spotify track IDs

        Returns:
            list: List of audio features data from Spotify API
        """
        results = []

        # Process in batches of 100 (Spotify API limit for audio features)
        for i in range(0, len(track_ids), 100):
            batch = track_ids[i:i+100]
            try:
                time.sleep(1)  # Rate limiting
                batch_results = self.sp.audio_features(batch)
                if batch_results:
                    results.extend([features for features in batch_results if features])
            except Exception as e:
                print(f"Error fetching audio features batch {i//100 + 1}: {e}")

        return results
