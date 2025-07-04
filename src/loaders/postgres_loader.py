from sqlalchemy.dialects.postgresql import insert
import sqlalchemy as sa
from src.config.connection import get_session
from src.models.database import Artist, Song, artist_song, Spotify_charts, Artist_stats
import json


class PostgresLoader:
    """Handles all database loading operations for the ETL pipeline"""

    def __init__(self):
        self.session = None

    def get_session(self):
        """Get or create database session"""
        if not self.session:
            self.session = get_session()
        return self.session

    def close_session(self):
        """Close database session"""
        if self.session:
            self.session.close()
            self.session = None

    def load_artists(self, artists_data: list):
        """Load artists data with upsert functionality

        Args:
            artists_data (list): List of artist dictionaries
        """
        if not artists_data:
            return

        session = self.get_session()
        try:
            stmt = insert(Artist).values(artists_data)
            stmt = stmt.on_conflict_do_nothing(index_elements=['spotify_id'])
            session.execute(stmt)
            session.commit()
            print(f"Loaded {len(artists_data)} artists")
        except Exception as e:
            session.rollback()
            print(f"Error loading artists: {e}")
            raise

    def load_songs(self, songs_data: list):
        """Load songs data with upsert functionality

        Args:
            songs_data (list): List of song dictionaries
        """
        if not songs_data:
            return

        session = self.get_session()
        try:
            stmt = insert(Song).values(songs_data)
            stmt = stmt.on_conflict_do_nothing(index_elements=['song_id'])
            session.execute(stmt)
            session.commit()
            print(f"Loaded {len(songs_data)} songs")
        except Exception as e:
            session.rollback()
            print(f"Error loading songs: {e}")
            raise

    def load_artist_song_relationships(self, relationships_data: list):
        """Load artist-song relationships

        Args:
            relationships_data (list): List of artist-song relationship dictionaries
        """
        if not relationships_data:
            return

        session = self.get_session()
        try:
            stmt = insert(artist_song).values(relationships_data)
            stmt = stmt.on_conflict_do_nothing(index_elements=['artist_id', 'song_id'])
            session.execute(stmt)
            session.commit()
            print(f"Loaded {len(relationships_data)} artist-song relationships")
        except Exception as e:
            session.rollback()
            print(f"Error loading relationships: {e}")
            raise

    def load_spotify_charts(self, charts_data: list):
        """Load Spotify charts data with upsert functionality

        Args:
            charts_data (list): List of chart entry dictionaries
        """
        if not charts_data:
            return

        session = self.get_session()
        try:
            stmt = insert(Spotify_charts).values(charts_data)
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
            print(f"Loaded {len(charts_data)} chart entries")
        except Exception as e:
            session.rollback()
            print(f"Error loading charts: {e}")
            raise

    def load_artist_stats(self, stats_data: list):
        """Load artist statistics with upsert functionality

        Args:
            stats_data (list): List of artist stats dictionaries
        """
        if not stats_data:
            return

        session = self.get_session()
        try:
            stmt = insert(Artist_stats).values(stats_data)
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
            print(f"Loaded {len(stats_data)} artist stats")
        except Exception as e:
            session.rollback()
            print(f"Error loading artist stats: {e}")
            raise

    def update_artist_spotify_data(self, artist_data: list):
        """Update artists with Spotify API data

        Args:
            artist_data (list): List of artist data from Spotify API
        """
        if not artist_data:
            return

        session = self.get_session()
        try:
            for artist in artist_data:
                if artist:
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
            print(f"Updated {len(artist_data)} artists with Spotify data")
        except Exception as e:
            session.rollback()
            print(f"Error updating artist Spotify data: {e}")
            raise

    def update_song_spotify_data(self, track_data: list):
        """Update songs with Spotify API data

        Args:
            track_data (list): List of track data from Spotify API
        """
        if not track_data:
            return

        session = self.get_session()
        try:
            for track in track_data:
                if track:
                    track_json = json.dumps(track)
                    session.execute(
                        sa.text("""
                        UPDATE song
                        SET sp_track = CAST(:track_data AS jsonb)
                        WHERE song_id = :track_id
                        """),
                        {"track_data": track_json, "track_id": track['id']}
                    )
            session.commit()
            print(f"Updated {len(track_data)} songs with Spotify data")
        except Exception as e:
            session.rollback()
            print(f"Error updating song Spotify data: {e}")
            raise

    def update_song_audio_features(self, features_data: list):
        """Update songs with audio features data

        Args:
            features_data (list): List of audio features data from Spotify API
        """
        if not features_data:
            return

        session = self.get_session()
        try:
            for features in features_data:
                if features:
                    features_json = json.dumps(features)
                    session.execute(
                        sa.text("""
                        UPDATE song
                        SET features = CAST(:features_data AS jsonb)
                        WHERE song_id = :track_id
                        """),
                        {"features_data": features_json, "track_id": features['id']}
                    )
            session.commit()
            print(f"Updated {len(features_data)} songs with audio features")
        except Exception as e:
            session.rollback()
            print(f"Error updating song audio features: {e}")
            raise

    def load_complete_chart_data(self, chart_data: dict):
        """Load complete chart data (artists, songs, relationships, charts)

        Args:
            chart_data (dict): Dictionary containing all chart-related data
        """
        try:
            # Load in proper order to respect foreign key constraints
            self.load_artists(chart_data.get('artists', []))
            self.load_songs(chart_data.get('songs', []))
            self.load_artist_song_relationships(chart_data.get('artist_songs', []))
            self.load_spotify_charts(chart_data.get('charts', []))

            print("Successfully loaded complete chart data")
        except Exception as e:
            print(f"Error loading complete chart data: {e}")
            raise
        finally:
            self.close_session()
