import time
from src.extractors.spotify_api_extractor import SpotifyAPIExtractor
from src.loaders.postgres_loader import PostgresLoader
from src.config.connection import get_session
from src.models.schema import ensure_schema_exists


class SpotifyMetadataPipeline:
    """Pipeline for fetching and loading Spotify metadata (artists and tracks)"""

    def __init__(self, batch_size: int = 500):
        self.batch_size = batch_size
        self.extractor = SpotifyAPIExtractor()
        self.loader = PostgresLoader()

    def run_artists_metadata(self):
        """Run the artists metadata ETL pipeline"""
        print("Starting Spotify Artists Metadata Pipeline...")
        start_time = time.time()

        try:
            # Ensure database schema exists
            ensure_schema_exists()

            session = get_session()

            # Extract phase - get missing artist IDs
            print("\n--- EXTRACT PHASE ---")
            missing_artist_ids = self.extractor.get_missing_artist_ids(session)
            session.close()

            total_artists = len(missing_artist_ids)
            print(f"Found {total_artists} artists without Spotify metadata")

            if total_artists == 0:
                print("No artists need metadata updates")
                return

            # Process in batches
            processed = 0
            for i in range(0, total_artists, self.batch_size):
                batch = missing_artist_ids[i:i+self.batch_size]
                processed += len(batch)
                print(f"Processing batch {i//self.batch_size + 1}: {len(batch)} artists ({processed}/{total_artists})")

                # Extract artist data from Spotify API
                artist_data = self.extractor.fetch_artists_batch(batch)

                # Load phase - update database
                if artist_data:
                    print(f"\n--- LOAD PHASE (Batch {i//self.batch_size + 1}) ---")
                    self.loader.update_artist_spotify_data(artist_data)

                time.sleep(1)  # Rate limiting

            elapsed_time = time.time() - start_time
            print(f"\nArtists Metadata Pipeline completed in {elapsed_time:.2f} seconds")
            print(f"Updated {processed} artists with Spotify metadata")

        except Exception as e:
            print(f"Artists Metadata Pipeline failed: {str(e)}")
            raise
        finally:
            self.loader.close_session()

    def run_tracks_metadata(self):
        """Run the tracks metadata ETL pipeline"""
        print("Starting Spotify Tracks Metadata Pipeline...")
        start_time = time.time()

        try:
            # Ensure database schema exists
            ensure_schema_exists()

            session = get_session()

            # Extract phase - get missing track IDs
            print("\n--- EXTRACT PHASE ---")
            missing_track_ids = self.extractor.get_missing_track_ids(session)
            session.close()

            total_tracks = len(missing_track_ids)
            print(f"Found {total_tracks} tracks without Spotify metadata")

            if total_tracks == 0:
                print("No tracks need metadata updates")
                return

            # Process in batches
            processed = 0
            for i in range(0, total_tracks, self.batch_size):
                batch = missing_track_ids[i:i+self.batch_size]
                processed += len(batch)
                print(f"Processing batch {i//self.batch_size + 1}: {len(batch)} tracks ({processed}/{total_tracks})")

                # Extract track data from Spotify API
                track_data = self.extractor.fetch_tracks_batch(batch)

                # Extract audio features
                audio_features = self.extractor.fetch_audio_features_batch(batch)

                # Load phase - update database
                if track_data or audio_features:
                    print(f"\n--- LOAD PHASE (Batch {i//self.batch_size + 1}) ---")
                    if track_data:
                        self.loader.update_song_spotify_data(track_data)
                    if audio_features:
                        self.loader.update_song_audio_features(audio_features)

                time.sleep(1)  # Rate limiting

            elapsed_time = time.time() - start_time
            print(f"\nTracks Metadata Pipeline completed in {elapsed_time:.2f} seconds")
            print(f"Updated {processed} tracks with Spotify metadata")

        except Exception as e:
            print(f"Tracks Metadata Pipeline failed: {str(e)}")
            raise
        finally:
            self.loader.close_session()

    def run_complete_metadata_pipeline(self):
        """Run both artists and tracks metadata pipelines"""
        print("Starting Complete Spotify Metadata Pipeline...")
        overall_start = time.time()

        try:
            # Run artists metadata pipeline
            self.run_artists_metadata()

            print("\n" + "="*60 + "\n")

            # Run tracks metadata pipeline
            self.run_tracks_metadata()

            overall_elapsed = time.time() - overall_start
            print(f"\nComplete Spotify Metadata Pipeline finished in {overall_elapsed:.2f} seconds")

        except Exception as e:
            print(f"Complete Metadata Pipeline failed: {str(e)}")
            raise


def run_spotify_artists():
    """Entry point for running the Spotify artists metadata pipeline"""
    pipeline = SpotifyMetadataPipeline()
    pipeline.run_artists_metadata()


def run_spotify_tracks():
    """Entry point for running the Spotify tracks metadata pipeline"""
    pipeline = SpotifyMetadataPipeline()
    pipeline.run_tracks_metadata()


def run_complete_spotify_metadata():
    """Entry point for running the complete Spotify metadata pipeline"""
    pipeline = SpotifyMetadataPipeline()
    pipeline.run_complete_metadata_pipeline()


if __name__ == "__main__":
    run_complete_spotify_metadata()
