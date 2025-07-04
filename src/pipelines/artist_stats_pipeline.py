import concurrent.futures
import time
from datetime import datetime, timedelta
from tqdm import tqdm
from src.extractors.kworb_stats_extractor import fetch_artist_stats, fetch_listeners
from src.transformers.stats_transformer import normalize_artist_stats, normalize_listeners_data
from src.loaders.postgres_loader import PostgresLoader
from src.config.connection import get_session
from src.models.database import Artist
from src.models.schema import ensure_schema_exists


class ArtistStatsPipeline:
    """Pipeline for fetching and loading artist statistics data"""

    def __init__(self, batch_size: int = 20, max_workers: int = 5):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.loader = PostgresLoader()

    def extract_artist_stats_batch(self, artist_ids: list) -> list:
        """
        Extract artist statistics for a batch of artists

        Args:
            artist_ids (list): List of Spotify artist IDs

        Returns:
            list: List of artist statistics dictionaries
        """
        stats_data = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(fetch_artist_stats, artist_id): artist_id for artist_id in artist_ids}

            for future in concurrent.futures.as_completed(futures):
                artist_id = futures[future]
                try:
                    raw_stats = future.result()
                    if raw_stats:
                        normalized_stats = normalize_artist_stats(raw_stats)
                        if normalized_stats:
                            normalized_stats['artist_id'] = artist_id
                            normalized_stats['date'] = datetime.now().date() - timedelta(days=1)
                            stats_data.append(normalized_stats)
                except Exception as e:
                    print(f"Error processing artist {artist_id}: {str(e)}")

        return stats_data

    def extract_all_artist_stats(self) -> list:
        """Extract statistics for all artists in the database

        Returns:
            list: List of all artist statistics
        """
        session = get_session()
        try:
            artists = session.query(Artist).all()
            artist_ids = [artist.spotify_id for artist in artists]
            print(f"Found {len(artist_ids)} artists to process")

            all_stats = []

            # Process artists in batches
            for i in tqdm(range(0, len(artist_ids), self.batch_size), desc="Processing artist batches"):
                batch = artist_ids[i:i + self.batch_size]
                batch_stats = self.extract_artist_stats_batch(batch)
                all_stats.extend(batch_stats)

                print(f"Processed batch {i // self.batch_size + 1}, collected {len(batch_stats)} stats")
                time.sleep(1)  # Rate limiting

            return all_stats

        except Exception as e:
            print(f"Error in extract_all_artist_stats: {str(e)}")
            return []
        finally:
            session.close()

    def extract_listeners_data(self) -> dict:
        """Extract listeners data and normalize it

        Returns:
            dict: Normalized listeners data mapped by artist name
        """
        try:
            print("Fetching listeners data...")
            raw_listeners = fetch_listeners()
            normalized_listeners = normalize_listeners_data(raw_listeners)
            print(f"Extracted listeners data for {len(normalized_listeners)} artists")
            return normalized_listeners
        except Exception as e:
            print(f"Error extracting listeners data: {str(e)}")
            return {}

    def enrich_stats_with_listeners(self, stats_data: list, listeners_map: dict) -> list:
        """Enrich artist stats with listeners data

        Args:
            stats_data (list): List of artist statistics
            listeners_map (dict): Mapping of artist names to listener counts

        Returns:
            list: Enriched stats data with listeners information
        """
        session = get_session()
        try:
            # Create a mapping of artist_id to artist_name
            artists = session.query(Artist).all()
            artist_name_map = {artist.spotify_id: artist.name for artist in artists}

            # Enrich stats with listeners data
            for stats in stats_data:
                artist_id = stats.get('artist_id')
                if artist_id and artist_id in artist_name_map:
                    artist_name = artist_name_map[artist_id]
                    if artist_name in listeners_map:
                        stats['listeners'] = listeners_map[artist_name]

            return stats_data

        except Exception as e:
            print(f"Error enriching stats with listeners: {str(e)}")
            return stats_data
        finally:
            session.close()

    def run(self):
        """Run the complete artist stats ETL pipeline"""
        print("Starting Artist Stats ETL Pipeline...")
        start_time = time.time()

        try:
            # Ensure database schema exists
            ensure_schema_exists()

            # Extract phase
            print("\n--- EXTRACT PHASE ---")

            # Extract artist statistics
            artist_stats = self.extract_all_artist_stats()

            # Extract listeners data
            listeners_map = self.extract_listeners_data()

            if not artist_stats and not listeners_map:
                print("No stats data extracted. Pipeline completed with no data.")
                return

            # Transform phase
            print("\n--- TRANSFORM PHASE ---")
            enriched_stats = self.enrich_stats_with_listeners(artist_stats, listeners_map)
            print(f"Enriched {len(enriched_stats)} artist stats with listeners data")

            # Load phase
            print("\n--- LOAD PHASE ---")
            if enriched_stats:
                self.loader.load_artist_stats(enriched_stats)

            elapsed_time = time.time() - start_time
            print(f"\nArtist Stats Pipeline completed successfully in {elapsed_time:.2f} seconds")
            print(f"Processed {len(enriched_stats)} artist statistics")

        except Exception as e:
            print(f"Pipeline failed: {str(e)}")
            raise
        finally:
            self.loader.close_session()


def run_artist_stats():
    """Entry point for running the artist stats pipeline"""
    pipeline = ArtistStatsPipeline()
    pipeline.run()


if __name__ == "__main__":
    run_artist_stats()
