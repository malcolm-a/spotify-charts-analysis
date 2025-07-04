import concurrent.futures
import time
from tqdm import tqdm
from src.extractors.kworb_charts_extractor import fetch_country_charts
from src.loaders.postgres_loader import PostgresLoader
from src.config.connection import get_session
from src.models.database import Country
from src.models.schema import ensure_schema_exists


class DailyChartsPipeline:
    """Pipeline for fetching and loading daily Spotify charts data"""

    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers
        self.loader = PostgresLoader()

    def extract_all_countries_charts(self) -> list:
        """Extract charts data for all countries

        Returns:
            list: List of chart data dictionaries for all countries
        """
        session = get_session()
        try:
            countries = session.query(Country).all()
            country_codes = [country.country_code for country in countries]
            print(f"Found {len(country_codes)} countries to process")

            all_chart_data = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(fetch_country_charts, code): code for code in country_codes}

                for future in tqdm(concurrent.futures.as_completed(futures), total=len(country_codes), desc="Fetching country charts"):
                    country_code = futures[future]
                    try:
                        data = future.result()
                        if data and data.get('charts'):
                            all_chart_data.append(data)
                            print(f"Successfully extracted {len(data['charts'])} entries for {country_code}")
                    except Exception as e:
                        print(f"Error processing {country_code}: {str(e)}")

            return all_chart_data

        except Exception as e:
            print(f"Error in extract_all_countries_charts: {str(e)}")
            return []
        finally:
            session.close()

    def load_charts_data(self, all_chart_data: list):
        """Load all extracted chart data into the database

        Args:
            all_chart_data (list): List of chart data dictionaries
        """
        total_charts = 0
        total_songs = 0
        total_artists = 0
        total_relationships = 0

        try:
            for chart_data in all_chart_data:
                if chart_data:
                    self.loader.load_complete_chart_data(chart_data)
                    total_charts += len(chart_data.get('charts', []))
                    total_songs += len(chart_data.get('songs', []))
                    total_artists += len(chart_data.get('artists', []))
                    total_relationships += len(chart_data.get('artist_songs', []))

            print(f"\nPipeline Summary:")
            print(f"- Loaded {total_charts} chart entries")
            print(f"- Processed {total_songs} songs")
            print(f"- Processed {total_artists} artists")
            print(f"- Created {total_relationships} artist-song relationships")

        except Exception as e:
            print(f"Error loading charts data: {str(e)}")
            raise

    def run(self):
        """Run the complete daily charts ETL pipeline"""
        print("Starting Daily Charts ETL Pipeline...")
        start_time = time.time()

        try:
            # Ensure database schema exists
            ensure_schema_exists()

            # Extract data from all countries
            print("\n--- EXTRACT PHASE ---")
            all_chart_data = self.extract_all_countries_charts()

            if not all_chart_data:
                print("No chart data extracted. Pipeline completed with no data.")
                return

            # Load data into database
            print("\n--- LOAD PHASE ---")
            self.load_charts_data(all_chart_data)

            elapsed_time = time.time() - start_time
            print(f"\nDaily Charts Pipeline completed successfully in {elapsed_time:.2f} seconds")

        except Exception as e:
            print(f"Pipeline failed: {str(e)}")
            raise
        finally:
            self.loader.close_session()


def run_daily_charts():
    """Entry point for running the daily charts pipeline"""
    pipeline = DailyChartsPipeline()
    pipeline.run()


if __name__ == "__main__":
    run_daily_charts()
