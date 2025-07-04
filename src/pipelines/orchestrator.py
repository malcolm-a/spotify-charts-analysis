import time
import schedule
import logging
import sys
from datetime import datetime
from src.pipelines.daily_charts_pipeline import DailyChartsPipeline
from src.pipelines.artist_stats_pipeline import ArtistStatsPipeline
from src.pipelines.spotify_metadata_pipeline import SpotifyMetadataPipeline

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Main orchestrator for all ETL pipelines"""

    def __init__(self):
        self.daily_charts_pipeline = DailyChartsPipeline()
        self.artist_stats_pipeline = ArtistStatsPipeline()
        self.spotify_metadata_pipeline = SpotifyMetadataPipeline()

    def run_daily_pipeline(self):
        """Run all daily pipelines in sequence"""
        logger.info("Starting daily ETL pipeline orchestration")
        start_time = time.time()

        try:
            # 1. Daily Charts Pipeline
            logger.info("=== Running Daily Charts Pipeline ===")
            self.daily_charts_pipeline.run()

            # 2. Artist Stats Pipeline
            logger.info("\n=== Running Artist Stats Pipeline ===")
            self.artist_stats_pipeline.run()

            # 3. Spotify Metadata Pipeline (for any new artists/songs)
            logger.info("\n=== Running Spotify Metadata Pipeline ===")
            self.spotify_metadata_pipeline.run_complete_metadata_pipeline()

            total_time = time.time() - start_time
            logger.info(f"Daily pipeline orchestration completed successfully in {total_time:.2f} seconds")

        except Exception as e:
            logger.error(f"Daily pipeline orchestration failed: {str(e)}")
            raise

    def run_metadata_only(self):
        """Run only the Spotify metadata pipeline"""
        logger.info("Running Spotify metadata pipeline only")
        try:
            self.spotify_metadata_pipeline.run_complete_metadata_pipeline()
            logger.info("Metadata pipeline completed successfully")
        except Exception as e:
            logger.error(f"Metadata pipeline failed: {str(e)}")
            raise

    def run_charts_only(self):
        """Run only the daily charts pipeline"""
        logger.info("Running daily charts pipeline only")
        try:
            self.daily_charts_pipeline.run()
            logger.info("Charts pipeline completed successfully")
        except Exception as e:
            logger.error(f"Charts pipeline failed: {str(e)}")
            raise

    def run_stats_only(self):
        """Run only the artist stats pipeline"""
        logger.info("Running artist stats pipeline only")
        try:
            self.artist_stats_pipeline.run()
            logger.info("Stats pipeline completed successfully")
        except Exception as e:
            logger.error(f"Stats pipeline failed: {str(e)}")
            raise

    def run_scheduler(self):
        """Run the pipeline on a schedule"""
        try:
            # Run immediately on startup
            logger.info("Running initial pipeline execution")
            self.run_daily_pipeline()

            # Schedule daily runs at 2 AM
            schedule.every().day.at("02:00").do(self.run_daily_pipeline)

            # Schedule metadata updates every 6 hours
            schedule.every(6).hours.do(self.run_metadata_only)

            logger.info("Pipeline scheduler started. Daily runs at 2 AM, metadata updates every 6 hours.")
            logger.info("Press Ctrl+C to stop the scheduler")

            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute

        except KeyboardInterrupt:
            logger.info("Pipeline scheduler manually stopped")
        except Exception as e:
            logger.error(f"Scheduler error: {str(e)}")
            raise


def main():
    """Main entry point for pipeline orchestration"""
    import argparse

    parser = argparse.ArgumentParser(description='Spotify Charts ETL Pipeline Orchestrator')
    parser.add_argument(
        '--mode',
        choices=['daily', 'charts', 'stats', 'metadata', 'scheduler'],
        default='daily',
        help='Pipeline mode to run'
    )

    args = parser.parse_args()
    orchestrator = PipelineOrchestrator()

    try:
        if args.mode == 'daily':
            orchestrator.run_daily_pipeline()
        elif args.mode == 'charts':
            orchestrator.run_charts_only()
        elif args.mode == 'stats':
            orchestrator.run_stats_only()
        elif args.mode == 'metadata':
            orchestrator.run_metadata_only()
        elif args.mode == 'scheduler':
            orchestrator.run_scheduler()

    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
