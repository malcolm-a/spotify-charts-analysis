#!/usr/bin/env python3
"""
Main entry point for the Spotify Charts ETL Pipeline
Uses the new ETL architecture with proper separation of concerns
"""

import sys
import os
import time
from datetime import datetime

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.pipelines.orchestrator import PipelineOrchestrator


def run_pipeline():
    """Run the ETL pipeline with proper error handling and logging"""
    print(f"=== Spotify Charts ETL Pipeline ===")
    print(f"Started at: {datetime.now()}")
    print("=" * 50)

    try:
        orchestrator = PipelineOrchestrator()
        orchestrator.run_daily_pipeline()

        print("=" * 50)
        print(f"Pipeline completed successfully at: {datetime.now()}")

    except Exception as e:
        print(f"Pipeline failed with error: {str(e)}")
        print(f"Failed at: {datetime.now()}")
        sys.exit(1)


def run_scheduler():
    """Run the pipeline on a continuous schedule"""
    print(f"=== Spotify Charts ETL Pipeline Scheduler ===")
    print(f"Started at: {datetime.now()}")
    print("Press Ctrl+C to stop the scheduler")
    print("=" * 50)

    try:
        orchestrator = PipelineOrchestrator()
        orchestrator.run_scheduler()

    except KeyboardInterrupt:
        print("\nScheduler stopped by user")
    except Exception as e:
        print(f"Scheduler failed with error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "scheduler":
        run_scheduler()
    else:
        run_pipeline()
