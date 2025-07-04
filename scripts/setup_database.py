#!/usr/bin/env python3
"""
Database setup script for Spotify Charts ETL Pipeline
This script creates all necessary tables and initial data
"""

import sys
import os
from pathlib import Path

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    from src.config.connection import get_engine, get_session
    from src.models.database import Base, Country
    from sqlalchemy import inspect, text
    import pandas as pd
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Make sure you're in the virtual environment and all dependencies are installed:")
    print("pip install -r requirements.txt")
    sys.exit(1)


def drop_and_recreate_tables():
    """Drop existing tables and recreate them with proper constraints"""
    print("Dropping and recreating database tables...")
    try:
        engine = get_engine()
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()

        # Drop tables if they exist (in reverse order to handle foreign keys)
        tables_to_drop = ['artist_song', 'spotify_charts', 'artist_stats', 'song', 'artist', 'country']
        for table in tables_to_drop:
            if table in existing_tables:
                print(f"Dropping {table} table...")
                with engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
                    conn.commit()

        # Create all tables
        Base.metadata.create_all(engine)
        print("✓ All tables created successfully with proper constraints")
        return True
    except Exception as e:
        print(f"✗ Error creating tables: {e}")
        return False


def check_tables():
    """Check which tables exist"""
    print("Checking existing tables...")
    try:
        engine = get_engine()
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()

        expected_tables = ['artist', 'song', 'artist_song', 'country', 'spotify_charts', 'artist_stats']

        for table in expected_tables:
            if table in existing_tables:
                print(f"✓ {table} table exists")
            else:
                print(f"✗ {table} table missing")

        return len([t for t in expected_tables if t in existing_tables]) == len(expected_tables)
    except Exception as e:
        print(f"✗ Error checking tables: {e}")
        return False


def load_countries_from_csv():
    """Load countries data from CSV file"""
    csv_path = Path("data/reference/countries.csv")
    if not csv_path.exists():
        print(f"✗ Countries CSV not found at {csv_path}")
        return []

    try:
        df = pd.read_csv(csv_path)
        countries_data = []

        for _, row in df.iterrows():
            countries_data.append({
                'country_code': row['alpha-2'],
                'country_name': row['name'],
                'region': row['region']
            })

        return countries_data
    except Exception as e:
        print(f"✗ Error reading countries CSV: {e}")
        return []


def insert_countries():
    """Insert countries data into the database"""
    print("Inserting countries data...")
    try:
        session = get_session()

        # Check if countries already exist
        existing_count = session.query(Country).count()
        if existing_count > 0:
            print(f"✓ Countries already exist ({existing_count} countries)")
            session.close()
            return True

        countries_data = load_countries_from_csv()

        if not countries_data:
            print("✗ No countries data loaded from CSV")
            return False

        for country_data in countries_data:
            country = Country(
                country_code=country_data['country_code'],
                country_name=country_data['country_name'],
                region=country_data['region']
            )
            session.add(country)

        session.commit()
        session.close()
        print(f"✓ Inserted {len(countries_data)} countries")
        return True

    except Exception as e:
        print(f"✗ Error inserting countries: {e}")
        if 'session' in locals():
            session.rollback()
            session.close()
        return False


def test_connection():
    """Test database connection"""
    print("Testing database connection...")
    try:
        session = get_session()
        result = session.execute(text("SELECT 1 as test")).fetchone()
        session.close()

        if result and result[0] == 1:
            print("✓ Database connection successful")
            return True
        else:
            print("✗ Database query failed")
            return False

    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False


def main():
    """Main setup function"""
    print("=== Spotify Charts ETL Database Setup ===")
    print()

    # Test connection first
    if not test_connection():
        print("\nDatabase connection failed. Please check your configuration:")
        print("1. Make sure PostgreSQL is running (docker-compose up -d)")
        print("2. Check your .env file has correct database credentials")
        sys.exit(1)

    print()

    # Create tables with proper constraints
    if not drop_and_recreate_tables():
        print("\nFailed to create tables. Exiting.")
        sys.exit(1)

    print()

    # Check tables
    if not check_tables():
        print("\nSome tables are missing. Please check for errors above.")
        sys.exit(1)

    print()

    # Insert countries
    if not insert_countries():
        print("\nFailed to insert countries. Exiting.")
        sys.exit(1)

    print()
    print("=== Database Setup Complete ===")
    print("✓ All tables created")
    print("✓ Countries data inserted")
    print("✓ Database ready for ETL pipeline")
    print()
    print("You can now run:")
    print("python run_etl_pipeline.py")
    print("or")
    print("python -m src.pipelines.orchestrator --mode charts")


if __name__ == "__main__":
    main()
