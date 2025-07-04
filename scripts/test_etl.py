#!/usr/bin/env python3
"""
ETL Components Test Suite using pytest
Keeps the same tests but in a cleaner pytest format
"""

import sys
import os
import pytest
from datetime import datetime

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))


class TestChartTransformer:
    """Test the chart transformer functions"""

    def test_parse_number(self):
        from src.transformers.chart_transformer import parse_number

        test_cases = [
            ("1,234,567", 1234567),
            ("100+", 100),
            ("-", 0),
            ("", 0),
            ("500", 500)
        ]

        for input_val, expected in test_cases:
            result = parse_number(input_val)
            assert result == expected, f"parse_number('{input_val}') = {result}, expected: {expected}"


class TestStatsTransformer:
    """Test the stats transformer functions"""

    def test_parse_number(self):
        from src.transformers.stats_transformer import parse_number

        test_cases = [
            (None, None),
            ("-", None),
            ("1,000", 1000.0),
            (500, 500.0),
            ("", None)
        ]

        for input_val, expected in test_cases:
            result = parse_number(input_val)
            assert result == expected, f"parse_number({input_val}) = {result}, expected: {expected}"

    def test_normalize_artist_stats(self):
        from src.transformers.stats_transformer import normalize_artist_stats

        test_data = {
            'total_streams': '1,000,000',
            'daily_streams': 50000,
            'listeners': None
        }

        result = normalize_artist_stats(test_data)
        expected = {'total_streams': 1000000.0, 'daily_streams': 50000.0, 'listeners': None}
        assert result == expected


class TestDatabaseConnection:
    """Test database connection"""

    def test_database_connection(self):
        try:
            from src.config.connection import get_session
            import sqlalchemy as sa
            session = get_session()

            # Test a simple query
            result = session.execute(sa.text("SELECT 1 as test")).fetchone()
            assert result is not None
            assert result[0] == 1

            session.close()
        except Exception as e:
            pytest.fail(f"Database connection failed: {str(e)}")


class TestPostgresLoader:
    """Test the PostgreSQL loader"""

    def test_postgres_loader_init(self):
        from src.loaders.postgres_loader import PostgresLoader
        from src.models.schema import ensure_schema_exists

        # Ensure schema exists first
        ensure_schema_exists()

        loader = PostgresLoader()
        assert loader is not None

        # Test with dummy data (won't actually insert due to conflicts)
        test_artists = [{'spotify_id': 'test123', 'name': 'Test Artist'}]
        test_songs = [{'song_id': 'test456', 'name': 'Test Song'}]

        # These should not raise exceptions
        loader.load_artists(test_artists)
        loader.load_songs(test_songs)
        loader.close_session()


class TestNetworkExtractors:
    """Test network-dependent extractors (requires internet)"""

    @pytest.mark.network
    def test_kworb_charts_extractor(self):
        from src.extractors.kworb_charts_extractor import fetch_country_charts

        # Test with a small country to avoid too much data
        result = fetch_country_charts('US')

        assert isinstance(result, dict)
        assert 'charts' in result
        assert 'songs' in result
        assert 'artists' in result
        assert 'artist_songs' in result
        assert len(result.get('charts', [])) > 0

    @pytest.mark.network
    @pytest.mark.skipif(
        not (os.getenv("SPOTIFY_CLIENT_ID") and os.getenv("SPOTIFY_CLIENT_SECRET")),
        reason="Spotify API credentials not found"
    )
    def test_spotify_api_extractor(self):
        from src.extractors.spotify_api_extractor import SpotifyAPIExtractor

        extractor = SpotifyAPIExtractor()
        assert extractor is not None

        # Test with a known artist ID (Drake)
        test_artist_ids = ['3TVXtAsR1Inumwj472S9r4']
        result = extractor.fetch_artists_batch(test_artist_ids)

        assert isinstance(result, list)
        assert len(result) > 0
        assert result[0].get('name') is not None


# Run specific test groups
if __name__ == "__main__":
    print(f"ETL Components Test Suite")
    print(f"Started at: {datetime.now()}")
    print("=" * 50)

    # Run all tests except network tests
    pytest.main([__file__, "-v", "-m", "not network"])

    print("\nTo run network tests:")
    print("pytest test_etl.py -v -m network")
