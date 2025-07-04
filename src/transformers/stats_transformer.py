try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


def parse_number(value):
    """Parse various number formats from kworb.net stats

    Args:
        value: The value to parse (can be string, int, float, numpy types, or None)

    Returns:
        float or None: The parsed number or None if invalid
    """
    if value is None or value == '-' or value == '':
        return None

    # Handle numeric types (including numpy if available)
    if isinstance(value, (int, float)):
        return float(value)

    if HAS_NUMPY and isinstance(value, (np.integer, np.floating)):
        return float(value)

    if isinstance(value, str):
        clean_value = value.replace(",", "").strip()
        if clean_value == '':
            return None
        return float(clean_value)
    return None


def normalize_artist_stats(stats_data):
    """Normalize artist statistics data

    Args:
        stats_data (dict): Raw stats data from extractor

    Returns:
        dict: Normalized stats data
    """
    if not stats_data:
        return None

    return {
        'total_streams': parse_number(stats_data.get('total_streams')),
        'daily_streams': parse_number(stats_data.get('daily_streams')),
        'listeners': parse_number(stats_data.get('listeners'))
    }


def normalize_listeners_data(listeners_list):
    """Normalize listeners data from kworb.net

    Args:
        listeners_list (list): List of raw listeners data

    Returns:
        dict: Normalized listeners data mapped by artist name
    """
    listeners_map = {}

    for item in listeners_list:
        if item.get('artist_name') and item.get('listeners') is not None:
            listeners_map[item['artist_name']] = parse_number(item['listeners'])

    return listeners_map
