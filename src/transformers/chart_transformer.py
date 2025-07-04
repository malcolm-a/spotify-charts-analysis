import re


def parse_number(text: str) -> int:
    """Transforms text to number

    Args:
        text (str): the text to transform

    Returns:
        int: the transformed number
    """
    if not text or text == '-':
        return 0
    return int(text.replace(",", "").replace("+", ""))


def extract_artists_and_title(text_cell) -> tuple:
    """Extracts artists and title from the text cell

    Args:
        text_cell: the cell containing the text (BeautifulSoup element)

    Returns:
        tuple: the song id, song name, and the list of its artists
    """
    artists = []
    song_id = None
    song_name = None

    links = text_cell.find_all('a')

    for i, link in enumerate(links):
        href = link.get('href', '')

        # extract song details (only the first track link)
        if '/track/' in href and not song_id:
            song_match = re.search(r'/track/([^.]+)\.html', href)
            if song_match:
                song_id = song_match.group(1)
                song_name = link.text.strip()

        # extract artist details
        if '/artist/' in href:
            artist_match = re.search(r'/artist/([^.]+)\.html', href)
            if artist_match:
                artist_id = artist_match.group(1)
                artist_name = link.text.strip()
                artists.append({
                    'spotify_id': artist_id,
                    'name': artist_name
                })

    return song_id, song_name, artists
