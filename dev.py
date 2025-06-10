"""Temporary script being used to develop out functionality."""

import logging

from dotenv import load_dotenv

from src.spotify_api_client import SpotipyClient

load_dotenv()  # Load environment variables from .env file

# Setup Logger
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for more verbose output
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)  # Logger for this specific script


def main():
    """Entry point to script."""
    client = SpotipyClient()
    playlists = client.get_users_playlists()
    playlist_id = playlists[0]["id"]

    songs = client.get_playlist_tracks(playlist_id)
    print(len(songs))


if __name__ == "__main__":
    main()
