"""
Spotify Web API client and data fetching utilities.

This module provides an interface for interacting with the Spotify Web API
using the `spotipy` library. It focuses on abstracting API requests to fetch
additional metadata (like track details, audio features, album information)
that is not available in the raw streaming history files.

Key functionalities include:
  - Initializing and authenticating the `spotipy` client.
  - Fetching detailed track information for a list of Spotify track IDs.
  - Transforming raw API responses into a structured format
    (e.g., Pandas DataFrame) suitable for downstream data processing and
    analysis.

This module is designed to be the single point of contact for all external
Spotify API interactions, isolating network operations and API-specific logic
from the core data pipeline.
"""

import logging
import os
from typing import Dict, List, Optional

import spotipy
from spotipy.oauth2 import SpotifyOAuth


class SpotipyClient:
    """
    Client for accessing the spotify python class
    """

    client: spotipy.Spotify

    _client_id: Optional[str]
    _client_secret: Optional[str]
    _redirect_uri: Optional[str]
    _scope: Optional[str]

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_uri: Optional[str] = None,
        scope: Optional[str] = None,
    ):
        """
        Initializes the SpotipyClient instance.

        Args:
            client_id (str, optional):
              Your Spotify application Client ID.
              Defaults to SPOTIPY_CLIENT_ID environment variable.
            client_secret (str, optional):
              Your Spotify application Client Secret.
              Defaults to SPOTIPY_CLIENT_SECRET environment variable.
            redirect_uri (str, optional):
              The URI to redirect to after authentication.
              Defaults to SPOTIPY_REDIRECT_URI environment variable,
              or "http://localhost:8890/" if not set.
            scope (str, optional):
              The Spotify API scope.
              Defaults to SPOTIPY_SCOPE environment variable,
              or "user-library-read" if not set.

        Raises:
            ValueError:
              If critical Spotify API credentials (client_id, client_secret)
              are not provided either as arguments or environment variables.
            spotipy.exceptions.SpotifyException:
              If there's an issue during Spotify authentication.
        """
        logging.debug("Attempting to initialize SpotipyClient...")

        # Prioritize arguments, then environment variables,
        # then sensible defaults
        self._client_id = client_id or os.getenv("SPOTIPY_CLIENT_ID")
        self._client_secret = client_secret or os.getenv(
            "SPOTIPY_CLIENT_SECRET"
        )
        self._redirect_uri = redirect_uri or os.getenv(
            "SPOTIPY_REDIRECT_URI", "http://localhost:8890/"
        )
        self._scope = scope or os.getenv("SPOTIPY_SCOPE", "user-library-read")

        # Validate that essential credentials are present
        if not self._client_id or not self._client_secret:
            error_msg = (
                "Spotify Client ID and Client Secret must be provided "
                "either as arguments or via SPOTIPY_CLIENT_ID and "
                "SPOTIPY_CLIENT_SECRET environment variables."
            )
            logging.critical(error_msg)
            raise ValueError(error_msg)

        try:
            # Instantiate the Spotify client with OAuth manager
            self.client: spotipy.Spotify = spotipy.Spotify(
                auth_manager=SpotifyOAuth(
                    client_id=self._client_id,
                    client_secret=self._client_secret,
                    redirect_uri=self._redirect_uri,
                    scope=self._scope,
                )
            )
            logging.info("SpotipyClient initialized successfully.")
        except spotipy.exceptions.SpotifyException as e:
            logging.critical(f"Spotify authentication failed: {e}")
            raise  # Re-raise the specific Spotify exception
        except Exception as e:
            error_message = (
                f"Unexpected error occurred during client initialization: {e}"
            )
            logging.critical(error_message)
            raise  # Re-raise any other unexpected exceptions

    def get_users_liked_songs(self) -> List[Dict]:
        """
        Retrieves all liked songs from the authenticated user's
        Spotify library.

        Handles pagination automatically to fetch all available tracks.

        Returns:
            list:
              A list of dictionaries, where each dictionary represents
              a liked song.
              Returns an empty list if no songs are found or an error occurs.
        """
        all_liked_songs = []
        logging.info("Starting to scrape user's liked songs...")

        try:
            results = self.client.current_user_saved_tracks()
            if not results or not results.get("items"):
                logging.info("No liked songs found or first page was empty.")
                return []

            all_liked_songs.extend(results["items"])
            logging.debug(
                f"Retrieved initial batch: {len(all_liked_songs)} songs."
            )

            # Continue fetching pages as long as there's a 'next' URL
            while results.get("next"):
                results = self.client.next(results)
                if not results or not results.get("items"):
                    logging.debug(
                        "No more items in the current page. Exiting."
                    )
                    break  # Break if a page is empty or malformed unexpectedly
                all_liked_songs.extend(results["items"])
                logging.debug(
                    f"Retrieved total: {len(all_liked_songs)} songs."
                )

        except spotipy.exceptions.SpotifyException as e:
            logging.error(f"Spotify API error while fetching liked songs: {e}")
            return []
        except Exception as e:
            logging.error(
                f"An unexpected error occurred while fetching liked songs: {e}"
            )
            return []

        logging.info(
            f"Successfully scraped {len(all_liked_songs)} liked songs"
        )
        return all_liked_songs

    def get_users_playlists(self) -> List[Dict]:
        """
        Retrieves all users playlists from the authenticated user's
        Spotify library.

        Handles pagination automatically to fetch all available playlists

        Returns:
            list:
              A list of dictionaries, where each dictionary represents
              the available data for a playlist
              Returns an empty list if no playlists are found
              or an error occurs.
        """
        all_playlists = []
        logging.info("Starting to scrape user's playlists...")
        try:
            results = self.client.current_user_playlists()
            if not results or not results.get("items"):
                logging.info("No playlists found or first page was empty.")
                return []

            all_playlists.extend(results["items"])
            logging.debug(
                f"Retrieved initial batch: {len(all_playlists)} playlists."
            )

            # Continue fetching pages as long as there's a 'next' URL
            while results.get("next"):
                results = self.client.next(results)
                if not results or not results.get("items"):
                    logging.debug(
                        "No more items in the current page. Exiting."
                    )
                    break  # Break if a page is empty or malformed unexpectedly
                all_playlists.extend(results["items"])
                logging.debug(f"Retrieved total: {len(all_playlists)} songs.")

        except spotipy.exceptions.SpotifyException as e:
            logging.error(f"Spotify API error while fetching liked songs: {e}")
            return []
        except Exception as e:
            logging.error(
                f"An unexpected error occurred while fetching liked songs: {e}"
            )
            return []

        logging.info(f"Successfully scraped {len(all_playlists)} playlists")
        return all_playlists
