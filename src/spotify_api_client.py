"""Spotify Web API client and data fetching utilities.

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

import functools
import logging
import math
import os
from typing import Any, Callable, Dict, List, Optional

import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Logger
logger = logging.getLogger(__name__)


class SpotipyClient:
    """Client for accessing the spotify python class."""

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
        """Initializes the SpotipyClient instance.

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
        logger.debug("Attempting to initialize SpotipyClient...")

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
            logger.critical(error_msg)
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
            logger.info("SpotipyClient initialized successfully.")
        except spotipy.exceptions.SpotifyException as e:
            logger.critical(f"Spotify authentication failed: {e}")
            raise  # Re-raise the specific Spotify exception
        except Exception as e:
            error_message = (
                f"Unexpected error occurred during client initialization: {e}"
            )
            logger.critical(error_message)
            raise  # Re-raise any other unexpected exceptions

    def _fetch_paginated_items(
        self, initial_call: Callable[..., Dict[str, Any]], item_type: str
    ) -> List[Dict]:
        """Generic private helper to fetch all items from paginated Spotify API
        endpoints.

        Args:
            initial_call (Callable):
              The Spotipy client method to call for the first page.
              This function should return a dictionary with 'items'
              and 'next' keys.
              Example: `self.client.current_user_saved_tracks`
            item_type (str):
              A string describing the type of item being fetched
              (e.g., "songs", "playlists") for use in logging messages.

        Returns:
            list:
              A list of dictionaries, where each dictionary represents
              an item.
              Returns an empty list if no items are found or an error occurs.
        """
        all_items = []
        logger.debug(f"Starting to scrape user's {item_type}...")

        try:
            results = initial_call()
            if not results or not results.get("items"):
                logger.info(f"No {item_type} found or first page was empty.")
                return []

            all_items.extend(results["items"])
            logger.debug(
                f"Retrieved initial batch: {len(all_items)} {item_type}."
            )

            # Continue fetching pages as long as there's a 'next' URL
            while results.get("next"):
                results = self.client.next(results)
                if not results or not results.get("items"):
                    logger.debug(
                        f"No more {item_type} in the current page. "
                        "Exiting pagination."
                    )
                    break  # Break if a page is empty or malformed unexpectedly
                all_items.extend(results["items"])
                logger.debug(f"Retrieved total: {len(all_items)} {item_type}.")

        except spotipy.exceptions.SpotifyException as e:
            logger.error(f"Spotify API error while fetching {item_type}: {e}")
            return []
        except Exception as e:
            logger.error(
                f"An unexpected error occurred while fetching {item_type}: {e}"
            )
            return []

        logger.debug(f"Successfully scraped {len(all_items)} {item_type}.")
        return all_items

    def get_users_liked_songs(self) -> List[Dict]:
        """Retrieves all liked songs from the authenticated user's Spotify
        library.

        Returns:
            list:
              A list of dictionaries, where each dictionary represents
              a liked song. Returns an empty list if no songs are found
              or an error occurs.
        """
        return self._fetch_paginated_items(
            self.client.current_user_saved_tracks, "liked songs"
        )

    def get_users_playlists(self) -> List[Dict]:
        """Retrieves all playlists created or followed by the authenticated
        user.

        Returns:
            list:
              A list of dictionaries, where each dictionary represents
              a playlist.
              Returns an empty list if no playlists are found or an
              error occurs.
        """
        return self._fetch_paginated_items(
            self.client.current_user_playlists, "playlists"
        )

    def get_playlist_tracks(self, playlist_id: str) -> List[Dict]:
        """Retrieves all tracks from a specific Spotify playlist.

        Args:
            playlist_id (str): The Spotify ID of the playlist.

        Returns:
            list:
              A list of dictionaries, where each dictionary represents a
              track in the playlist.
              Returns an empty list if no tracks are found or an error
              occurs.
        """
        # Use functools.partial to create a callable that takes no arguments
        # but already has playlist_id bound to it.
        initial_call_with_id = functools.partial(
            self.client.playlist_tracks, playlist_id
        )

        return self._fetch_paginated_items(
            initial_call_with_id, f"tracks for playlist ID '{playlist_id}'"
        )

    def get_track_info_by_ids(self, track_ids: List[str]) -> List[Dict]:
        """Retrieves tracks based on the IDs that are passed to the function.

        Args:
          track_ids:
            A list of track IDs, must be less that 50 in length due to
            API limits

        Returns:
          list:
            A list of dictionaries, where each dictionary represents a
            track returned from the API for the provided IDs.
            Returns an empty list if no tracks are found or an error
            occurs.

        Raises:
          ValueError:
              If more than 50 track IDs are passed in the args, this
              exceeds the API limits
        """

        if len(track_ids) == 0:
            return []
        if len(track_ids) > 50:
            raise ValueError(
                f"Provided list of IDs ({len(track_ids)}) "
                "exceeds maximum amount (50)"
            )
        # Fetch Tracks
        try:
            track_data = self.client.tracks(track_ids)
            # Data returned as as Dict, we want the list of
            # tracks using "tracks" key
            return track_data["tracks"]
        except spotipy.exceptions.SpotifyException as e:
            logger.error(
                "Spotify API error while fetching track "
                f"info for IDs {track_ids}: {e}"
            )
            return []
        except Exception as e:
            # Catch any other unexpected errors
            logger.error(
                "An unexpected error occurred while fetching track"
                f" info for IDs {track_ids}: {e}"
            )
            return []

    def get_track_info_in_batches(self, track_ids: List[str]) -> List[Dict]:
        """Retrieves detailed information for a potentially large list of
        Spotify track IDs by automatically splitting them into batches of 50 to
        adhere to Spotify API limits.

        This method handles the batching transparently for the user.

        Args:
            track_ids (List[str]): A list of Spotify track IDs of any length.

        Returns:
            List[Dict]:
              A list of dictionaries, where each dictionary represents a
              track's detailed information. Returns an empty list if
              no valid tracks are found for the given IDs across all batches,
              or if significant errors occur during batch processing.
        """
        if not track_ids:
            logger.info(
                "No track IDs provided to get_track_info_in_batches. "
                "Returning empty list."
            )
            return []

        all_retrieved_tracks = []
        total_ids = len(track_ids)
        batch_size = (
            50  # Max allowed by get_track_info_by_ids for a single call
        )

        logger.info(
            f"Starting to retrieve info for {total_ids}"
            f" tracks in batches of {batch_size}."
        )

        try:
            # Iterate through the track_ids list, taking chunks of batch_size
            for idx in range(0, total_ids, batch_size):
                current_batch_ids = track_ids[idx : idx + batch_size]

                # Calculate batch number for logger
                batch_number = (idx // batch_size) + 1
                total_batches = math.ceil(total_ids / batch_size)

                logger.debug(
                    f"Processing batch {batch_number}/{total_batches}"
                    f" with {len(current_batch_ids)} IDs."
                )

                # Call the underlying function which already handles its own
                # errors and filtering
                batch_results = self.get_track_info_by_ids(current_batch_ids)

                # Extend the overall list with results from the current batch
                all_retrieved_tracks.extend(batch_results)

        except Exception as e:
            # This 'catch-all' here is for errors in the batching logic itself,
            # as get_track_info_by_ids handles API errors.
            logger.error(
                "An unexpected error occurred during batch "
                f"processing tracks: {e}"
            )
            # Decide if you want to return partial results or raise the
            # exception.
            # Returning partial results is often more user-friendly for
            # large operations.
            return all_retrieved_tracks if all_retrieved_tracks else []

        logger.info(
            f"Finished retrieving info for {len(all_retrieved_tracks)}"
            f" valid tracks from {total_ids} requested IDs."
        )
        return all_retrieved_tracks
