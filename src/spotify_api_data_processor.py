"""This module provides a robust data processing utility for Spotify API
responses.

It contains the `SpotifyDataProcessor` class, which is
responsible for transforming raw, nested JSON data received
from the Spotify API (e.g., track objects, playlist items,
saved track items) into clean, flat, and consistent pandas
DataFrames.

Key functionalities include:
- Safe extraction of relevant features from complex Spotify
    track objects.
- Handling of potential missing keys or malformed data in
    API responses without crashing.
- Efficient conversion of lists of processed dictionaries
    into DataFrames.
- Support for tagging processed data with its source
    (e.g., "liked songs").

This module promotes separation of concerns by isolating data
transformation logic from the API interaction logic found in
`spotify_client`.
"""

import logging
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

# Logger
logger = logging.getLogger(__name__)


class SpotifyApiDataProcessor:
    """A class responsible for processing raw Spotify API data into structured
    pandas DataFrames."""

    def __init__(self):
        """Initializes the SpotifyDataProcessor.

        No specific configuration needed for basic track processing.
        """
        logger.debug("SpotifyDataProcessor initialized.")

    # TODO: Reduce function complexity
    def _extract_track_features(
        self, track_data: Dict[str, Any]
    ) -> Dict[str, Union[str, int, float, List[Any | None], None]]:
        """Extracts and flattens relevant features from a single raw Spotify
        track dictionary.

        This method uses dictionary's .get() method with default
        values to safely handle cases where keys or nested structures
        might be missing in the Spotify API response, preventing KeyErrors.

        Args:
            track_data (Dict[str, Any]):
                A dictionary representing a single raw track
                object from the Spotify API response. This
                could be the 'track' object itself (from
                e.g., get_track_info_by_ids) or the nested
                'track' dictionary within a saved track item.

        Returns:
            Dict[str, Union[str, int, float, List[str], None]]:
                A dictionary with extracted and flattened track
                features. Keys are standardized for DataFrame columns.
                Returns an empty dictionary if the input track_data
                is not valid (e.g., not a dictionary).
        """
        # Safely extract top-level track information
        added_at = track_data.get("added_at")

        track_details = track_data.copy()
        if "track" in track_details:
            track_details = track_details["track"]

        track_name = track_details.get("name")
        track_id = track_details.get("id")
        track_duration_ms = track_details.get("duration_ms")
        track_popularity = track_details.get("popularity")

        # Extract artists from the track: robustly handle missing 'artists'
        # key or empty lists
        # Also ensure each artist dict has a 'name' key
        track_artists = [
            artist.get("name")
            for artist in track_details.get("artists", [])
            if isinstance(artist, dict) and artist.get("name")
        ]

        # Safely extract album information, providing an empty dict if 'album'
        # key is missing
        album_info = track_details.get("album", {})
        album_name = album_info.get("name")
        album_release_date = album_info.get("release_date")

        # Extract artists from the album: similar robust handling as
        # track artists
        album_artists = [
            artist.get("name")
            for artist in album_info.get("artists", [])
            if isinstance(artist, dict) and artist.get("name")
        ]

        # Extract album artwork URL: handle nested keys and potential
        # absence of images
        album_artwork_url = np.nan  # Default to NaN as per original request
        album_images = album_info.get("images")
        if (
            album_images
            and isinstance(album_images, list)
            and len(album_images) > 0
        ):
            first_image = album_images[0]
            if isinstance(first_image, dict):
                album_artwork_url = first_image.get("url", np.nan)

        # Return a flattened dictionary with standardized column names
        return {
            "track_id": track_id,
            "track_name": track_name,
            "track_artists": track_artists,
            "track_duration_ms": track_duration_ms,
            "track_popularity": track_popularity,
            "album_name": album_name,
            "album_release_date": album_release_date,
            "album_artists": album_artists,
            "album_artwork_url": album_artwork_url,
            "added_at": added_at,
        }

    def process_tracks_to_dataframe(
        self,
        raw_tracks_data: List[Dict[str, Any]],
        source_playlist: Optional[str] = None,
    ) -> pd.DataFrame:
        """Processes a list of raw Spotify track dictionaries into a structured
        pandas DataFrame.

        This method iterates through a list of raw API responses, extracts
        relevant features for each track, and compiles them into a DataFrame.
        It is designed to handle different Spotify API response formats for
        tracks (e.g., from user saved tracks which nest the track under an
        'item' key, or direct track objects).

        Args:
            raw_tracks_data (List[Dict[str, Any]]):
                A list of dictionaries, where each
                dictionary represents a raw track item
                or track object from the Spotify API.
                Examples:
                    - `[{'added_at': '...', 'track': {...}}, ...]`
                    - `[{'album': {...}, 'artists': [...], ...}, ...]`
            source_playlist (str, optional):
                An optional string to tag all tracks in the
                resulting DataFrame with their origin (e.g.,
                "liked_songs", "My Favorite Playlist"). Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame containing processed track information.
                          Returns an empty DataFrame if the input list is empty
                          or invalid, or if no valid track records could be
                          extracted.
        """
        if not isinstance(raw_tracks_data, list) or not raw_tracks_data:
            logger.info(
                "No raw track data provided for processing."
                "Returning empty DataFrame."
            )
            return (
                pd.DataFrame()
            )  # Return empty DataFrame if input is invalid or empty

        logger.info(
            f"Starting to process {len(raw_tracks_data)} "
            "raw track entries into DataFrame."
        )

        processed_records = []
        for idx, track_dict in enumerate(raw_tracks_data):
            # Spotify API responses for saved tracks or playlist tracks often
            # wrap the actual track object under a "track" key. Other endpoints
            # (like get_track_info_by_ids) return the track object directly.
            # This handles both cases.

            if track_dict and isinstance(track_dict, dict):
                # Call the private helper to extract features safely
                extracted_features = self._extract_track_features(track_dict)
                if (
                    extracted_features
                ):  # Only append if features were successfully extracted
                    processed_records.append(extracted_features)
            else:
                logger.warning(
                    f"Item {idx} in raw_tracks_data was not a "
                    f"valid track dictionary or item. Skipping: {track_dict}"
                )

        if not processed_records:
            logger.debug(
                "No valid track records extracted after processing. "
                "Returning empty DataFrame."
            )
            return pd.DataFrame()

        # Create DataFrame from the list of dictionaries
        df = pd.DataFrame(processed_records)

        # Add the source_playlist column if provided
        if source_playlist:
            df["playlists"] = source_playlist
            logger.debug(
                f"Added 'playlists' column with value '{source_playlist}'."
            )
        else:
            df["playlists"] = np.nan

        logger.debug(
            f"Successfully processed {len(processed_records)} tracks "
            f"into a DataFrame with {len(df.columns)} columns."
        )
        return df

    @staticmethod
    def _aggregate_source_playlists(source_playlists: pd.Series) -> List[str]:
        """Aggregates a series of source playlist names into a list, removing
        null values. Used as an aggregation function for groupby.

        Args:
            source_playlists (pd.Series):
                A pandas Series containing source playlist names
                for a single track_id group.

        Returns:
            list: A list of unique, non-null source playlist names.
                  Returns an empty list if all source_playlists are
                  null or the series is empty.
        """
        # Dropna removes pd.NA or None values
        # .unique() ensures only distinct playlist names are kept
        # .tolist() converts the resulting pandas Series or Index to
        # a Python list
        return source_playlists.dropna().unique().tolist()

    def aggregate_track_dataframes(
        self, list_of_track_dfs: List[pd.DataFrame]
    ) -> pd.DataFrame:
        """Aggregates multiple track DataFrames into a single DataFrame,
        removing duplicate tracks (based on 'track_id') and combining their
        source playlist information.

        For fields like track name, artists, album info, etc., the first
        non-null value encountered for a given track_id is kept. For the
        'source_playlist' column, all unique source playlist names
        associated with a track_id are collected into a list.

        Args:
            list_of_track_dfs (List[pd.DataFrame]):
                A list of pandas DataFrames,
                each containing processed track
                information (e.g., from
                process_tracks_to_dataframe).
                Each DataFrame should ideally
                have a 'track_id' column and
                a 'source_playlist' column.

        Returns:
            pd.DataFrame:
                A single DataFrame with de-duplicated tracks, where the
                'source_playlist' column is a list of all playlists
                a track belongs to. Returns an empty DataFrame if
                no valid DataFrames are provided or no tracks remain
                after aggregation.
        """
        if not isinstance(list_of_track_dfs, list) or not list_of_track_dfs:
            logger.info(
                "No DataFrames provided for aggregation. "
                "Returning empty DataFrame."
            )
            return pd.DataFrame()

        # Filter out empty or invalid DataFrames before concatenation
        valid_dfs = [
            df
            for df in list_of_track_dfs
            if isinstance(df, pd.DataFrame) and not df.empty
        ]

        if not valid_dfs:
            logger.info(
                "No valid (non-empty) DataFrames found in the input "
                "list. Returning empty DataFrame."
            )
            return pd.DataFrame()

        logger.info(
            f"Aggregating {len(valid_dfs)} DataFrames, containing a total of "
            f"{sum(len(df) for df in valid_dfs)} rows before de-duplication."
        )

        # Concatenate all DataFrames into one. ignore_index=True
        # resets the index.
        combined_df = pd.concat(valid_dfs, ignore_index=True)

        # Perform the groupby aggregation
        try:
            aggregated_df = combined_df.groupby(
                "track_id", as_index=False
            ).agg(
                added_at=pd.NamedAgg(column="added_at", aggfunc="first"),
                album_release_date=pd.NamedAgg(
                    column="album_release_date", aggfunc="first"
                ),
                album_name=pd.NamedAgg(column="album_name", aggfunc="first"),
                album_artists=pd.NamedAgg(
                    column="album_artists", aggfunc="first"
                ),
                track_artists=pd.NamedAgg(
                    column="track_artists", aggfunc="first"
                ),
                track_duration_ms=pd.NamedAgg(
                    column="track_duration_ms", aggfunc="first"
                ),
                track_name=pd.NamedAgg(column="track_name", aggfunc="first"),
                track_popularity=pd.NamedAgg(
                    column="track_popularity", aggfunc="first"
                ),
                playlists=pd.NamedAgg(
                    column="playlists",
                    aggfunc=self._aggregate_source_playlists,
                ),
                album_artwork_url=pd.NamedAgg(
                    column="album_artwork_url", aggfunc="first"
                ),
            )
            logger.info(
                f"Aggregation complete. Reduced to {len(aggregated_df)} "
                "unique tracks."
            )
            return aggregated_df
        except Exception as e:
            logger.error(
                f"An error occurred during DataFrame aggregation: {e}"
            )
            return pd.DataFrame()
