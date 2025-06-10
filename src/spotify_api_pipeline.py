"""This module contains high-level pipeline functions that orchestrate the
fetching and processing of Spotify data."""

import logging
from typing import Set

import pandas as pd

# Assuming your classes are in these modules. Adjust paths if necessary.
from .spotify_api_client import SpotipyClient
from .spotify_api_data_processor import SpotifyApiDataProcessor

logger = logging.getLogger(__name__)


def _fetch_liked_songs_data(
    client: SpotipyClient, processor: SpotifyApiDataProcessor
) -> pd.DataFrame:
    """Fetch and process user's liked songs.

    Args:
        client: An initialized Spotify client instance.
        processor: An initialized data processor instance.

    Returns:
        DataFrame containing processed liked songs data.
    """
    logger.debug("Fetching liked songs...")
    liked_songs = client.get_users_liked_songs()
    return processor.process_tracks_to_dataframe(
        raw_tracks_data=liked_songs, source_playlist="Liked Songs"
    )


def _fetch_playlist_data(
    client: SpotipyClient, processor: SpotifyApiDataProcessor
) -> pd.DataFrame:
    """Fetch and process all user playlist tracks.

    Args:
        client: An initialized Spotify client instance.
        processor: An initialized data processor instance.

    Returns:
        DataFrame containing all playlist tracks.
    """
    logger.debug("Fetching user playlists...")
    user_playlists = client.get_users_playlists()

    if not user_playlists:
        return pd.DataFrame()

    playlist_track_dfs = []

    for playlist in user_playlists:
        playlist_id = playlist["id"]
        playlist_name = playlist["name"]

        logger.debug(f"Fetching tracks for playlist: '{playlist_name}'")

        playlist_tracks = client.get_playlist_tracks(playlist_id=playlist_id)
        playlist_tracks_df = processor.process_tracks_to_dataframe(
            raw_tracks_data=playlist_tracks,
            source_playlist=playlist_name,
        )
        playlist_track_dfs.append(playlist_tracks_df)

    return (
        pd.concat(playlist_track_dfs, ignore_index=True)
        if playlist_track_dfs
        else pd.DataFrame()
    )


def _get_saved_track_ids(
    liked_songs_df: pd.DataFrame, playlist_df: pd.DataFrame
) -> Set[str]:
    """Extract all saved track IDs from liked songs and playlists.

    Args:
        liked_songs_df: DataFrame containing liked songs.
        playlist_df: DataFrame containing playlist tracks.

    Returns:
        Set of all saved track IDs.
    """
    liked_track_ids = (
        set(liked_songs_df["track_id"]) if not liked_songs_df.empty else set()
    )
    playlist_track_ids = (
        set(playlist_df["track_id"]) if not playlist_df.empty else set()
    )

    return liked_track_ids | playlist_track_ids


def _fetch_unsaved_tracks_data(
    client: SpotipyClient,
    processor: SpotifyApiDataProcessor,
    streaming_history_df: pd.DataFrame,
    saved_track_ids: Set[str],
) -> pd.DataFrame:
    """Identify and fetch metadata for unsaved streamed tracks.

    Args:
        client: An initialized Spotify client instance.
        processor: An initialized data processor instance.
        streaming_history_df: DataFrame containing streaming history.
        saved_track_ids: Set of already saved track IDs.

    Returns:
        DataFrame containing unsaved track metadata.
    """
    streamed_track_ids = set(streaming_history_df["track_id"])
    unsaved_track_ids = list(streamed_track_ids.difference(saved_track_ids))

    logger.info(
        f"Found {len(unsaved_track_ids)} unsaved tracks in "
        "streaming history to fetch."
    )

    if not unsaved_track_ids:
        return pd.DataFrame()

    unsaved_track_data = client.get_track_info_in_batches(unsaved_track_ids)
    return processor.process_tracks_to_dataframe(unsaved_track_data)


def get_unified_spotify_track_data(
    client: SpotipyClient,
    processor: SpotifyApiDataProcessor,
    streaming_history_df: pd.DataFrame,
) -> pd.DataFrame:
    """Orchestrates the fetching and processing of all relevant track metadata
    from Spotify.

    This function performs the following steps:
    1. Fetches the user's liked songs.
    2. Fetches all user playlists and the tracks within them.
    3. Identifies tracks from the streaming history that are not in liked
        songs or playlists.
    4. Fetches metadata for these "unsaved" streamed tracks.
    5. Processes and aggregates all three sources into a single,
        de-duplicated DataFrame.

    Args:
        client (SpotipyClient): An initialized Spotify client instance.
        processor (SpotifyDataProcessor): An initialized data processor
        instance.
        streaming_history_df (pd.DataFrame):
            A DataFrame containing the user's
            processed streaming history, which must
            include a 'track_id' column.

    Returns:
        pd.DataFrame:
            A single, aggregated DataFrame containing detailed information
            for all unique tracks from the user's library and history.
            Returns an empty DataFrame if no data can be fetched or processed.
    """
    logger.info("Starting to gather track data from Spotify API...")

    # --- 1. Get Liked Songs ---
    liked_songs_df = _fetch_liked_songs_data(
        client=client, processor=processor
    )

    # --- 2. Get All Playlist Songs ---
    playlist_df = _fetch_playlist_data(client=client, processor=processor)

    # --- 3. Get Unsaved Streamed Songs ---
    # Get saved IDs for all songs
    saved_track_ids = _get_saved_track_ids(liked_songs_df, playlist_df)

    # Get the unsaved songs
    unsaved_track_df = _fetch_unsaved_tracks_data(
        client=client,
        processor=processor,
        saved_track_ids=saved_track_ids,
        streaming_history_df=streaming_history_df,
    )

    # --- 4. Aggregate All Data Sources ---
    logger.info(
        "Aggregating all song data sources (liked, playlists, unsaved)..."
    )
    final_api_df = processor.aggregate_track_dataframes(
        list_of_track_dfs=[
            liked_songs_df,
            playlist_df,
            unsaved_track_df,
        ]
    )

    logger.info(
        f"Gathered and aggregated info for {len(final_api_df)} "
        "unique tracks from API."
    )
    return final_api_df
