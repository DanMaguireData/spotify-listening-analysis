"""This module contains high-level pipeline functions that orchestrate the
fetching and processing of Spotify data."""

import logging

import pandas as pd

# Assuming your classes are in these modules. Adjust paths if necessary.
from .spotify_api_client import SpotipyClient
from .spotify_api_data_processor import SpotifyApiDataProcessor

logger = logging.getLogger(__name__)


# TODO: Reduce function complexity
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
    liked_songs = client.get_users_liked_songs()
    liked_songs_df = processor.process_tracks_to_dataframe(
        raw_tracks_data=liked_songs, source_playlist="Liked Songs"
    )

    # --- 2. Get All Playlist Songs ---
    user_playlists = client.get_users_playlists()
    playlist_names_and_ids = [
        {"id": playlist["id"], "name": playlist["name"]}
        for playlist in user_playlists
    ]

    playlist_track_dfs = []
    for playlist_info in playlist_names_and_ids:
        logger.debug(
            f"Fetching tracks for playlist: '{playlist_info['name']}'"
        )
        playlist_tracks = client.get_playlist_tracks(
            playlist_id=playlist_info["id"]
        )
        playlist_tracks_df = processor.process_tracks_to_dataframe(
            raw_tracks_data=playlist_tracks,
            source_playlist=playlist_info["name"],
        )
        playlist_track_dfs.append(playlist_tracks_df)

    # Concatenate all playlist DFs if the list is not empty
    if playlist_track_dfs:
        all_playlist_df = pd.concat(playlist_track_dfs, ignore_index=True)
    else:
        all_playlist_df = pd.DataFrame()

    # --- 3. Get Unsaved Streamed Songs ---
    # Combine liked and playlist track IDs to find all "saved" tracks
    liked_track_ids = (
        set(liked_songs_df["track_id"]) if not liked_songs_df.empty else set()
    )
    playlist_track_ids = (
        set(all_playlist_df["track_id"])
        if not all_playlist_df.empty
        else set()
    )

    # Cleaner way to combine sets
    saved_track_ids = liked_track_ids | playlist_track_ids

    streamed_track_ids = set(streaming_history_df["track_id"])
    unsaved_track_ids = list(streamed_track_ids.difference(saved_track_ids))

    logger.info(
        f"Found {len(unsaved_track_ids)} unsaved tracks in "
        "streaming history to fetch."
    )

    if unsaved_track_ids:
        unsaved_track_data = client.get_track_info_in_batches(
            unsaved_track_ids
        )
        unsaved_track_df = processor.process_tracks_to_dataframe(
            unsaved_track_data
        )
    else:
        unsaved_track_df = pd.DataFrame()

    # --- 4. Aggregate All Data Sources ---
    logger.info(
        "Aggregating all song data sources (liked, playlists, unsaved)..."
    )
    final_api_df = processor.aggregate_track_dataframes(
        list_of_track_dfs=[
            liked_songs_df,
            all_playlist_df,
            unsaved_track_df,
        ]
    )

    logger.info(
        f"Gathered and aggregated info for {len(final_api_df)} "
        "unique tracks from API."
    )
    return final_api_df
