"""Data processing and cleaning utilities for Spotify streaming history.

This module is responsible for transforming raw Spotify streaming data into a
clean, structured, and analysis-ready format
"""

import logging
from typing import Dict, List

import numpy as np
import pandas as pd

# Logger
logger = logging.getLogger(__name__)

# --- Column Names (Input from raw data) ---
# These are the expected column names in the raw Spotify JSON data
RAW_TS_COL = "ts"
RAW_TRACK_NAME_COL = "master_metadata_track_name"
RAW_ALBUM_NAME_COL = "master_metadata_album_album_name"
RAW_ARTIST_NAME_COL = "master_metadata_album_artist_name"
RAW_SPOTIFY_URI_COL = "spotify_track_uri"
RAW_MS_PLAYED_COL = "ms_played"
RAW_IP_ADDR_COL = "ip_addr"
RAW_REASON_START_COL = "reason_start"
RAW_REASON_END_COL = "reason_end"
RAW_SHUFFLE_COL = "shuffle"
RAW_SKIPPED_COL = "skipped"
RAW_CONN_COUNTRY_COL = "conn_country"


# --- Column Names (Output / Renamed) ---
# These are the desired column names after processing
PROCESSED_STREAMED_AT_COL = "streamed_at"
PROCESSED_TRACK_NAME_COL = "track_name"
PROCESSED_ALBUM_NAME_COL = "album_name"
PROCESSED_ALBUM_ARTIST_COL = "album_artist"
PROCESSED_TRACK_ID_COL = "track_id"
PROCESSED_MS_PLAYED_COL = "ms_played"
PROCESSED_IP_ADDR_COL = "ip_addr"
PROCESSED_REASON_START_COL = "reason_start"
PROCESSED_REASON_END_COL = "reason_end"
PROCESSED_SHUFFLE_COL = "shuffle"
PROCESSED_SKIPPED_COL = "skipped"
PROCESSED_FULL_TRACK_NAME_COL = "full_track_name"
PROCESSED_CONN_COUNTRY_COL = "conn_country"

# --- Mapping for renaming ---
COLUMN_RENAME_MAP: Dict[str, str] = {
    RAW_TS_COL: PROCESSED_STREAMED_AT_COL,
    RAW_TRACK_NAME_COL: PROCESSED_TRACK_NAME_COL,
    RAW_ALBUM_NAME_COL: PROCESSED_ALBUM_NAME_COL,
    RAW_ARTIST_NAME_COL: PROCESSED_ALBUM_ARTIST_COL,
}

# --- Desired Final Column Order ---
FINAL_COLUMNS_ORDER: List[str] = [
    PROCESSED_STREAMED_AT_COL,
    PROCESSED_TRACK_NAME_COL,
    PROCESSED_ALBUM_NAME_COL,
    PROCESSED_ALBUM_ARTIST_COL,
    PROCESSED_TRACK_ID_COL,
    PROCESSED_MS_PLAYED_COL,
    PROCESSED_IP_ADDR_COL,
    PROCESSED_REASON_START_COL,
    PROCESSED_REASON_END_COL,
    PROCESSED_SHUFFLE_COL,
    PROCESSED_SKIPPED_COL,
    PROCESSED_FULL_TRACK_NAME_COL,
    PROCESSED_CONN_COUNTRY_COL,
]


def _create_full_track_name_column(df: pd.DataFrame) -> pd.DataFrame:
    """Creates a 'full_track_name' column by combining artist and track name.
    Handles cases where source columns are missing or not strings.

    This function attempts to replicate the original notebook's behavior
    of returning NaN for 'full_track_name' if either
    'master_metadata_track_name' or 'master_metadata_album_artist_name'
    is missing or not a string.

    Args:
        df: The input DataFrame containing raw Spotify streaming data.
            Expected columns: `RAW_TRACK_NAME_COL` and `RAW_ARTIST_NAME_COL`.

    Returns:
        A DataFrame with the new `PROCESSED_FULL_TRACK_NAME_COL` column.
    """
    logger.debug("Creating '%s' column.", PROCESSED_FULL_TRACK_NAME_COL)

    # Condition to identify rows where both track
    # name and artist are valid strings
    valid_names_condition = (
        df[RAW_TRACK_NAME_COL].notna()
        & df[RAW_ARTIST_NAME_COL].notna()
        & df[RAW_TRACK_NAME_COL].apply(lambda x: isinstance(x, str))
        & df[RAW_ARTIST_NAME_COL].apply(lambda x: isinstance(x, str))
    )

    # Create the 'full_track_name' column
    df[PROCESSED_FULL_TRACK_NAME_COL] = np.where(
        valid_names_condition,
        df[RAW_ARTIST_NAME_COL].astype(str)
        + " - "
        + df[RAW_TRACK_NAME_COL].astype(str),
        np.nan,  # If condition is False, set to NaN
    )
    return df


def _filter_non_music_streams(df: pd.DataFrame) -> pd.DataFrame:
    """Filters out entries identified as non-music streams (e.g., podcasts,
    audiobooks). This includes rows where `PROCESSED_FULL_TRACK_NAME_COL` is
    NaN.

    Args:
        df: The DataFrame with a `PROCESSED_FULL_TRACK_NAME_COL` column.

    Returns:
        A DataFrame containing only music streams.
    """
    logger.debug("Filtering non-music streams and null track names.")

    initial_count = len(df)
    filtered_df = df.copy()

    # Filter out rows where full_track_name is NaN
    if PROCESSED_FULL_TRACK_NAME_COL in filtered_df.columns:
        filtered_df = filtered_df[
            filtered_df[PROCESSED_FULL_TRACK_NAME_COL].notna()
        ]
        logger.debug(
            "Removed %d rows with null '%s'.",
            initial_count - len(filtered_df),
            PROCESSED_FULL_TRACK_NAME_COL,
        )
        initial_count = len(filtered_df)  # Update count after first filter

    else:
        logger.warning(
            "Column '%s' not found for filtering. "
            "Skipping non-music filtering.",
            PROCESSED_FULL_TRACK_NAME_COL,
        )

    logger.info("Filtered data. Remaining records: %d.", len(filtered_df))
    return filtered_df


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renames columns in the DataFrame according to the `COLUMN_RENAME_MAP`.

    Args:
        df: The input DataFrame.

    Returns:
        A DataFrame with renamed columns.
    """
    logger.debug("Renaming columns.")
    # Check if columns to be renamed exist before renaming
    cols_to_rename = {
        k: v for k, v in COLUMN_RENAME_MAP.items() if k in df.columns
    }
    renamed_df = df.rename(columns=cols_to_rename)
    logger.debug("Columns renamed: %s", cols_to_rename)
    return renamed_df.copy()


def _extract_track_id(df: pd.DataFrame) -> pd.DataFrame:
    """Extracts the Spotify track ID from `RAW_SPOTIFY_URI_COL` and creates a
    new `PROCESSED_TRACK_ID_COL` column. Handles missing URIs gracefully.

    Args:
        df: The DataFrame containing a `RAW_SPOTIFY_URI_COL` column.

    Returns:
        A DataFrame with the new `PROCESSED_TRACK_ID_COL` column.
    """
    logger.debug("Extracting track IDs from URIs.")

    # Extract the song ID from the raw field
    df[PROCESSED_TRACK_ID_COL] = (
        df[RAW_SPOTIFY_URI_COL].astype(str).str.split(":").str[-1]
    )

    # Set 'track_id' to NaN if the original URI was not a string or was empty
    df.loc[
        ~df[RAW_SPOTIFY_URI_COL].apply(
            lambda x: isinstance(x, str) and bool(x.strip())
        ),
        PROCESSED_TRACK_ID_COL,
    ] = np.nan

    return df


def _select_and_reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Selects a predefined set of columns (`FINAL_COLUMNS_ORDER`) and reorders
    them. Missing expected columns will be logged as warnings and omitted from
    the final DataFrame.

    Args:
        df: The DataFrame with columns to select and reorder.

    Returns:
        A DataFrame with selected and reordered columns.
    """
    logger.debug("Selecting and reordering final columns.")
    # Ensure all required columns exist before attempting to select
    existing_cols = [col for col in FINAL_COLUMNS_ORDER if col in df.columns]
    missing_cols = set(FINAL_COLUMNS_ORDER) - set(existing_cols)
    if missing_cols:
        logger.warning(
            "Missing expected columns in DataFrame for final selection: %s. "
            "They will not be included.",
            missing_cols,
        )
    return df[
        existing_cols
    ].copy()  # Return a copy to avoid SettingWithCopyWarning


def clean_and_prepare_streaming_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Processes raw Spotify streaming history data by cleaning, filtering,
    renaming columns, and extracting derived information.

    This function orchestrates a series of transformations:
    1. Creates a combined 'full_track_name' from artist and track title.
    2. Filters out non-music streams (based on 'full_track_name'
    nulls and keywords).
    3. Renames columns to a standardized, more readable format.
    4. Extracts 'track_id' from Spotify URIs.
    5. Selects and reorders the final set of desired columns.

    Args:
        raw_df: A Pandas DataFrame containing the raw Spotify
                streaming history data, typically loaded directly
                from JSON files.

    Returns:
        A new Pandas DataFrame containing the cleaned and prepared
        streaming data, ready for further analysis. Returns an empty
        DataFrame if the input DataFrame is empty or all data is
        filtered out.
    """
    if raw_df.empty:
        logger.warning(
            "Input DataFrame is empty, returning an empty DataFrame."
        )
        return pd.DataFrame()

    logger.info(
        "Starting data cleaning and preparation process. Initial records: %d.",
        len(raw_df),
    )

    # Use .pipe() for chaining operations for better readability
    processed_df = (
        raw_df.pipe(_create_full_track_name_column)
        .pipe(_filter_non_music_streams)
        .pipe(_rename_columns)
        .pipe(_extract_track_id)
        .pipe(_select_and_reorder_columns)
    )

    logger.info(
        "Data cleaning and preparation complete. Final records: %d.",
        len(processed_df),
    )

    processed_df.streamed_at = pd.to_datetime(processed_df.streamed_at)
    return processed_df
