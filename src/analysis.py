"""Functions for analyzing Spotify streaming data and deriving insights.

... (your module docstring) ...
"""

import logging
import math
from typing import Set

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

logger = logging.getLogger(__name__)

# --- Constants for Scoring Logic ---
# These constants are well-defined and perfect as they are.
POSITIVE_END_REASONS: Set[str] = {"trackdone"}
NEGATIVE_END_REASONS: Set[str] = {"fwdbtn", "backbtn"}
POSITIVE_START_REASONS: Set[str] = {"clickrow", "playbtn", "backbtn"}
TRACK_RESTART_REASON: str = "backbtn"

HIGH_FRACTION_PLAYED_THRESHOLD: float = 0.85

SCORE_VERY_POSITIVE: float = 1.5
SCORE_POSITIVE: float = 1.0
SCORE_NEGATIVE: float = -1.0
SCORE_NEGATIVE_HIGH_FRACTION_PLAYED: float = -0.5
SCORE_NEUTRAL: float = 0.0

WEIGHT_FRACTION_PLAYED: float = 1.0
WEIGHT_REASON_START: float = 1.0
WEIGHT_REASON_END: float = 1.0
WEIGHT_SKIPPED: float = 1.0
WEIGHT_SAVED: float = 1.0


def explode_long_streams(df: pd.DataFrame) -> pd.DataFrame:
    """Expands streaming entries that represent multiple consecutive listens.

    Any stream where 'fraction_played' > 1.0 is "exploded" into multiple
    rows: one row for each full listen (fraction_played = 1.0) and a final
    row for the remaining partial listen. Timestamps and reasons are adjusted
    accordingly.

    Args:
        df (pd.DataFrame):
            A DataFrame of processed streams, containing at least
            'fraction_played', 'ms_played', 'track_duration_ms', 'streamed_at',
            'reason_start', and 'reason_end'.

    Returns:
        pd.DataFrame: A DataFrame where long streams have been expanded into
                      individual listen events, ready for enjoyment scoring.
    """

    single_listens = df[df.fraction_played <= 1].copy()
    multi_listens = df[df.fraction_played > 1].copy()

    # For each row we want to take the veiling of the current fraction
    # to get the total number of listens
    multi_listens["stream_count"] = multi_listens.fraction_played.apply(
        math.ceil
    )
    # Duplicate
    new_rows = []
    for _, row in multi_listens.iterrows():
        for stream in range(row.stream_count):
            new_row = row.copy()
            # If the last stream
            if stream == row.stream_count - 1:
                # Played for remainder
                new_row["fraction_played"] = row.fraction_played % 1
                new_row["ms_played"] = new_row["track_duration_ms"] * (
                    row.fraction_played % 1
                )
                new_row["reason_start"] = "trackdone"
            else:
                new_row["fraction_played"] = 1.0
                new_row["ms_played"] = new_row["track_duration_ms"]
                # Change start resaon if not first stream
                if stream > 0:
                    new_row["reason_start"] = "trackdone"
                new_row["reason_end"] = "trackdone"
            # Start time is now based on what listen we are in the sequence
            new_row["streamed_at"] = new_row["streamed_at"] + pd.Timedelta(
                milliseconds=stream * new_row["track_duration_ms"]
            )
            new_rows.append(new_row)

    # Sense Check
    assert len(new_rows) == multi_listens["stream_count"].sum()

    return pd.concat(
        [single_listens, pd.DataFrame(new_rows)[single_listens.columns]]
    )


def calculate_enjoyment_scores(streaming_df: pd.DataFrame) -> pd.DataFrame:
    """Calculates and appends enjoyment scores to a streaming history
    DataFrame.

    This function enriches the DataFrame with several new columns:
    - `start_score`: A score based on how a track started.
    - `end_score`: A score based on how a track ended.
    - `enjoyment_score`: A final, weighted score for each listening stream,
      representing an estimated level of user enjoyment for that specific
      listen.

    The calculations are performed using efficient, vectorized operations.

    Args:
        streaming_df (pd.DataFrame): The processed streaming history DataFrame.
            It must contain the columns: 'reason_start', 'reason_end',
            'fraction_played', 'skipped', and 'is_saved'.

    Returns:
        pd.DataFrame: The input DataFrame with the new score columns appended.
    """
    logger.info("Calculating enjoyment scores for streaming data...")
    df = streaming_df.copy()

    # --- Step 1: Evaluate the fraction of the full song being listened to ---
    df["fraction_played"] = df.ms_played / df.track_duration_ms

    df.loc[
        df["ms_played"].apply(lambda x: x == 0 or pd.isnull(x)),
        "fraction_played",
    ] = 0
    # 1. Replace infinite values (from division by zero) with NaN.
    df["fraction_played"] = df["fraction_played"].replace(
        [np.inf, -np.inf], np.nan
    )

    # 2. Deal with streams that exceed the duration of a song i.e.
    # Single rows with multiple streams
    df = explode_long_streams(df)

    # --- Step 2: Calculate Start Score ---
    start_conditions = [
        df["reason_start"] == TRACK_RESTART_REASON,
        df["reason_start"].isin(POSITIVE_START_REASONS),
    ]
    start_choices = [SCORE_VERY_POSITIVE, SCORE_POSITIVE]
    df["start_score"] = np.select(
        start_conditions, start_choices, default=SCORE_NEUTRAL
    )
    logger.debug("Calculated 'start_score' column.")

    # --- Step 3: Calculate End Score ---
    end_conditions = [
        # Positive case: track finished naturally.
        df["reason_end"].isin(POSITIVE_END_REASONS),
        # Negative case 1: Skipped, but after listening to most of it.
        (
            df["reason_end"].isin(NEGATIVE_END_REASONS)
            & (df["reason_end"] != TRACK_RESTART_REASON)
            & (df["fraction_played"] > HIGH_FRACTION_PLAYED_THRESHOLD)
        ),
        # Negative case 2: Any other skip.
        df["reason_end"].isin(NEGATIVE_END_REASONS),
    ]
    end_choices = [
        SCORE_POSITIVE,
        SCORE_NEGATIVE_HIGH_FRACTION_PLAYED,
        SCORE_NEGATIVE,
    ]
    # Note: The order of conditions matters. np.select uses the first True
    # condition.
    df["end_score"] = np.select(
        end_conditions, end_choices, default=SCORE_NEUTRAL
    )
    logger.debug("Calculated 'end_score' column.")

    # --- Step 4: Evaluate if the song has been saved by the user in any
    # playlists to date:
    df["is_saved"] = df.playlists.apply(lambda x: len(x) > 0)

    # --- Step 5: Calculate Final Enjoyment Score (Vectorized) ---
    # Ensure boolean columns are treated as 0s and 1s for the calculation.
    # Note: 'skipped' is a penalty, so we multiply by -1.
    df["enjoyment_score"] = (
        (df["fraction_played"] * WEIGHT_FRACTION_PLAYED)
        + (df["start_score"] * WEIGHT_REASON_START)
        + (df["end_score"] * WEIGHT_REASON_END)
        - (df["skipped"].astype(int) * WEIGHT_SKIPPED)
        + (df["is_saved"].astype(int) * WEIGHT_SAVED)
    )
    logger.debug("Calculated final 'enjoyment_score' column.")

    logger.info(
        "Successfully calculated and appended enjoyment "
        "scores to the DataFrame."
    )
    return df


def normalise_scores(track_stream_scores: pd.Series) -> pd.Series:
    """Normalise the scoring using clipping and min/max scaling to achieve a
    more uniform distribution of scores between 0 and 1.

    Args:
        track_stream_scores:
            Pandas Series covering the enjoyment scores across all streams

    Returns:
        Pandas Series of normalised scored
    """

    # Lower bound of the bottom 1%
    lower_bound = track_stream_scores.quantile(0.01)
    # Upper Bound of the top 99%
    upper_bound = track_stream_scores.quantile(0.99)

    # Clip the series
    clipped_scores = track_stream_scores.clip(
        upper=upper_bound, lower=lower_bound
    )

    # Define out scaler
    scaler = MinMaxScaler(feature_range=(0, 1))
    # Return the scaled series
    return scaler.fit_transform(clipped_scores.values.reshape(-1, 1))


def summarize_track_enjoyment(
    scored_df: pd.DataFrame, k: int = 5
) -> pd.DataFrame:
    """Aggregates stream-level data to the track level and calculates an
    adjusted score.

    This function groups the detailed streaming data by track,
    calculating summary statistics like total play count and average
    enjoyment. It then computes a Bayesian-adjusted mean enjoyment score,
    which moderates the scores of tracks with very few plays, pulling
    them closer to the global average. This provides
    a more stable and reliable ranking of top tracks.

    Args:
        scored_df (pd.DataFrame): The enriched DataFrame from
            `enrich_with_analytical_metrics`, containing stream-level data and
            normalized enjoyment scores.
        k (int, optional): The Bayesian shrinkage parameter, representing the
            "prior strength" or the number of "ghost plays" at the global
            average score. Higher values cause scores of low-play-count tracks
            to be pulled more strongly toward the global mean. Defaults to 5.

    Returns:
        pd.DataFrame:
            A DataFrame aggregated by track, with columns for play count,
            first/last listen times, and the Bayesian-adjusted mean
            enjoyment score. Returns an empty DataFrame if the input
            is empty.
    """
    if scored_df.empty:
        logger.info(
            "Input DataFrame is empty. Cannot summarize track enjoyment."
        )
        return pd.DataFrame()

    required_cols = [
        "track_name",
        "album_artist",
        "enjoyment_score_norm",
        "streamed_at",
    ]
    if not all(col in scored_df.columns for col in required_cols):
        logger.error(
            "Input DataFrame is missing one or more "
            "required columns for summarization."
        )
        return pd.DataFrame()

    logger.info(
        f"Aggregating track enjoyment data with Bayesian adjustment (k={k})..."
    )

    # Calculate the global mean score across all streams, used for
    # the adjustment
    global_mean_score = scored_df["enjoyment_score_norm"].mean()
    logger.debug(
        f"Global mean normalized enjoyment score: {global_mean_score:.4f}"
    )

    # Group by track and perform initial aggregations
    grouped_df = (
        scored_df.groupby(["track_name", "album_artist"])
        .agg(
            mean_enjoyment_score=("enjoyment_score_norm", "mean"),
            play_count=("streamed_at", "count"),
            first_listen=("streamed_at", "min"),
            last_listen=("streamed_at", "max"),
            album_artwork_url=("album_artwork_url", "first"),
        )
        .reset_index()
    )

    # Calculate the Bayesian-adjusted mean
    # The formula is:
    # ((mean_score * play_count) + (k * global_mean)) / ( play_count + k )
    numerator = (
        grouped_df["mean_enjoyment_score"] * grouped_df["play_count"]
    ) + (k * global_mean_score)
    denominator = grouped_df["play_count"] + k

    grouped_df["adjusted_enjoyment_score"] = numerator / denominator

    logger.info(
        f"Successfully aggregated {len(scored_df)} streams into "
        f"{len(grouped_df)} unique tracks."
    )

    return grouped_df
