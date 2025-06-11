"""Functions for analyzing Spotify streaming data and deriving insights.

... (your module docstring) ...
"""

import logging
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

    # --- Step 1: Calculate Start Score (Vectorized) ---
    start_conditions = [
        df["reason_start"] == TRACK_RESTART_REASON,
        df["reason_start"].isin(POSITIVE_START_REASONS),
    ]
    start_choices = [SCORE_VERY_POSITIVE, SCORE_POSITIVE]
    df["start_score"] = np.select(
        start_conditions, start_choices, default=SCORE_NEUTRAL
    )
    logger.debug("Calculated 'start_score' column.")

    # --- Step 2: Calculate End Score (Vectorized) ---
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

    # --- Step 3: Evaluate if the song has been saved by the user in any
    # playlists to date:
    df["is_saved"] = df.playlists.apply(lambda x: len(x) > 0)

    # --- Step 4: Calculate Final Enjoyment Score (Vectorized) ---
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
