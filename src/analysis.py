"""
Functions for analyzing Spotify streaming data and deriving insights.

This module contains the core analytical logic for the Spotify Deep
Dive project. It operates on cleaned and processed streaming history
data, focusing on calculating custom metrics, identifying listening
patterns, and generating meaningful insights.

Key functionalities include:
  - Calculating custom enjoyment scores for individual tracks.
  - Identifying top-listened tracks, artists, and albums.

The functions within this module are designed to take a Pandas DataFrame
(typically prepared by `src.data_processor`) as input and return either
modified DataFrames with new analytical columns or aggregated summary
DataFrames.
"""

from typing import Set, Union

import numpy as np

# --- Constants for End Reason Scores ---
POSITIVE_END_REASONS: Set[str] = {"trackdone", "endplay"}
NEGATIVE_END_REASONS = {"fwdbtn", "backbtn", "nextbtn", "clickrow"}

# --- Constants for Start Reason Score ---
POSITIVE_START_REASONS = {"clickrow", "backbtn", "playbtn"}

# Threshold for fractional play to reduce penalty
HIGH_FRACTION_PLAYED_THRESHOLD = 0.85

# Score values
SCORE_VERY_POSITIVE = 1.5
SCORE_POSITIVE = 1.0
SCORE_NEGATIVE = -1.0
SCORE_NEGATIVE_HIGH_FRACTION_PLAYED = -0.5
SCORE_NEUTRAL = 0.0


def _calculate_end_reason_score_single(
    reason_end: Union[str, float],  # float to account for np.nan
    fraction_played: float,  # np.number for numpy's float types
) -> float:
    """
    Calculates a score based on the reason a track ended and the fraction
    played.

    This is a helper function designed for row-wise application
    (e.g., using df.apply).
    It encapsulates the logic for a single stream entry.

    Args:
        reason_end: The reason the track ended (e.g., "trackdone", "fwdbtn").
                    Can be NaN if missing.
        fraction_played: The proportion of the track that was played
            (0.0 to 1.0). Can be NaN if missing.

    Returns:
        A float representing the score:
        - `SCORE_POSITIVE` (1.0) if the reason is in `POSITIVE_END_REASONS`.
        - `SCORE_NEGATIVE_HIGH_FRACTION_PLAYED` (-0.5) if in
            `NEGATIVE_END_REASONS` (excluding "backbtn") and
            `fraction_played` is above
          `HIGH_FRACTION_PLAYED_THRESHOLD`.
        - `SCORE_NEGATIVE` (-1.0) if in `NEGATIVE_END_REASONS` (or "backbtn").
        - `SCORE_NEUTRAL` (0.0) for any other reason or if inputs are
            invalid/missing.
    """
    # Handle missing/invalid inputs first
    if not isinstance(reason_end, str):
        # A non-string reason_end (e.g., NaN) implies an unknown or
        # neutral outcome.
        return SCORE_NEUTRAL

    # Ensure fraction_played is a valid number, default to 0 for invalid/NaN
    if not isinstance(fraction_played, (float, np.number)) or np.isnan(
        fraction_played
    ):
        fraction_played = 0.0

    reason_end_lower = (
        reason_end.lower()
    )  # Normalize to lowercase for robust comparison

    if reason_end_lower in POSITIVE_END_REASONS:
        return SCORE_POSITIVE
    if reason_end_lower in NEGATIVE_END_REASONS:
        if reason_end_lower != "backbtn":
            # If the user has almost finished the song, lower penalty
            if fraction_played > HIGH_FRACTION_PLAYED_THRESHOLD:
                # TODO: Possibly refine this so non constant
                return SCORE_NEGATIVE_HIGH_FRACTION_PLAYED
        return SCORE_NEGATIVE
    # Default for unknown reasons or neutral ones like 'revbtn'
    # You could extend POSITIVE_END_REASONS, NEGATIVE_END_REASONS,
    # NEUTRAL_END_REASONS to ensure all known reasons are explicitly
    # categorized.
    return SCORE_NEUTRAL


def _calculate_start_reason_score_single(
    reason_start: Union[str, float],  # float to account for np.nan
) -> float:
    """
    Calculates a score based on the reason a track started.

    This is a helper function designed for row-wise application
    (e.g., using df.apply).
    It encapsulates the logic for a single stream entry.

    Args:
        reason_end: The reason the track ended (e.g., "clickrow").
                    Can be NaN if missing.

    Returns:
        A float representing the score:
        - `SCORE_VERY_POSITIVE` if the reason is user restarting the song.
        - `SCORE_POSITIVE` if any other positive reason
        - `SCORE_NEUTRAL` (0.0) for any other reason or if inputs are
            invalid/missing.
    """

    if not isinstance(reason_start, str):
        return SCORE_NEUTRAL

    if reason_start in POSITIVE_START_REASONS:
        if reason_start == "backbtn":
            # User is restarting a song, very positive indicator
            return SCORE_VERY_POSITIVE
        return SCORE_POSITIVE
    # Default to negative for all other reasons or if reason is null
    return SCORE_NEUTRAL
