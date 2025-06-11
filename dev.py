"""Temporary script being used to develop out functionality."""

import logging

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from src.analysis import calculate_enjoyment_scores, normalise_scores

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
    df = pd.read_csv("data/merged.csv")

    df["fraction_played"] = df.ms_played / df.track_duration_ms

    df.loc[
        df["ms_played"].apply(lambda x: x == 0 or pd.isnull(x)),
        "fraction_played",
    ] = np.nan

    # TODO: How to handle if track played in excess of 100% of the runtime?
    # TODO: How to handle if track has no duration?
    track = df.loc[21]
    print(track)
    df = calculate_enjoyment_scores(df)
    df["enjoyment_score_norm"] = normalise_scores(df["enjoyment_score"])
    df.to_csv("data/scored.csv")


if __name__ == "__main__":
    main()
