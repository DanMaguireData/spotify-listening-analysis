"""
Main entry point and orchestration script for the Spotify analysis.

This script manages the end-to-end workflow for analyzing personal Spotify
extended streaming history data. It handles command-line argument parsing,
initial setup (like logging and data directory validation), and orchestrates
the data loading, processing, and analysis phases.

To run the analysis, ensure your Spotify streaming history JSON files are
placed in the 'data/' directory
"""

import logging
import sys

from src.file_io import list_streaming_files, load_files_into_dataframe

# Setup Logger
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG for more verbose output
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)  # Logger for this specific script


def main() -> None:
    """
    Runs analysis
    """
    logger.info("Starting Spotify data analysis script.")

    # Step 1: Check files exist
    logger.info("Scanning for streaming data...")
    streaming_files = list_streaming_files()

    # If no files are present warn user and exit
    if not streaming_files:
        logger.error("No valid streaming history files found.")
        logger.error("Process terminating due to no data")
        sys.exit(1)  # Exit with error

    logger.info(f"Found the following files: {streaming_files}")

    # Step 2: Load files into a DataFrame
    df = load_files_into_dataframe(streaming_files)
    print(df.loc[0])


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Unexpected error occured: {e}", exc_info=True)
        sys.exit(1)
