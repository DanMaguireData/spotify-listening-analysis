"""
File I/O operations for the Spotify data analysis project.

This module handles tasks related to finding, validating, and loading
Spotify streaming history files from the local filesystem.
"""

import json
import logging
import os
from typing import List, Optional

import pandas as pd

DATA_DIR = "./data/"
EXPECTED_FILE_PREFIX = "Streaming_History_"
EXPECXED_FILE_TYPE = ".json"


def check_filename_valid(filename: str) -> bool:
    """
    Validates if the given filename conforms to the expected Spotify
    extended streaming history file format.

    The function checks for two conditions:
    1. The filename must start with "Streaming_History_".
    2. The filename must end with ".json".

    Args:
        filename: The filename string to validate.

    Returns:
        True if the filename meets both criteria, False otherwise.
        Returns False if the input is not a string or is an empty string.
    """

    # Run checks, both conditions should be met
    starts_with_prefix = filename.startswith(EXPECTED_FILE_PREFIX)
    ends_with_extension = filename.endswith(EXPECXED_FILE_TYPE)

    return starts_with_prefix and ends_with_extension


def list_streaming_files() -> List[str]:
    """
    Lists files available in directory, running checks to ensure they
    conform to the expected file format

    Args:
        None

    Returns:
        List of files that conform to expected naming convetion
        Empty List if no files found
    """
    file_list = list(os.listdir(DATA_DIR))
    valid_files = [file for file in file_list if check_filename_valid(file)]
    if not valid_files:
        logging.info(
            "No valid streaming history files (e.g.,"
            f"{EXPECTED_FILE_PREFIX}*.{EXPECXED_FILE_TYPE})"
            f" found in '{DATA_DIR}'."
        )
    else:
        logging.info(f"Found {len(valid_files)} files in {DATA_DIR}")
    # Add in the data directory to each filename and return list]
    return [os.path.join(DATA_DIR, file) for file in valid_files]


def load_file_contents_into_dataframe(
    file_path: str,
) -> Optional[pd.DataFrame]:
    """
    Loads contents of the specified streaming file into
    a pandas DataFrame for downstream analysis

    Args:
        file_path: String dentoing the path to the file that we will load

    Returns:
        Contents of the file loaded into a DataFrame
        None if error occurs during loading
    """
    # Get contents from JSON file
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            file_content = json.load(file)
    # If no file throw Error
    except FileNotFoundError:
        logging.error(f"File not found at {file_path}")
        return None
    # Catch Other issues
    except Exception as e:
        logging.error(f"Error occurred reading {file_path}: {e}")
        return None

    # Load into DataFrame
    file_df = pd.DataFrame(file_content)
    logging.info(f"Loaded {len(file_df)} records from {file_path}")
    return file_df


def load_files_into_dataframe(file_paths: List[str]) -> pd.DataFrame:
    """
    Loads the contents of multiple streaming files into a single DataFrame

    Args:
        file_paths: A list of paths to the streaming files to be loaded

    Returns:
        A dataframe containg the records from all files loaded into a DataFrame
    """
    dfs = []
    for file_path in file_paths:
        dfs.append(load_file_contents_into_dataframe(file_path))
    # Concat files and reset the index
    stream_df = pd.concat(dfs).reset_index(drop=True)
    logging.info(
        f"Loaded {len(stream_df)} records from {len(file_paths)} files"
    )
    return stream_df
