"""
Spotify Web API client and data fetching utilities.

This module provides an interface for interacting with the Spotify Web API
using the `spotipy` library. It focuses on abstracting API requests to fetch
additional metadata (like track details, audio features, album information)
that is not available in the raw streaming history files.

Key functionalities include:
  - Initializing and authenticating the `spotipy` client.
  - Fetching detailed track information for a list of Spotify track IDs.
  - Transforming raw API responses into a structured format
    (e.g., Pandas DataFrame) suitable for downstream data processing and
    analysis.

This module is designed to be the single point of contact for all external
Spotify API interactions, isolating network operations and API-specific logic
from the core data pipeline.
"""
