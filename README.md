# Spotify Listening Analysis
A Python project for an in-depth analysis of your personal Spotify streaming history. Uncover your most listened-to tracks, artists, genres, and evolving listening patterns, creating a personalized music profile beyond the annual Wrapped summary. This repository emphasizes professional coding standards and Python best practices.

## Features (WIP)

*   Detailed analysis of listening habits over time.
*   Identification of top tracks, artists, and genres.
*   Visualization of listening trends.

## Setup and Usage

### 1. Prerequisites

*   Python 3.11+
*   Git

### 2. Getting Your Spotify Data

This project requires your **Extended Streaming History** data from Spotify. Here's how to request it:

1.  **Navigate to Spotify's Privacy Settings:**
    Go to [https://www.spotify.com/us/account/privacy/](https://www.spotify.com/us/account/privacy/) and log in if prompted.

2.  **Request Your Data:**
    *   Scroll down towards the bottom of the page until you find a section related to "Download your data".
    *   You need to request your **Extended streaming history**. This is different from the more basic "Account data" and contains the detailed track-by-track listening information necessary for this analysis. Ensure you select the option that gives you your listening history for "all time" or the longest period available.
    *   Follow the on-screen instructions to submit your request.

3.  **Wait for Spotify's Email:**
    Spotify will process your request and then email you a link to download your data. **This process can take several days, sometimes up to 30 days**, though often it's much faster.

4.  **Download and Place Your Data:**
    *   Once you receive the email from Spotify, download the ZIP file.
    *   Extract the contents of the ZIP file. You should find one or more JSON files containing your streaming history (e.g., `StreamingHistory0.json`, `endsong_0.json`, `Streaming_History_Audio_Features_0.json`, etc. – the exact names might vary).
    *   Create a directory named `data` in the root of this project (e.g., `spotify-deep-dive/data/`).
    *   Place all the downloaded streaming history JSON files into this `data/` directory.
    *   **Important:** Add `data/` to your `.gitignore` file to ensure you don't accidentally commit your personal streaming data to the repository. If it's not already there, add the line `data/` to your `.gitignore`.




### 3. Setting Up the Project Environment

1.  **Clone the repository (if you haven't already):**
    ```bash
    git clone https://github.com/your-username/spotify-deep-dive.git
    cd spotify-deep-dive
    ```

2.  **Create and activate a virtual environment:**
    This isolates project dependencies.
    ```bash
    python -m venv .venv
    ```
    Activate it:
    *   On macOS/Linux:
        ```bash
        source .venv/bin/activate
        ```
    *   On Windows (Git Bash):
        ```bash
        source .venv/Scripts/activate
        ```
    *   On Windows (CMD/PowerShell):
        ```bash
        .venv\Scripts\activate
        ```
    Your command prompt should change to indicate the active virtual environment.

3.  **Install dependencies:**
    This project uses a `requirements.txt` for core dependencies and `requirements-dev.txt` for development tools.
    ```bash
    pip install -r requirements.txt
    pip install -r requirements-dev.txt  # Installs tools like pre-commit, linters, etc.
    ```
    *(We will create/update these files as we add libraries. `pre-commit` itself should be listed in `requirements-dev.txt`)*

4.  **Set up Pre-Commit Hooks:**
    This project uses `pre-commit` to automatically run linters and formatters (like Black, Flake8, isort) before each commit, ensuring code quality and consistency.

    *   **Install `pre-commit` (if not already installed via `requirements-dev.txt`):**
        If you haven't installed it via `requirements-dev.txt` or don't have it globally, you can install it:
        ```bash
        pip install pre-commit
        # Alternatively, on macOS with Homebrew:
        # brew install pre-commit
        ```
        *(It's best practice to include `pre-commit` in your `requirements-dev.txt` so it's version-controlled with the project.)*

    *   **Install the Git hook scripts:**
        This command reads the `.pre-commit-config.yaml` file (which we will create) and sets up the hooks for this repository. You only need to run this once per clone/setup.
        ```bash
        pre-commit install
        ```
        Now, `pre-commit` will automatically run its configured checks whenever you run `git commit`. If any checks fail, the commit will be aborted, allowing you to fix the issues.

    *   **Useful Pre-Commit Commands:**
        *   **Update hook versions:** To update the hooks to their latest versions as defined in your `.pre-commit-config.yaml` (or their repositories):
            ```bash
            pre-commit autoupdate
            ```
        *   **Run manually on all files:** If you want to run all pre-commit hooks across all files at any time:
            ```bash
            pre-commit run --all-files
            ```
        *   **Skip pre-commit hooks (use with caution):** If you need to make a commit without running the hooks (e.g., for a work-in-progress commit you don't intend to push yet):
            ```bash
            git commit --no-verify
            ```

5.  **Configure Spotify API Credentials (Environment Variables):**
    This project securely manages sensitive information like API keys using environment variables.

    *   **Obtain Spotify API Credentials:**
        1.  Go to your Spotify Developer Dashboard: [https://developer.spotify.com/dashboard/applications](https://developer.spotify.com/dashboard/applications)
        2.  Log in or create an account.
        3.  Create a new application or select an existing one.
        4.  Note down your `Client ID` and `Client Secret`.
        5.  In your application settings, under "Redirect URIs," add `http://localhost:8890/` (or your preferred redirect URI). This is crucial for the OAuth flow.

    *   **Set up your `.env` file:**
        A template file, `.env.example`, is provided in the project root. This file shows you which environment variables are needed.

        1.  **Copy the example file:**
            ```bash
            cp .env.example .env
            ```
        2.  **Edit the `.env` file:** Open the newly created `.env` file in your text editor and replace the placeholder values (`your_spotify_client_id_here`, `your_spotify_client_secret_here`) with your actual Spotify credentials. You can also customize `SPOTIPY_REDIRECT_URI` and `SPOTIPY_SCOPE` if needed.

        ```ini
        # Example of your .env file (DO NOT COMMIT THIS FILE!)
        SPOTIPY_CLIENT_ID=your_actual_client_id
        SPOTIPY_CLIENT_SECRET=your_actual_client_secret
        SPOTIPY_REDIRECT_URI=http://localhost:8890/
        SPOTIPY_SCOPE=user-library-read
        ```
        **Important:** The `.env` file is explicitly excluded from version control (`.gitignore`) to protect your sensitive data. **Do NOT commit your `.env` file.**
