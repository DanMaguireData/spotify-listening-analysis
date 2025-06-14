"""Module generates basic reporting based on the scroed tracks."""

import logging

import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.text import Text

logger = logging.getLogger(__name__)


class AnalysisReporter:
    """A class for generating and printing beautiful terminal reports from
    analysis DataFrames."""

    def __init__(self, track_summary_df: pd.DataFrame):
        """Initializes the reporter with the aggregated track summary data.

        Args:
            track_summary_df (pd.DataFrame):
                The final aggregated DataFrame from
                `summarize_track_enjoyment`.
        """
        self.df = track_summary_df
        self.console = Console()

    def _create_results_table(self, title: str) -> Table:
        """Creates a styled `rich` Table for displaying top tracks.

        Args:
            title (str): The title to display above the table.

        Returns:
            Table: A `rich` Table object, styled and ready for data.
        """
        table = Table(
            title=Text(title, style="bold magenta"),
            show_header=True,
            header_style="bold cyan",
            border_style="dim",
        )
        table.add_column("Rank", style="dim", width=4)
        table.add_column("Track Name", style="bold", no_wrap=True)
        # --- THIS IS THE CORRECTED LINE ---
        table.add_column("Artist", no_wrap=True)  # Removed style="normal"
        # --- END OF CORRECTION ---
        table.add_column("Play Count", justify="right", style="green")
        table.add_column("Adj. Score", justify="right", style="yellow")
        return table

    def _populate_table_with_data(
        self, table: Table, data: pd.DataFrame
    ) -> None:
        """Populates a rich Table with data from a top tracks DataFrame.

        Args:
            table (Table):
                The `rich` Table to populate.
            data (pd.DataFrame):
                A DataFrame containing the top tracks to display.
        """
        for i, row in enumerate(data.itertuples(), 1):
            table.add_row(
                str(i),
                row.track_name,
                row.album_artist,
                str(row.play_count),
                f"{row.adjusted_enjoyment_score:.4f}",
            )

    def print_overall_top_10(self) -> None:
        """Generates and prints a report for the top 10 tracks overall."""
        self.console.rule(
            "[bold green]🏆 Overall Top 10 Tracks 🏆[/bold green]"
        )

        top_10_overall = self.df.sort_values(
            "adjusted_enjoyment_score", ascending=False
        ).head(10)

        table = self._create_results_table(
            "Based on Bayesian Adjusted Enjoyment Score"
        )
        self._populate_table_with_data(table, top_10_overall)
        self.console.print(table)

    def print_overall_bottom_10(self) -> None:
        """Generates and prints a report for the bottom 10 tracks overall."""
        self.console.rule(
            "[bold red]👎 Overall Least Enjoyed Tracks 👎[/bold red]"
        )

        bottom_10_overall = self.df.sort_values(
            "adjusted_enjoyment_score", ascending=False
        ).tail(10)

        table = self._create_results_table(
            "Based on Bayesian Adjusted Enjoyment Score"
        )
        self._populate_table_with_data(table, bottom_10_overall)
        self.console.print(table)

    def print_top_10_by_year(self) -> None:
        """Generates and prints reports for the top 10 tracks for each year
        based on when they were first listened to."""
        self.console.rule(
            "[bold green]📅 Top 10 Tracks by "
            "Year of First Listen 📅[/bold green]"
        )

        # Ensure 'first_listen' is a datetime object to extract the year
        df_with_year = self.df.copy()
        df_with_year["year"] = pd.to_datetime(
            df_with_year["first_listen"]
        ).dt.year

        # Get unique years and sort them
        years = sorted(
            df_with_year["year"].unique(), reverse=True
        )  # Show recent years first

        for year in years:
            top_10_for_year = (
                df_with_year[df_with_year["year"] == year]
                .sort_values("adjusted_enjoyment_score", ascending=False)
                .head(10)
            )

            if top_10_for_year.empty:
                continue

            table = self._create_results_table(f"Top Tracks for {year}")
            self._populate_table_with_data(table, top_10_for_year)
            self.console.print(table)
