# src/infographics.py
"""Generates high-quality, standalone infographic visualizations from analysis
data.

This module is dedicated to creating bespoke, presentation-ready infographics
that go beyond standard charts. It uses matplotlib's lower-level APIs to gain
fine-grained control over layout, text, and design elements, producing a
professional and narrative-driven visual output.

The functions here are designed to be called at the end of the analysis
pipeline, taking a processed and aggregated DataFrame as input.
"""

import logging
import textwrap
import urllib.request
from datetime import datetime
from io import BytesIO
from typing import Dict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.patches import FancyBboxPatch, Rectangle
from PIL import Image

logger = logging.getLogger(__name__)

# Professional color palette, defined as a module-level constant.
INFOGRAPHIC_COLORS: Dict[str, str] = {
    "primary_black": "#0B0B0B",
    "spotify_green": "#1ED760",
    "accent_green": "#1DB954",
    "white": "#FFFFFF",
    "light_grey": "#E1E1E1",
    "medium_grey": "#B3B3B3",
    "dark_grey": "#404040",
    "card_bg": "#1A1A1A",
    "subtle_accent": "#2A2A2A",
    "fuchsia": "#f037a5",
}


def _truncate_text(text: str, max_length: int = 25) -> str:
    """Truncates text with an ellipsis if it exceeds the maximum length."""
    return text if len(text) <= max_length else text[: max_length - 3] + "..."


def _get_image_from_url(url: str) -> Image:
    """Fetches and opens an image from a given URL."""
    with urllib.request.urlopen(url) as img:
        return Image.open(BytesIO(img.read()))


def _plot_header(ax: Axes) -> None:
    """Draws the header section of the infographic."""
    ax.set_facecolor(INFOGRAPHIC_COLORS["primary_black"])
    ax.axis("off")
    ax.text(
        0.0,
        0.7,
        "YOUR TOP TRACKS",
        fontsize=32,
        weight="bold",
        color=INFOGRAPHIC_COLORS["white"],
    )
    ax.text(
        0.0,
        0.3,
        "Ranked by Personal Enjoyment Score",
        fontsize=16,
        color=INFOGRAPHIC_COLORS["medium_grey"],
    )
    ax.add_patch(
        Rectangle(
            (0, 0),
            0.32,
            0.08,
            facecolor=INFOGRAPHIC_COLORS["fuchsia"],
            transform=ax.transAxes,
        )
    )


def _plot_footer(ax: Axes) -> None:
    """Draws the footer section of the infographic."""
    ax.set_facecolor(INFOGRAPHIC_COLORS["primary_black"])
    ax.axis("off")

    ax.text(
        0.0,
        0.8,
        "METHODOLOGY",
        fontsize=10,
        weight="bold",
        color=INFOGRAPHIC_COLORS["medium_grey"],
    )
    methodology_text = (
        "Enjoyment Score combines listening frequency, completion "
        "rates, and user engagement patterns. "
        "Scores are normalized, with higher values indicating stronger"
        " personal connection to tracks."
    )
    wrapped_text = textwrap.fill(methodology_text, width=80)
    ax.text(
        0.0,
        0.25,
        wrapped_text,
        fontsize=9,
        color=INFOGRAPHIC_COLORS["dark_grey"],
        linespacing=1.5,
    )

    current_date = datetime.now().strftime("%B %Y")
    ax.text(
        1.0,
        0.8,
        f"Generated: {current_date}",
        fontsize=9,
        color=INFOGRAPHIC_COLORS["medium_grey"],
        ha="right",
    )
    ax.text(
        1.0,
        0.5,
        "Data Source: Spotify Listening History",
        fontsize=9,
        color=INFOGRAPHIC_COLORS["dark_grey"],
        ha="right",
    )


def _plot_main_chart(ax: Axes, top_tracks: pd.DataFrame) -> None:
    """Draws the main horizontal bar chart of top tracks."""
    ax.set_facecolor(INFOGRAPHIC_COLORS["primary_black"])
    for spine in ax.spines.values():
        spine.set_visible(False)

    y_positions = np.arange(len(top_tracks))
    bar_height = 0.75

    # Plot bars
    ax.barh(
        y_positions,
        top_tracks["adjusted_enjoyment_score"],
        height=bar_height,
        color=INFOGRAPHIC_COLORS["spotify_green"],
        alpha=0.95,
        edgecolor=INFOGRAPHIC_COLORS["accent_green"],
        linewidth=0.5,
    )

    # Format and set labels
    track_labels = [
        f"{_truncate_text(str(row.track_name), 30)}\n"
        f"{_truncate_text(str(row.album_artist), 25)}"
        for _, row in top_tracks.iterrows()
    ]
    ax.set_yticks(y_positions)
    ax.set_yticklabels(
        track_labels,
        fontsize=11,
        color=INFOGRAPHIC_COLORS["white"],
        verticalalignment="center",
    )
    ax.get_xaxis().set_visible(False)
    ax.invert_yaxis()

    # Add rank and score annotations
    max_score = top_tracks["adjusted_enjoyment_score"].max()
    for i, row in top_tracks.iterrows():
        score = row["adjusted_enjoyment_score"]
        ax.text(
            -0.15,
            y_positions[i],
            f"#{i+1}",
            ha="center",
            va="center",
            fontsize=13,
            weight="bold",
            color=INFOGRAPHIC_COLORS["spotify_green"],
        )
        ax.text(
            max_score + 0.05,
            y_positions[i],
            f"{score:.3f}",
            ha="left",
            va="center",
            fontsize=12,
            weight="bold",
            color=INFOGRAPHIC_COLORS["white"],
        )

    # Final styling
    ax.set_xlim(-0.2, max_score + 0.2)
    ax.set_ylim(-0.5, len(top_tracks) - 0.5)
    for x_val in np.linspace(0, max_score, 5):
        if x_val > 0:
            ax.axvline(
                x_val,
                color=INFOGRAPHIC_COLORS["dark_grey"],
                alpha=0.2,
                linewidth=0.5,
                linestyle="--",
            )


def _plot_stats_panel(
    fig: Figure, ax: Axes, top_track: pd.Series, track_summary_df: pd.DataFrame
) -> None:
    """Draws the statistics panel card."""
    ax.set_facecolor(INFOGRAPHIC_COLORS["primary_black"])
    ax.axis("off")

    # Card background
    card_bg = FancyBboxPatch(
        (0.05, 0.05),
        0.9,
        0.9,
        boxstyle="round,pad=0.02",
        facecolor=INFOGRAPHIC_COLORS["card_bg"],
        edgecolor=INFOGRAPHIC_COLORS["dark_grey"],
        linewidth=1,
        transform=ax.transAxes,
    )
    ax.add_patch(card_bg)

    # Top track score
    ax.text(
        0.5,
        0.8,
        "HIGHEST SCORE",
        fontsize=11,
        weight="bold",
        color=INFOGRAPHIC_COLORS["medium_grey"],
        ha="center",
        transform=ax.transAxes,
    )
    ax.text(
        0.5,
        0.7,
        f"{top_track['adjusted_enjoyment_score']:.3f}",
        fontsize=35,
        weight="bold",
        color=INFOGRAPHIC_COLORS["spotify_green"],
        ha="center",
        transform=ax.transAxes,
    )

    # Album artwork
    text_y_pos = 0.55  # Default y-position for text if image fails
    try:
        top_img = _get_image_from_url(top_track["album_artwork_url"])
        img_ax = fig.add_axes(
            [
                ax.get_position().x0 + 0.06,
                ax.get_position().y0 + 0.28,
                0.12,
                0.12,
            ]
        )
        img_ax.imshow(top_img)
        img_ax.axis("off")
        img_border = FancyBboxPatch(
            (0, 0),
            1,
            1,
            boxstyle="round,pad=0.02",
            facecolor="none",
            edgecolor=INFOGRAPHIC_COLORS["dark_grey"],
            linewidth=1,
            transform=img_ax.transAxes,
        )
        img_ax.add_patch(img_border)
        text_y_pos = 0.4  # Adjust text position to make space for image
    except Exception as e:
        logger.warning(f"Could not load album artwork for stats panel: {e}")

    # Top track details
    track_name_display = _truncate_text(str(top_track["track_name"]), 20)
    artist_name_display = _truncate_text(str(top_track["album_artist"]), 18)
    ax.text(
        0.5,
        text_y_pos,
        f'"{track_name_display}"',
        fontsize=13,
        color=INFOGRAPHIC_COLORS["white"],
        ha="center",
        weight="bold",
        transform=ax.transAxes,
        style="italic",
    )
    ax.text(
        0.5,
        text_y_pos - 0.05,
        artist_name_display,
        fontsize=11,
        color=INFOGRAPHIC_COLORS["medium_grey"],
        ha="center",
        transform=ax.transAxes,
    )

    # Summary statistics
    total_tracks = len(track_summary_df)
    avg_score = track_summary_df["adjusted_enjoyment_score"].mean()
    stats_items = [
        ("TOTAL ANALYZED", f"{total_tracks:,}"),
        ("AVERAGE SCORE", f"{avg_score:.3f}"),
    ]
    for i, (label, value) in enumerate(stats_items):
        y_pos = 0.32 - (i * 0.1)
        ax.text(
            0.5,
            y_pos,
            value,
            fontsize=14,
            weight="bold",
            color=INFOGRAPHIC_COLORS["spotify_green"],
            ha="center",
            transform=ax.transAxes,
        )
        ax.text(
            0.5,
            y_pos - 0.04,
            label,
            fontsize=9,
            color=INFOGRAPHIC_COLORS["medium_grey"],
            ha="center",
            transform=ax.transAxes,
        )


def plot_top_tracks_infographic(
    track_summary_df: pd.DataFrame,
    top_n: int = 10,
    save_path: str = "reports/top_tracks_infographic.png",
):
    """Generates a professional, multi-part infographic for top tracks.

    This function orchestrates the creation of a complex visualization by
    delegating the drawing of each component (header, chart, stats, footer)
    to specialized helper functions.

    Args:
        track_summary_df (pd.DataFrame): Aggregated DataFrame of tracks, sorted
            by enjoyment score.
        top_n (int, optional):
            The number of top tracks to display. Defaults to 10.
        save_path (str, optional): The path to save the generated image
            file. Defaults to "reports/top_tracks_infographic.png".
    """
    if track_summary_df.empty:
        logger.warning(
            "Track summary DataFrame is empty. "
            "Skipping infographic generation."
        )
        return

    top_tracks = (
        track_summary_df.sort_values(
            "adjusted_enjoyment_score", ascending=False
        )
        .head(top_n)
        .reset_index()
    )

    fig_height = 10 + max(0.6 * top_n, 6) * 0.3
    fig = plt.figure(figsize=(18, fig_height))
    fig.set_facecolor(INFOGRAPHIC_COLORS["primary_black"])

    grid_spec = fig.add_gridspec(
        3,
        3,
        height_ratios=[0.6, 3, 0.4],
        width_ratios=[2.2, 1, 0.3],
        hspace=0.15,
        wspace=0.1,
        left=0.05,
        right=0.95,
        top=0.95,
        bottom=0.05,
    )

    # --- Call helper functions to draw each section ---
    _plot_header(fig.add_subplot(grid_spec[0, :2]))
    _plot_main_chart(fig.add_subplot(grid_spec[1, 0]), top_tracks)
    _plot_stats_panel(
        fig,
        fig.add_subplot(grid_spec[1, 1]),
        top_tracks.iloc[0],
        track_summary_df,
    )
    _plot_footer(fig.add_subplot(grid_spec[2, :2]))

    try:
        plt.savefig(
            save_path,
            facecolor=INFOGRAPHIC_COLORS["primary_black"],
            dpi=300,
            bbox_inches="tight",
            edgecolor="none",
            pad_inches=0.1,
        )
        logger.info(f"Infographic saved successfully to {save_path}")
    except Exception as e:
        logger.error(f"Failed to save infographic to {save_path}: {e}")
    finally:
        plt.close(fig)  # Ensure the figure is closed to free memory
