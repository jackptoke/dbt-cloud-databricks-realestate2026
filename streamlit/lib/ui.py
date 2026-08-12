"""Shared formatting and chart styling.

One place for the palette and the number formats so four pages cannot drift into
four different ideas of what a dollar looks like.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from lib import data

# Sequential-safe categorical palette. Chosen to stay distinguishable in both
# light and dark Streamlit themes and under the common forms of colour blindness
# — no red/green pairing carries meaning on its own anywhere in the app.
PALETTE = [
    "#2F6F8F",  # deep teal-blue
    "#C4623D",  # terracotta
    "#5C8A5C",  # sage
    "#8A6BA1",  # muted violet
    "#B08A3E",  # ochre
    "#4A6D7C",  # slate
]

# Semantic colours. Used only where the direction genuinely is good/bad, and
# always alongside a label so colour is never the only channel.
ABOVE = "#C4623D"
BELOW = "#5C8A5C"
NEUTRAL = "#6B7280"
MUTED = "#9CA3AF"

CHART_LAYOUT = dict(
    margin=dict(l=8, r=8, t=32, b=8),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(size=12),
    hoverlabel=dict(font_size=12),
    legend=dict(orientation="h", yanchor="bottom", y=1.0, xanchor="left", x=0),
)


def style_fig(fig, height: int = 320):
    """Apply the shared layout to a plotly figure."""
    fig.update_layout(height=height, **CHART_LAYOUT)
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor="rgba(128,128,128,0.18)", zeroline=False)
    return fig


def aud(value, precision: int = 0) -> str:
    """Format a number as Australian dollars, or an em dash when absent."""
    if value is None or pd.isna(value):
        return "—"
    return f"${value:,.{precision}f}"


def pct(value, precision: int = 1, signed: bool = False) -> str:
    if value is None or pd.isna(value):
        return "—"
    sign = "+" if signed and value > 0 else ""
    return f"{sign}{value:.{precision}f}%"


def num(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{value:,.0f}"


def page_header(title: str, subtitle: str) -> None:
    st.title(title)
    st.caption(subtitle)


def crawl_footer(coverage_df: pd.DataFrame) -> None:
    """Provenance line shown on every page.

    Non-negotiable per the scope: a reader must never have to guess whether they
    are looking at a mature dataset or a nascent one. Reads from
    mart_data_coverage rather than hardcoded prose so it cannot drift.
    """
    if coverage_df.empty:
        return
    crawled = pd.to_datetime(coverage_df["last_crawled_on"]).max()
    crawl_dates = int(coverage_df["crawl_dates"].max())
    st.divider()
    st.caption(
        f"Crawled {crawled:%d %b %Y} · {crawl_dates} crawl date"
        f"{'s' if crawl_dates != 1 else ''} · "
        f"{int(coverage_df['sales'].sum()):,} sales · "
        f"{int(coverage_df['live_listings'].sum()):,} live listings · "
        f"{int(coverage_df['suburbs'].sum()):,} suburbs across "
        f"{len(coverage_df)} LGAs · source `{data.GOLD}`"
    )


def truncation_note(rows: pd.DataFrame) -> None:
    """Warn where an LGA's sold history was cut short by the source's cap."""
    truncated = rows[rows["is_truncated_history"]]
    if truncated.empty:
        return
    names = ", ".join(sorted(truncated["local_government_area"]))
    st.caption(
        f":warning: History truncated for {names}. The source caps each region "
        "query at 1,500 results and returns the most *relevant* listings, not a "
        "random sample — so early years are missing entirely and the remaining "
        "medians are drawn from a biased subset, not merely a smaller one."
    )
