"""Coverage & Caveats — what this dataset can and cannot answer.

Reads from mart_data_coverage rather than hardcoded prose, so the caveats cannot
drift away from the data they describe.
"""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from lib import data, ui

# Measures that need a second ingest_date before they can be computed at all.
BLOCKED_ON_HISTORY = [
    ("Days on market", "A listing must be observed across many crawl dates."),
    ("Price reductions", "A price cut is a delta between two observations."),
    ("Discount to asking", "Needs a buy listing to later appear as sold."),
    ("Listing velocity", "New listings against sales requires a time axis."),
]


def render() -> None:
    ui.page_header(
        "Coverage & Caveats",
        "What is in this dataset, what is missing, and which questions it cannot answer yet.",
    )

    cov = data.coverage()
    if cov.empty:
        st.warning("No coverage data available.")
        return

    crawl_dates = int(cov["crawl_dates"].max())
    supports_ts = bool(cov["supports_time_series"].any())

    # --- the three limits --------------------------------------------------
    st.subheader("Three separate limits, easy to confuse")
    a, b, c = st.columns(3)
    with a:
        st.markdown("**Depth**")
        st.caption(
            "The source caps each region query at 1,500 results. The cap bites "
            "hardest where the market is busiest, so history depth runs "
            "*inverse* to activity — the busiest LGA has the shallowest history."
        )
    with b:
        st.markdown("**Breadth**")
        st.caption(
            f"The crawl covers {len(cov)} LGAs of western Victoria, not the "
            "state. Nothing here generalises to metropolitan Melbourne or to "
            "other regions."
        )
    with c:
        st.markdown("**Time**")
        st.caption(
            f"{crawl_dates} crawl date"
            f"{'s' if crawl_dates != 1 else ''} on record. "
            + (
                "Day-over-day measures are computable."
                if supports_ts
                else "Until a second lands, every day-over-day measure is "
                "**uncomputable**, not merely imprecise."
            )
        )

    # --- history depth -----------------------------------------------------
    st.subheader("How far each LGA's sold history reaches")
    depth = cov[cov["sales"] > 0].sort_values("history_years")
    fig = go.Figure(
        go.Bar(
            x=depth["history_years"],
            y=depth["local_government_area"],
            orientation="h",
            marker_color=[
                ui.ABOVE if t else ui.PALETTE[2] for t in depth["is_truncated_history"]
            ],
            text=[
                f"{y:.1f} yrs · {int(s):,} sales" + (" · capped" if t else " · complete")
                for y, s, t in zip(
                    depth["history_years"], depth["sales"], depth["is_truncated_history"]
                )
            ],
            textposition="outside",
            hovertemplate="%{y}<br>%{x:.1f} years<extra></extra>",
        )
    )
    fig.update_xaxes(title="Years of sold history", range=[0, depth["history_years"].max() * 1.6])
    fig.update_layout(showlegend=False)
    st.plotly_chart(ui.style_fig(fig, 300), use_container_width=True)
    st.caption(
        "Red bars hit the source's result ceiling — their 1,500 sales are the "
        "most *relevant* the source held, not a random sample, so their medians "
        "are drawn from a biased subset rather than merely a smaller one. Green "
        "bars are complete because the market was quiet enough to fit under the cap."
    )

    # --- per-LGA table -----------------------------------------------------
    st.subheader("Per-LGA coverage")
    st.dataframe(
        cov[[
            "local_government_area", "is_reportable", "suburbs", "sales",
            "history_years", "is_truncated_history", "supplying_crawl_regions",
            "buy_listings", "listings_with_verdict", "rent_listings",
        ]],
        hide_index=True,
        use_container_width=True,
        column_config={
            "local_government_area": "LGA",
            "is_reportable": st.column_config.CheckboxColumn("Reportable"),
            "suburbs": st.column_config.NumberColumn("Suburbs", width="small"),
            "sales": st.column_config.NumberColumn("Sales"),
            "history_years": st.column_config.NumberColumn("History (yrs)", format="%.1f"),
            "is_truncated_history": st.column_config.CheckboxColumn("Capped"),
            "supplying_crawl_regions": st.column_config.NumberColumn("Crawls", width="small"),
            "buy_listings": st.column_config.NumberColumn("Buy"),
            "listings_with_verdict": st.column_config.NumberColumn("Valued"),
            "rent_listings": st.column_config.NumberColumn("Rent"),
        },
    )

    # --- geography resolution ---------------------------------------------
    st.subheader("How suburbs were matched to LGAs")
    total = int(cov["suburbs"].sum())
    q = st.columns(4)
    q[0].metric("Exact match", ui.num(cov["suburbs_matched_exact"].sum()),
                help="Matched on suburb, postcode and state against the ABS reference.")
    q[1].metric("By name", ui.num(cov["suburbs_matched_by_name"].sum()),
                help="Postcode was wrong in the source but the name is unambiguous within the state.")
    q[2].metric("Manual override", ui.num(cov["suburbs_matched_by_override"].sum()),
                help="Localities the ABS reference does not carry, assigned by hand.")
    q[3].metric("Unmatched", ui.num(cov["suburbs_unmatched"].sum()),
                help="Resolved to no LGA. These are excluded from every LGA rollup.")
    st.caption(
        f"{total} suburbs in total. A match rate means little without knowing how "
        "the matches were made, which is why the tiers are broken out rather than "
        "summed into one percentage."
    )

    # --- known gaps in the sold data --------------------------------------
    st.subheader("Known gaps in the sold data")
    g = st.columns(4)
    g[0].metric("No sold date", ui.num(cov["sales_without_date"].sum()))
    g[1].metric("Price withheld", ui.num(cov["sales_with_withheld_price"].sum()))
    g[2].metric("Address not identified", ui.num(cov["sales_without_identified_address"].sum()))
    g[3].metric("Consolidated duplicates", ui.num(cov["sales_consolidated_from_duplicates"].sum()),
                help=(
                    "Sales the source published under more than one listing id "
                    "and the pipeline merged back into one event."
                ))

    # --- deliberate omissions ----------------------------------------------
    st.subheader("Deliberately not shown")
    rentals = int(cov["rent_listings"].sum())
    st.markdown(
        f"- **Rental yield.** {rentals} rental listings exist dataset-wide. "
        "Sliced to the grain a yield figure needs — suburb, property type and "
        "bedrooms — only two cells clear five observations. Two credible numbers "
        "is not an analysis, so yield is deferred rather than published thinly. "
        "The rental pipeline still runs and accumulates.\n"
        "- **Swimming pool as a feature premium.** 21 listings out of 6,609 "
        "properties. Rural Victoria, not a data fault.\n"
        "- **Inspections.** 43 records, too thin to build on."
    )

    st.subheader("Blocked until the pipeline accumulates")
    if supports_ts:
        st.success("A second crawl date has landed — these are now computable.")
    for name, why in BLOCKED_ON_HISTORY:
        st.markdown(f"- **{name}** — {why}")
    st.caption(
        "These are placeholders on purpose. The daily cadence is the product: a "
        "one-off crawl is a snapshot, and the measures investors care about live "
        "in the differences between snapshots."
    )

    ui.crawl_footer(cov)
