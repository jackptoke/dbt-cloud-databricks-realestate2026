"""Market Explorer — the landing page.

What a market costs, what it has done, and what is on it right now.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from lib import data, ui


def render() -> None:
    ui.page_header(
        "Market Explorer",
        "Sold prices, supply and affordability across western Victoria — for individual investors.",
    )

    scorecard = data.scorecard()
    coverage = data.coverage()

    reportable = scorecard[scorecard["is_reportable"]]
    if reportable.empty:
        st.warning("No LGA currently has enough sales to report on.")
        return

    # --- filters -----------------------------------------------------------
    # Default to the reportable LGAs. The other three hold 4, 2 and 1 sales
    # between them and are by-catch from a neighbouring region's crawl; they are
    # reachable via the toggle rather than hidden, because "we excluded these
    # and here is why" is a stronger statement than a filtered list.
    show_all = st.toggle(
        "Include LGAs with too little data to report",
        value=False,
        help=(
            "Loddon, Buloke and Southern Grampians appear only as stray listings "
            "picked up by a neighbouring region's crawl. Their medians are drawn "
            "from a handful of sales."
        ),
    )
    pool = scorecard if show_all else reportable

    c1, c2, c3 = st.columns([2, 1.4, 1])
    lga = c1.selectbox(
        "Local Government Area",
        options=sorted(pool["local_government_area"]),
        index=0,
    )
    row = pool[pool["local_government_area"] == lga].iloc[0]

    trend = data.price_trend()
    trend_lga = trend[trend["lga_key"] == row["lga_key"]]

    types = ["All types"] + sorted(trend_lga["property_type"].dropna().unique())
    ptype = c2.selectbox("Property type", options=types, index=0)

    dist = data.sales_for_distribution()
    dist_lga = dist[dist["lga_key"] == row["lga_key"]]
    bed_opts = ["Any"] + [
        str(int(b)) for b in sorted(dist_lga["bedrooms"].dropna().unique()) if b <= 6
    ]
    beds = c3.selectbox("Bedrooms", options=bed_opts, index=0)

    if not row["is_reportable"]:
        st.error(
            f"**{lga}** holds only {int(row['total_sales'])} sales in this dataset. "
            "Every figure below is shown for completeness, not for decisions."
        )

    # --- KPIs --------------------------------------------------------------
    k = st.columns(6)
    k[0].metric("Median sold · 12m", ui.aud(row["median_sold_price_12m_aud"]))
    k[1].metric("Sales · 12m", ui.num(row["sales_12m"]))
    k[2].metric("Median asking", ui.aud(row["median_asking_price_aud"]))
    k[3].metric("Active listings", ui.num(row["active_listings"]))
    k[4].metric(
        "Growth p.a.",
        ui.pct(row["annual_growth_pct"]),
        help=(
            "Repeat-sales CAGR — the median annual return of properties that sold "
            "twice, which holds the dwelling constant. Used instead of a windowed "
            "figure because history depth varies from 3.9 to 19.1 years across "
            "these LGAs, so a 10-year comparison would compare different spans of "
            "time rather than different markets."
        ),
    )
    # pd.notna, not truthiness: a NULL ratio arrives as NaN, and NaN is truthy,
    # so `if row[...]` renders the literal string "nan×".
    pti = row["price_to_income_ratio"]
    k[5].metric(
        "Price to income",
        f"{pti:.1f}×" if pd.notna(pti) else "—",
        help=(
            "Median sold price over the LGA's population-weighted mean household "
            "income. An approximation for ranking, not a lending calculation."
        ),
    )

    # An LGA reachable through the low-data toggle can have no dated sale at
    # all, and formatting NaT raises TypeError — so the depth clause is built
    # only when there is a date to describe.
    depth = ""
    if pd.notna(row["earliest_sale_date"]) and pd.notna(row["history_years"]):
        depth = (
            f", reaching back {row['history_years']} years to "
            f"{pd.to_datetime(row['earliest_sale_date']):%b %Y}"
        )
    st.caption(
        f"{int(row['total_sales']):,} sales on record{depth}. "
        f"Growth from {int(row['repeat_sale_properties'])} repeat-sale properties."
    )

    # --- price trend -------------------------------------------------------
    st.subheader("Median sold price by year")

    # A real median either way. For a single type that is the mart row; for
    # "All types" it is a separate query against fct_sale, because combining the
    # per-type medians would be averaging medians — a number that looks like a
    # median and is not one.
    if ptype == "All types":
        allt = data.price_trend_all_types()
        series = allt[allt["lga_key"] == row["lga_key"]].copy()
    else:
        series = trend_lga[trend_lga["property_type"] == ptype].copy()

    if series.empty:
        st.info("No sales recorded for this combination.")
    else:
        series = series.sort_values("sold_year")
        years = series["sold_year"].tolist()
        med = series["median_price_aud"].tolist()
        q1 = series["q1_price_aud"].tolist()
        q3 = series["q3_price_aud"].tolist()
        counts = series["sales"].tolist()

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=years + years[::-1],
                y=q3 + q1[::-1],
                fill="toself",
                fillcolor="rgba(47,111,143,0.15)",
                # mode is explicit: plotly defaults to lines+markers under 20
                # points, which speckles the band edges with stray dots.
                mode="lines",
                line=dict(width=0),
                hoverinfo="skip",
                name="Q1-Q3 range",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=years,
                y=med,
                mode="lines+markers",
                line=dict(color=ui.PALETTE[0], width=2.5),
                marker=dict(size=6),
                name="Median",
                customdata=counts,
                hovertemplate="%{x}<br>Median %{y:$,.0f}<br>%{customdata} sales<extra></extra>",
            )
        )
        partial = series[series["is_partial_year"]]["sold_year"].tolist()
        if partial:
            fig.add_vrect(
                x0=min(partial) - 0.5,
                x1=max(years) + 0.5,
                fillcolor="rgba(156,163,175,0.18)",
                line_width=0,
                annotation_text="part year",
                annotation_position="top left",
                annotation_font_size=11,
            )
        fig.update_yaxes(tickprefix="$", tickformat=",.0f")
        st.plotly_chart(ui.style_fig(fig, 340), use_container_width=True)

        thin = series[series["sales"] < 10]
        if not thin.empty:
            st.caption(
                f"{len(thin)} year(s) rest on fewer than 10 sales — the median is "
                "a weak signal there and the Q1-Q3 band will look erratic."
            )
        ui.truncation_note(coverage[coverage["lga_key"] == row["lga_key"]])

    # --- distribution and supply ------------------------------------------
    left, right = st.columns(2)

    with left:
        st.subheader("Price spread by property type")
        d = dist_lga
        if beds != "Any":
            d = d[d["bedrooms"] == int(beds)]
        # Only types with enough sales to draw a meaningful box.
        keep = d.groupby("property_type")["sale_price_aud"].count()
        d = d[d["property_type"].isin(keep[keep >= 10].index)]
        if d.empty:
            st.info("Not enough sales in the last 24 months to show a distribution.")
        else:
            fig = go.Figure()
            for i, t in enumerate(sorted(d["property_type"].unique())):
                vals = d[d["property_type"] == t]["sale_price_aud"]
                fig.add_trace(
                    go.Box(
                        y=vals,
                        name=t,
                        marker_color=ui.PALETTE[i % len(ui.PALETTE)],
                        boxpoints=False,
                    )
                )
            fig.update_layout(showlegend=False)
            fig.update_yaxes(tickprefix="$", tickformat=",.0f")
            st.plotly_chart(ui.style_fig(fig, 320), use_container_width=True)
            st.caption("Settled sales, last 24 months. Types with under 10 sales are omitted.")

    with right:
        st.subheader("Supply mix")
        live = data.valuation()
        live_lga = live[live["lga_key"] == row["lga_key"]]
        sales_by_type = dist_lga.groupby("property_type").size()
        live_by_type = live_lga.groupby("property_type").size()
        types_all = sorted(set(sales_by_type.index) | set(live_by_type.index))
        if not types_all:
            st.info("No listings or sales to compare.")
        else:
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=types_all,
                    y=[int(live_by_type.get(t, 0)) for t in types_all],
                    name="Active listings",
                    marker_color=ui.PALETTE[0],
                )
            )
            fig.add_trace(
                go.Bar(
                    x=types_all,
                    y=[int(sales_by_type.get(t, 0)) for t in types_all],
                    name="Sales (24m)",
                    marker_color=ui.PALETTE[1],
                )
            )
            fig.update_layout(barmode="group")
            st.plotly_chart(ui.style_fig(fig, 320), use_container_width=True)
            st.caption(
                "Active listings are deduplicated across portals, so a property "
                "advertised twice is counted once."
            )

    ui.crawl_footer(coverage)
