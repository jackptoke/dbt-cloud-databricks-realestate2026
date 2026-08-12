"""Capital Growth — repeat sales.

The strongest evidence in the dataset, because it holds the dwelling constant.
"""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from lib import data, ui


def render() -> None:
    ui.page_header(
        "Capital Growth",
        "What properties actually returned between their own resales — the same dwelling, bought and sold.",
    )

    rs = data.repeat_sales()
    rates = data.growth_rates()
    coverage = data.coverage()

    if rs.empty:
        st.warning("No repeat sales available.")
        return

    st.info(
        "**Why repeat sales rather than a median-price series.** A median tracks "
        "whatever happened to sell that year, so it moves when the *mix* of "
        "housing moves. Comparing a property against itself removes that. It is "
        "also the only growth measure comparable across these LGAs, whose sold "
        "history runs from 3.9 to 19.1 years deep — a windowed figure would "
        "compare different spans of time.",
        icon=":material/info:",
    )

    # --- filters -----------------------------------------------------------
    c1, c2, c3 = st.columns([2, 1.4, 1.4])
    lgas = ["All LGAs"] + sorted(rs["local_government_area"].dropna().unique())
    lga = c1.selectbox("Local Government Area", options=lgas, index=0)
    types = ["All types"] + sorted(rs["property_type"].dropna().unique())
    ptype = c2.selectbox("Property type", options=types, index=0)
    exclude_short = c3.toggle(
        "Exclude holds under a year",
        value=True,
        help=(
            "Annualising a gain over a few weeks produces a real number that "
            "means nothing — a 10% gain over 10 days annualises past 3,000%. "
            "Those rows carry a null annualised figure; this also drops them "
            "from the totals below."
        ),
    )

    view = rs.copy()
    if lga != "All LGAs":
        view = view[view["local_government_area"] == lga]
    if ptype != "All types":
        view = view[view["property_type"] == ptype]
    if exclude_short:
        view = view[~view["is_short_hold"]]

    if view.empty:
        st.info("No repeat-sale properties match these filters.")
        return

    annualised = view["annualised_growth_pct"].dropna()

    # --- KPIs --------------------------------------------------------------
    k = st.columns(5)
    k[0].metric("Properties", ui.num(len(view)))
    k[1].metric(
        "Median growth p.a.",
        ui.pct(annualised.median()) if not annualised.empty else "—",
    )
    k[2].metric("Median hold", f"{view['years_held'].median():.1f} yrs")
    k[3].metric("Median total growth", ui.pct(view["total_growth_pct"].median(), signed=True))
    k[4].metric(
        "Sold 3+ times",
        ui.num(int(view["spans_multiple_ownerships"].sum())),
        help=(
            "These span more than one ownership, so 'years held' is the "
            "property's observed history rather than any single owner's."
        ),
    )

    left, right = st.columns([1.3, 1])

    # --- scatter -----------------------------------------------------------
    with left:
        st.subheader("Holding period vs annualised growth")
        pts = view.dropna(subset=["annualised_growth_pct"])
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=pts["years_held"],
                y=pts["annualised_growth_pct"],
                mode="markers",
                marker=dict(size=7, color=ui.PALETTE[0], opacity=0.45,
                            line=dict(width=0)),
                text=pts["suburb"],
                customdata=pts[["first_sale_price_aud", "last_sale_price_aud"]],
                hovertemplate=(
                    "%{text}<br>%{x:.1f} yrs held<br>"
                    "%{customdata[0]:$,.0f} → %{customdata[1]:$,.0f}"
                    "<br>%{y:.1f}% p.a.<extra></extra>"
                ),
                name="Property",
            )
        )
        if not annualised.empty:
            fig.add_hline(
                y=annualised.median(),
                line=dict(color=ui.PALETTE[1], width=2, dash="dash"),
                annotation_text=f"median {annualised.median():.1f}% p.a.",
                annotation_position="top right",
                annotation_font_size=11,
            )
        fig.update_xaxes(title="Years between first and last sale")
        fig.update_yaxes(title="Annualised growth", ticksuffix="%")
        st.plotly_chart(ui.style_fig(fig, 380), use_container_width=True)
        st.caption(
            "Short holds scatter widest — annualising a small window magnifies "
            "noise, which is the shape of the left-hand edge rather than a "
            "property of those markets."
        )

    # --- by LGA ------------------------------------------------------------
    with right:
        st.subheader("Median growth by LGA")
        by_lga = (
            view.dropna(subset=["annualised_growth_pct"])
            .groupby("local_government_area")
            .agg(median_growth=("annualised_growth_pct", "median"),
                 properties=("property_key", "count"))
            .sort_values("median_growth", ascending=True)
            .reset_index()
        )
        if by_lga.empty:
            st.info("Nothing to rank.")
        else:
            fig = go.Figure(
                go.Bar(
                    x=by_lga["median_growth"],
                    y=by_lga["local_government_area"],
                    orientation="h",
                    marker_color=ui.PALETTE[0],
                    text=[
                        f"{g:.1f}%  ({n} props)"
                        for g, n in zip(by_lga["median_growth"], by_lga["properties"])
                    ],
                    textposition="outside",
                    hovertemplate="%{y}<br>%{x:.2f}% p.a.<extra></extra>",
                )
            )
            # Span the actual data, not [0, max]: a hardcoded zero floor hides
            # any LGA with negative median growth entirely — the bar renders
            # outside the plotting area and the row reads as blank.
            lo = min(0.0, float(by_lga["median_growth"].min()) * 1.15)
            hi = float(by_lga["median_growth"].max()) * 1.45
            fig.update_xaxes(ticksuffix="%", range=[lo, hi])
            st.plotly_chart(ui.style_fig(fig, 380), use_container_width=True)
            st.caption(
                "Ranked by median growth. The property count beside each bar is "
                "the sample it rests on — read them together."
            )

    # --- the rate actually used --------------------------------------------
    with st.expander("The rates used to index comparables on the Valuation page"):
        st.dataframe(
            rates[[
                "local_government_area", "repeat_sale_pairs", "lga_median_growth_pct",
                "applied_growth_pct", "growth_rate_source", "is_reliable",
            ]].rename(columns={
                "local_government_area": "LGA",
                "repeat_sale_pairs": "Pairs",
                "lga_median_growth_pct": "Own rate %",
                "applied_growth_pct": "Applied %",
                "growth_rate_source": "Source",
                "is_reliable": "Reliable",
            }),
            hide_index=True,
            use_container_width=True,
        )
        st.caption(
            "An LGA below the reliability threshold borrows the national median "
            "rather than indexing everything by a rate drawn from a handful of "
            "properties."
        )

    # --- table -------------------------------------------------------------
    st.subheader("Property-level repeat sales")
    table = view.sort_values("annualised_growth_pct", ascending=False)[[
        "suburb", "local_government_area", "property_type", "bedrooms",
        "first_sold_date", "first_sale_price_aud",
        "last_sold_date", "last_sale_price_aud",
        "years_held", "total_growth_pct", "annualised_growth_pct", "sale_count",
    ]]
    st.dataframe(
        table,
        hide_index=True,
        use_container_width=True,
        height=380,
        column_config={
            "suburb": "Suburb",
            "local_government_area": "LGA",
            "property_type": "Type",
            "bedrooms": st.column_config.NumberColumn("Beds", width="small"),
            "first_sold_date": st.column_config.DateColumn("First sale", format="MMM YYYY"),
            "first_sale_price_aud": st.column_config.NumberColumn("First price", format="$%d"),
            "last_sold_date": st.column_config.DateColumn("Last sale", format="MMM YYYY"),
            "last_sale_price_aud": st.column_config.NumberColumn("Last price", format="$%d"),
            "years_held": st.column_config.NumberColumn("Years", format="%.1f"),
            "total_growth_pct": st.column_config.NumberColumn("Total", format="%.0f%%"),
            "annualised_growth_pct": st.column_config.NumberColumn("Per year", format="%.1f%%"),
            "sale_count": st.column_config.NumberColumn("Sales", width="small"),
        },
    )
    st.caption(
        "Only properties with an identified street address and a non-withheld "
        "price on both sales are included — a withheld address cannot be proven "
        "to be the same dwelling twice, so it can never form a pair."
    )

    ui.crawl_footer(coverage)
