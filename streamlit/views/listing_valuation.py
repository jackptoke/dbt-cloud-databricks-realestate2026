"""Listing Valuation — is this listing priced above its comparables?

The page that needs user input, and therefore the reason this is an app rather
than a dashboard.
"""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from lib import data, ui


def render() -> None:
    ui.page_header(
        "Listing Valuation",
        "Pick a listing and see what comparable properties actually sold for.",
    )

    val = data.valuation()
    coverage = data.coverage()

    if val.empty:
        st.warning("No live listings available.")
        return

    priced = val[val["has_verdict"]]

    # THE calibration guard. An asking price sits above sold prices as a matter
    # of course — vendors leave negotiating room, and live stock is a different
    # mix from sold stock. Measured across every verdict, the typical listing
    # asks this much above its indexed comparables. Reading a single listing's
    # variance against ZERO would call most of the market overpriced, which is
    # exactly the miscalibration the mart's time-indexation was built to remove;
    # re-introducing it here would undo that at the last step.
    typical_premium = float(priced["variance_from_comparable_pct"].median()) if not priced.empty else 0.0

    # --- selection ---------------------------------------------------------
    with st.sidebar:
        st.subheader("Choose a listing")
        lgas = ["All LGAs"] + sorted(val["local_government_area"].dropna().unique())
        f_lga = st.selectbox("LGA", options=lgas, index=0)
        pool = val if f_lga == "All LGAs" else val[val["local_government_area"] == f_lga]

        types = ["All types"] + sorted(pool["property_type"].dropna().unique())
        f_type = st.selectbox("Property type", options=types, index=0)
        if f_type != "All types":
            pool = pool[pool["property_type"] == f_type]

        only_verdict = st.toggle("Only listings with a verdict", value=True)
        if only_verdict:
            pool = pool[pool["has_verdict"]]

        if pool.empty:
            st.warning("Nothing matches these filters.")
            st.stop()

        pool = pool.sort_values(
            "variance_from_comparable_pct", ascending=False, na_position="last"
        )

        def _label(r):
            addr = f"{r['suburb']} · {r['property_type']}"
            if r["has_verdict"]:
                return f"{addr} · {r['variance_from_comparable_pct']:+.0f}%"
            return f"{addr} · no verdict"

        options = pool["listing_id"].tolist()
        labels = {r["listing_id"]: _label(r) for _, r in pool.iterrows()}
        listing_id = st.selectbox(
            f"{len(pool)} listings",
            options=options,
            format_func=lambda lid: labels[lid],
        )

    row = pool[pool["listing_id"] == listing_id].iloc[0]

    # --- header ------------------------------------------------------------
    left, right = st.columns([2, 1])
    with left:
        st.subheader(f"{row['suburb']}, {row['state_code']} {row['postcode']}")
        st.caption(
            f"{row['local_government_area']} · listing {row['listing_id']}"
            + (f" · {int(row['offer_listing_count'])} source listings merged"
               if row["offer_listing_count"] > 1 else "")
        )
        # Plain markdown rather than st.metric: "residential land" is wider
        # than a metric tile and renders as "residenti...".
        land = f"{ui.num(row['land_size_m2'])} m²" if row["land_size_m2"] else "—"
        band = row["comparable_band"] or "no band"
        st.markdown(
            f"**{row['property_type']}** &nbsp;·&nbsp; {ui.num(row['bedrooms'])} bed "
            f"&nbsp;·&nbsp; {ui.num(row['bathrooms'])} bath &nbsp;·&nbsp; {land}"
            f"<br><span style='color:{ui.MUTED};font-size:0.85em'>"
            f"compared against <code>{band}</code> in this LGA</span>",
            unsafe_allow_html=True,
        )

    with right:
        st.metric("Asking price", ui.aud(row["asking_price_aud"]))

    if not row["has_verdict"]:
        _no_verdict(row)
        ui.crawl_footer(coverage)
        return

    variance = float(row["variance_from_comparable_pct"])
    relative = variance - typical_premium

    # --- the verdict, stated relative to the typical premium ---------------
    if relative > 10:
        verdict_text = "Asking a bigger premium than most listings here"
        colour = ui.ABOVE
    elif relative < -10:
        verdict_text = "Asking less of a premium than most listings here"
        colour = ui.BELOW
    else:
        verdict_text = "Asking about the usual premium for this market"
        colour = ui.NEUTRAL

    st.markdown(
        f"<div style='padding:0.75rem 1rem;border-left:4px solid {colour};"
        f"background:rgba(128,128,128,0.08);border-radius:4px;margin:0.5rem 0 1rem'>"
        f"<strong>{verdict_text}</strong><br>"
        f"<span style='color:{ui.MUTED}'>"
        f"{variance:+.1f}% against comparables, where the typical listing in this "
        f"dataset asks {typical_premium:+.1f}%. That puts it {relative:+.1f} points "
        f"{'above' if relative >= 0 else 'below'} the norm.</span></div>",
        unsafe_allow_html=True,
    )

    m = st.columns(4)
    m[0].metric(
        "Comparable median",
        ui.aud(row["comparable_median_indexed_aud"]),
        help="Indexed forward to the crawl date. The raw figure is below.",
    )
    m[1].metric("Variance", ui.pct(variance, signed=True))
    m[2].metric(
        "vs typical listing",
        ui.pct(relative, signed=True),
        help=(
            "The number to actually judge on. Asking prices sit above sold "
            "prices everywhere; this is how this listing compares with that norm."
        ),
    )
    m[3].metric("Comparable sales", ui.num(row["comparable_sales"]))

    # --- indexation disclosure ---------------------------------------------
    raw = row["comparable_median_price_aud"]
    indexed = row["comparable_median_indexed_aud"]
    uplift = (indexed / raw - 1) * 100 if raw else 0
    match_desc = _band_phrase(row)
    st.caption(
        f"**How the comparable median was built.** {int(row['comparable_sales'])} "
        f"sales of {row['property_type']} {match_desc} in "
        f"{row['local_government_area']}, settled since 2024. Their raw median is "
        f"{ui.aud(raw)}; each sale was then indexed forward to the crawl date at "
        f"{row['applied_growth_pct']:.2f}% a year "
        f"({'this LGA’s own repeat-sales rate' if row['growth_rate_source'] == 'lga' else 'the national fallback rate'}), "
        f"giving {ui.aud(indexed)} — an uplift of {uplift:+.1f}% over a median "
        f"comparable age of {row['comparable_median_age_years']:.2f} years. "
        "Without that step a stale comparable would make every current listing "
        "look overpriced."
    )

    # --- distribution ------------------------------------------------------
    comps = data.comparables_for_listing(row)
    if not comps.empty:
        st.subheader("Where this listing sits")
        vals = comps["indexed_price_aud"].astype(float)
        fig = go.Figure()
        fig.add_trace(
            go.Histogram(
                x=vals,
                nbinsx=min(12, max(5, len(vals) // 2)),
                marker_color=ui.PALETTE[0],
                opacity=0.75,
                name="Comparable sales (indexed)",
            )
        )
        fig.add_vline(
            x=float(indexed),
            line=dict(color=ui.NEUTRAL, width=2, dash="dot"),
            annotation_text="comparable median",
            annotation_position="top left",
            annotation_font_size=11,
        )
        fig.add_vline(
            x=float(row["asking_price_aud"]),
            line=dict(color=ui.ABOVE, width=3),
            annotation_text="this listing",
            annotation_position="top right",
            annotation_font_size=11,
        )
        fig.update_xaxes(tickprefix="$", tickformat=",.0f")
        fig.update_yaxes(title="Sales")
        fig.update_layout(showlegend=False)
        st.plotly_chart(ui.style_fig(fig, 280), use_container_width=True)

        st.subheader("The comparable sales behind this")
        show = comps.copy()
        show["vs_asking_pct"] = (
            show["indexed_price_aud"] / float(row["asking_price_aud"]) - 1
        ) * 100
        st.dataframe(
            show[[
                "street_address", "suburb", "sold_date", "bedrooms", "land_size_m2",
                "sale_price_aud", "age_years", "indexed_price_aud", "vs_asking_pct",
            ]],
            hide_index=True,
            use_container_width=True,
            height=300,
            column_config={
                "street_address": "Address",
                "suburb": "Suburb",
                "sold_date": st.column_config.DateColumn("Sold", format="DD MMM YYYY"),
                "bedrooms": st.column_config.NumberColumn("Beds", width="small"),
                "land_size_m2": st.column_config.NumberColumn("Land m²"),
                "sale_price_aud": st.column_config.NumberColumn("Sold price", format="$%d"),
                "age_years": st.column_config.NumberColumn("Age (yrs)", format="%.2f"),
                "indexed_price_aud": st.column_config.NumberColumn("Indexed to today", format="$%d"),
                "vs_asking_pct": st.column_config.NumberColumn("vs asking", format="%.1f%%"),
            },
        )
        st.caption(
            "Both the raw sold price and its indexed value are shown, so the "
            "adjustment can be checked rather than taken on trust."
        )

    ui.crawl_footer(coverage)


def _band_phrase(row) -> str:
    """Describe the comparable cell in the terms that actually apply.

    Land bands on area and dwellings on bedrooms, so a single phrasing is wrong
    for one of them — "0 bedrooms" is a true but useless thing to tell someone
    looking at a vacant block.
    """
    band = row.get("comparable_band") or ""
    if band.startswith("land:"):
        return f"in the {band.replace('land:', '')} size band"
    if band.startswith("beds:"):
        return f"with {band.replace('beds:', '')} bedrooms"
    return "in this category"


def _no_verdict(row) -> None:
    """The deliberate non-answer."""
    reasons = {
        "no asking price": (
            "This listing does not advertise a price — it is marketed as "
            "contact-agent or by auction. There is nothing to compare."
        ),
        "no comparable sales": (
            f"No {row['property_type']} with {ui.num(row['bedrooms'])} bedrooms has "
            f"sold in {row['local_government_area']} since 2024. This combination "
            "simply does not turn over here."
        ),
        "no land size recorded": (
            "This is a land or rural listing with no recorded area. Land is "
            "compared by size band rather than bedroom count — every vacant "
            "block reports zero bedrooms — so without an area there is nothing "
            "to match it against."
        ),
        "too few comparable sales": (
            f"Only a handful of comparable sales exist for this combination in "
            f"{row['local_government_area']}, below the minimum of five. A median "
            "drawn from two or three sales is not a price."
        ),
    }
    st.warning(
        f"**No verdict for this listing.** "
        f"{reasons.get(row['no_verdict_reason'], row['no_verdict_reason'])}",
        icon=":material/help:",
    )
    st.caption(
        "The mart returns nothing rather than a confident-looking guess. "
        "Widening the comparable window or relaxing the bedroom match would "
        "produce a number here, but not a trustworthy one."
    )
