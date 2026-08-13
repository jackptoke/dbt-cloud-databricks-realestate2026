"""realestate2026 — investor dashboard.

Entry point. set_page_config must be the first Streamlit call in the process,
so nothing above it may import a module that touches st.
"""

from __future__ import annotations

import streamlit as st

st.set_page_config(
    page_title="Regional VIC Property Intelligence",
    page_icon=":material/home_work:",
    layout="wide",
    initial_sidebar_state="expanded",
)

from lib import data  # noqa: E402
from views import capital_growth, coverage, listing_valuation, market_explorer  # noqa: E402

PAGES = {
    "Market Explorer": (market_explorer.render, ":material/explore:"),
    "Capital Growth": (capital_growth.render, ":material/trending_up:"),
    "Listing Valuation": (listing_valuation.render, ":material/price_check:"),
    "Coverage & Caveats": (coverage.render, ":material/fact_check:"),
}


def main() -> None:
    # Fail loudly and specifically. A deploy missing one variable should say
    # which one, not surface an SDK authentication trace.
    missing = data.missing_config()
    if missing:
        st.error("This deployment is not configured. Missing environment:")
        st.code("\n".join(missing))
        st.caption(
            "Set these as service variables. REALESTATE_CATALOG has no default "
            "on purpose — a default would let a production deploy silently "
            "serve development data."
        )
        st.stop()

    with st.sidebar:
        st.markdown("### Regional VIC")
        st.caption("Property intelligence for individual investors")

    # The first page is the default and always answers at "/", so an explicit
    # url_path on it would 404. Only the rest get one.
    pages = []
    for i, (title, (fn, icon)) in enumerate(PAGES.items()):
        slug = title.lower().replace(" & ", "-").replace(" ", "-")
        pages.append(
            st.Page(fn, title=title, icon=icon, default=(i == 0))
            if i == 0
            else st.Page(fn, title=title, icon=icon, url_path=slug)
        )
    st.navigation(pages).run()


if __name__ == "__main__":
    main()
