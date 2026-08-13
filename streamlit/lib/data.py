"""Data access for the realestate2026 dashboard.

The whole gold layer is roughly 40k rows, so every mart is pulled into memory
once behind ``st.cache_data`` and filtered in pandas. That is a deliberate
choice rather than laziness: a query per interaction would put a two-second
warehouse round trip behind every filter change, and there is no volume here
that justifies it. If the crawl footprint grows by an order of magnitude, push
the filters back down into SQL.

Authentication goes through the Databricks SDK ``Config``, which resolves
credentials from the environment. Deployed, that is the app's service principal
via the auto-injected ``DATABRICKS_CLIENT_ID`` / ``DATABRICKS_CLIENT_SECRET``.
Locally it is whatever ``DATABRICKS_CONFIG_PROFILE`` points at. No token is ever
read from a file this module owns.
"""

from __future__ import annotations

import os
import threading
from decimal import Decimal

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

# No default. On Databricks Apps the catalog came from app.yaml; on Railway it
# comes from a service variable. Defaulting to the DEV catalog here is how a
# production deployment quietly serves development data after one missing
# variable, so an unset value is a startup error instead.
CATALOG = os.getenv("REALESTATE_CATALOG", "").strip()
GOLD = f"{CATALOG}.gold"

# Cache TTL. The pipeline runs nightly, so an hour is generous and still means a
# long-lived session picks up a fresh build without a restart.
_TTL_SECONDS = 3600


def missing_config() -> list[str]:
    """Required environment, checked before anything tries to connect.

    Runs at import so a misconfigured deploy shows one clear message rather than
    a Databricks SDK stack trace from four frames deep, which is what an unset
    host produces and which tells a reader nothing about what to fix.

    Authentication itself is left entirely to the SDK's Config(): it accepts a
    PAT (DATABRICKS_TOKEN) or an OAuth service principal (DATABRICKS_CLIENT_ID
    plus DATABRICKS_CLIENT_SECRET), and nothing here needs to know which is in
    use. That is why the app runs unchanged on Databricks Apps, on Railway and
    on a laptop.
    """
    missing = []
    if not CATALOG:
        missing.append("REALESTATE_CATALOG  (e.g. realestate_prod)")
    if not os.getenv("DATABRICKS_WAREHOUSE_ID", "").strip():
        missing.append("DATABRICKS_WAREHOUSE_ID")
    # Authentication is NOT enumerated here. Listing the env vars we happen to
    # know about rejected the profile-based local run the README documents —
    # DATABRICKS_CONFIG_PROFILE resolves host and credentials from
    # ~/.databrickscfg, so neither DATABRICKS_HOST nor a token need be set and
    # the app refused to start despite being perfectly configured.
    #
    # Config() is the authority on whether credentials resolve, across every
    # mechanism it supports — env vars, a profile, Databricks Apps injection,
    # Azure CLI, metadata service. So ask it, rather than reimplementing a
    # subset of its rules and getting them wrong.
    try:
        cfg = Config()
        if not cfg.host:
            missing.append("DATABRICKS_HOST  (or a profile via DATABRICKS_CONFIG_PROFILE)")
        cfg.authenticate()
    except Exception as exc:  # noqa: BLE001 - any failure means "cannot authenticate"
        missing.append(
            "Databricks credentials — Config() could not authenticate: "
            f"{str(exc).splitlines()[0][:160]}"
        )
    return missing


# databricks-sql-connector declares threadsafety = 1: the module is safe to
# share, a Connection is NOT. Streamlit serves every browser session on its own
# script-runner thread, so two users arriving on a cold cache would otherwise
# interleave on one Thrift transport. Serialising execution is the right trade
# here — every query is a cached whole-table read of a few thousand rows, so the
# lock is held for milliseconds and contention is bounded by the number of
# distinct marts, not by the number of users.
_CONN_LOCK = threading.RLock()


@st.cache_resource
def _connection():
    """One warehouse connection per app process.

    ``cache_resource`` rather than ``cache_data`` because a connection is not
    serialisable and must not be duplicated per session — the app runs on 2 vCPU
    and one connection per user would exhaust the warehouse well before it
    exhausted the app.
    """
    cfg = Config()
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "").strip()
    if not warehouse_id:
        raise RuntimeError(
            "DATABRICKS_WAREHOUSE_ID is not set. On Railway and Databricks Apps "
            "it is a service variable; locally, export it."
        )
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: cfg.authenticate,
    )


def _execute(sql_text: str, params: dict | None = None) -> pd.DataFrame:
    """Run a statement, reconnecting once if the cached session has died.

    A warehouse session does not live forever — the warehouse auto-stops, is
    restarted, or the token behind it is rotated. Without this the cached
    Connection stays cached while every query against it fails, and the app is
    permanently broken until someone notices. Railway will not save us either:
    /_stcore/health only proves Streamlit is serving, so it keeps returning 200
    and the ON_FAILURE restart policy never fires.

    One retry, not a loop: a dead session is fixed by reconnecting, and anything
    that survives a fresh connection is a real error that should surface rather
    than be retried into a timeout.
    """
    for attempt in (1, 2):
        try:
            with _CONN_LOCK:
                with _connection().cursor() as cur:
                    cur.execute(sql_text, params) if params else cur.execute(sql_text)
                    return _floats(cur.fetchall_arrow().to_pandas())
        except Exception:  # noqa: BLE001 - reconnect once, then let it raise
            if attempt == 2:
                raise
            # Drop the dead Connection so cache_resource builds a new one.
            _connection.clear()


def _floats(df: pd.DataFrame) -> pd.DataFrame:
    """Convert SQL DECIMAL columns to float.

    Arrow hands decimals back as ``decimal.Decimal`` in object-dtype columns.
    They format fine and compare fine, so the problem hides until something
    multiplies one by a Python float and raises TypeError — which is a runtime
    error on a page nobody exercised, not a load-time one. The marts publish a
    dozen decimal columns (growth rates, ratios, years held, variances), so this
    is fixed once here rather than cast at each of thirty call sites.

    Precision is not a concern: every one of these is a display measure already
    rounded to one or two places by the model.
    """
    for col in df.columns:
        if df[col].dtype == object:
            non_null = df[col].dropna()
            if len(non_null) and isinstance(non_null.iloc[0], Decimal):
                df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data(ttl=_TTL_SECONDS, show_spinner=False)
def query(sql_text: str) -> pd.DataFrame:
    """Run SQL and return a DataFrame.

    Callers pass fully-formed SQL with no user input interpolated — every call
    site in this module is a static SELECT against a known mart. Filtering
    happens in pandas, so nothing a user types ever reaches this string.
    """
    return _execute(sql_text)


# ---------------------------------------------------------------------------
# Marts. One loader each, all cached, all whole-table reads.
# ---------------------------------------------------------------------------


@st.cache_data(ttl=_TTL_SECONDS, show_spinner=False)
def scorecard() -> pd.DataFrame:
    """One row per LGA with listing data. The landing page's spine."""
    return query(f"select * from {GOLD}.mart_lga_scorecard order by sales_12m desc")


@st.cache_data(ttl=_TTL_SECONDS, show_spinner=False)
def price_trend() -> pd.DataFrame:
    """LGA x year x property type price distribution."""
    return query(
        f"select * from {GOLD}.mart_lga_price_trend order by lga_key, sold_year"
    )


@st.cache_data(ttl=_TTL_SECONDS, show_spinner=False)
def price_trend_all_types() -> pd.DataFrame:
    """LGA x year price distribution across ALL property types.

    A separate query rather than an aggregation of mart_lga_price_trend, because
    a median cannot be recovered by averaging medians. Combining the per-type
    rows weighted by sales would produce a number that looks like a median,
    tracks one loosely, and is not one — worse than either honest alternative.
    Same definitions as the mart so the two agree where they overlap.
    """
    return query(
        f"""
        select
            l.lga_key,
            g.local_government_area,
            year(s.sold_date)                                   as sold_year,
            count(*)                                            as sales,
            cast(percentile(s.sale_price_aud, 0.25) as bigint)  as q1_price_aud,
            cast(median(s.sale_price_aud) as bigint)            as median_price_aud,
            cast(percentile(s.sale_price_aud, 0.75) as bigint)  as q3_price_aud,
            -- Must match mart_lga_price_trend exactly: the crawl's year, not
            -- the wall clock. They diverge every January, when current_date()
            -- would shade a year the mart still calls complete and the two
            -- series would disagree about which bar is partial.
            --
            -- Sourced from mart_data_coverage rather than the silver view the
            -- mart itself reads: the app's service principal is granted SELECT
            -- on gold ONLY, so reaching into silver fails in production while
            -- working fine for a developer whose profile can see everything.
            -- Least privilege caught exactly that here.
            year(s.sold_date) >= (
                select max(year(last_crawled_on)) from {GOLD}.mart_data_coverage
            )                                                   as is_partial_year
        from {GOLD}.fct_sale s
        join {GOLD}.dim_location l on s.location_key = l.location_key
        join {GOLD}.dim_lga g      on l.lga_key = g.lga_key
        where s.sold_date is not null
          and s.sale_price_aud is not null
        group by 1, 2, 3
        order by 1, 3
        """
    )


@st.cache_data(ttl=_TTL_SECONDS, show_spinner=False)
def repeat_sales() -> pd.DataFrame:
    """One row per property that has sold twice or more."""
    return query(f"select * from {GOLD}.mart_repeat_sales")


@st.cache_data(ttl=_TTL_SECONDS, show_spinner=False)
def growth_rates() -> pd.DataFrame:
    """Per-LGA repeat-sales growth rate used to index comparables forward."""
    return query(f"select * from {GOLD}.mart_lga_growth_rate")


@st.cache_data(ttl=_TTL_SECONDS, show_spinner=False)
def valuation() -> pd.DataFrame:
    """One row per live for-sale offering, priced against indexed comparables."""
    return query(
        f"select * from {GOLD}.mart_listing_valuation order by local_government_area, suburb"
    )


@st.cache_data(ttl=_TTL_SECONDS, show_spinner=False)
def coverage() -> pd.DataFrame:
    """Per-LGA data quality and completeness."""
    return query(f"select * from {GOLD}.mart_data_coverage order by sales desc")


@st.cache_data(ttl=_TTL_SECONDS, show_spinner=False)
def sales_for_distribution() -> pd.DataFrame:
    """Sale-level prices for the box plot.

    The only place the app reads a fact table rather than a mart. A box plot
    needs the underlying distribution, and mart_lga_price_trend publishes
    quartiles per year — correct for a trend line, but it cannot be recombined
    into a single distribution across years without weighting by sales, which
    would be reconstructing the raw data badly rather than just reading it.
    Restricted to the recent window so the spread describes today's market.
    """
    return query(
        f"""
        select
            l.lga_key,
            g.local_government_area,
            t.property_type,
            s.bedrooms,
            s.sale_price_aud,
            s.sold_date
        from {GOLD}.fct_sale s
        join {GOLD}.dim_location l      on s.location_key = l.location_key
        join {GOLD}.dim_lga g           on l.lga_key = g.lga_key
        join {GOLD}.dim_property_type t on s.property_type_key = t.property_type_key
        where s.sold_date >= add_months(current_date(), -24)
          and s.sale_price_aud is not null
          and not s.is_price_withheld
        """
    )


@st.cache_data(ttl=_TTL_SECONDS, show_spinner=False)
def comparable_sales() -> pd.DataFrame:
    """Every sale that qualifies as a comparable, with its band and indexed value.

    Read whole and filtered in pandas like every other mart — ~2.5k rows.

    Nothing about WHICH sales qualify is decided here. The band, the date window
    and the indexation all come from mart_comparable_sale, which is also what
    mart_listing_valuation aggregates. An earlier version reimplemented the
    banding macro and hardcoded the 2024 cut in Python, which meant this table
    could silently list different sales than the verdict it exists to justify.
    """
    return query(f"select * from {GOLD}.mart_comparable_sale")


def comparables_for_listing(row) -> pd.DataFrame:
    """The comparables behind one listing's verdict.

    Takes the mart row rather than an id, because the cell is defined by
    (lga_key, property_type_key, comparable_band) and the mart already carries
    all three — so the match is a lookup, not a re-derivation.
    """
    if not row.get("comparable_band"):
        return pd.DataFrame()
    df = comparable_sales()
    return df[
        (df["lga_key"] == row["lga_key"])
        & (df["property_type_key"] == row["property_type_key"])
        & (df["comparable_band"] == row["comparable_band"])
    ].sort_values("sold_date", ascending=False)


@st.cache_data(ttl=_TTL_SECONDS, show_spinner=False)
def feature_premium() -> pd.DataFrame:
    """Median sold price with and without each common feature.

    Restricted to features with enough listings on both sides to mean anything.
    Pool is deliberately absent from the result rather than filtered in the UI:
    21 listings dataset-wide out of 6,609 properties, which is rural Victoria
    rather than a data problem.
    """
    return query(
        f"""
        with sold_features as (
            select
                s.sale_key,
                s.sale_price_aud,
                g.local_government_area,
                f.feature_name
            from {GOLD}.fct_sale s
            join {GOLD}.dim_location l on s.location_key = l.location_key
            join {GOLD}.dim_lga g      on l.lga_key = g.lga_key
            left join {GOLD}.bridge_listing_feature b on b.listing_id = s.listing_id
            left join {GOLD}.dim_feature f            on b.feature_key = f.feature_key
            where s.sale_price_aud is not null
              and not s.is_price_withheld
              and s.sold_date >= add_months(current_date(), -36)
        ),
        candidates as (
            select feature_name
            from sold_features
            where feature_name is not null
            group by feature_name
            having count(distinct sale_key) >= 40
        ),
        matrix as (
            select
                c.feature_name,
                s.sale_key,
                max(s.sale_price_aud) as sale_price_aud,
                max(case when sf.sale_key is not null then 1 else 0 end) as has_feature
            from candidates c
            cross join (select distinct sale_key, sale_price_aud from sold_features) s
            left join sold_features sf
              on sf.sale_key = s.sale_key and sf.feature_name = c.feature_name
            group by c.feature_name, s.sale_key
        )
        select
            feature_name,
            count_if(has_feature = 1)                                          as with_feature,
            count_if(has_feature = 0)                                          as without_feature,
            cast(median(case when has_feature = 1 then sale_price_aud end) as bigint) as median_with_aud,
            cast(median(case when has_feature = 0 then sale_price_aud end) as bigint) as median_without_aud
        from matrix
        group by feature_name
        having count_if(has_feature = 1) >= 40 and count_if(has_feature = 0) >= 40
        order by median_with_aud - median_without_aud desc
        """
    )


@st.cache_data(ttl=_TTL_SECONDS, show_spinner=False)
def agency_league() -> pd.DataFrame:
    """Listing volume by agency and LGA, off the agent bridge."""
    return query(
        f"""
        select
            a.agency_name,
            g.local_government_area,
            count(distinct s.listing_id) as listings
        from {GOLD}.fct_listing_snapshot s
        join {GOLD}.dim_location l on s.location_key = l.location_key
        join {GOLD}.dim_lga g      on l.lga_key = g.lga_key
        join {GOLD}.dim_agency a   on s.agency_key = a.agency_key
        where not a.is_unknown
        group by a.agency_name, g.local_government_area
        order by listings desc
        """
    )
