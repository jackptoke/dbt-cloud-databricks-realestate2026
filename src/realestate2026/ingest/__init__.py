"""API ingestion: realty-in-au -> NDJSON in the Unity Catalog landing volume.

Ported from the Airflow DAGs at realestate/astro/. The vendor payload handling
(realty_au.py), retry policy (http.py) and watched suburbs (suburbs.py) carry
over unchanged; landing.py was rewritten against UC volumes, and fetch_suburb.py
replaces the three-task dynamic mapping with a single per-suburb entry point.
"""
