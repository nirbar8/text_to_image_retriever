from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
import pydeck as pdk
import streamlit as st

from app_streamlit.utils.geo import simulate_lat_lon
from app_streamlit.utils.state import get_context


st.title("Heatmap (query-driven)")

ctx = get_context()
s = ctx.settings

table_name = st.text_input("Table name", value=s.table_name)

query = st.text_input("Text query", value="a dog")
topk = st.slider("Top K (points)", min_value=100, max_value=10000, value=3000, step=100)

where = st.text_input("Optional filter (Lance predicate)", value="")

st.caption("The map shows the topK retrieval results. If lat/lon are missing, we generate deterministic simulated coordinates from image_id.")

run = st.button("Run query and visualize", type="primary")

if run:
    with st.spinner("Searching..."):
        rows = ctx.retriever.search(
            query_text=query,
            table_name=table_name,
            k=int(topk),
            where=where if where.strip() else None,
            columns=["image_id", "lat", "lon"],
        )

    points: List[Dict[str, Any]] = []
    for r in rows:
        image_id = int(r["image_id"])
        lat = r.get("lat")
        lon = r.get("lon")
        if lat is None or lon is None:
            lat, lon = simulate_lat_lon(image_id)
        points.append({"image_id": image_id, "lat": float(lat), "lon": float(lon)})

    df = pd.DataFrame(points)
    st.write(df.head())

    layer = pdk.Layer(
        "HexagonLayer",
        data=df,
        get_position=["lon", "lat"],
        radius=150,
        elevation_scale=4,
        elevation_range=[0, 3000],
        pickable=True,
        extruded=True,
    )

    view_state = pdk.ViewState(
        latitude=float(df["lat"].mean()),
        longitude=float(df["lon"].mean()),
        zoom=10,
        pitch=45,
    )

    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state))
