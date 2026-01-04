from __future__ import annotations

import streamlit as st

st.set_page_config(
    page_title="PE Retrieval Console",
    page_icon="🔎",
    layout="wide",
)

st.title("PE-Core Retrieval Console")
st.caption("Search, visualize, and administer your LanceDB image embedding store.")

st.markdown(
    """
Use the pages on the left:
- **Retrieval**: text query, topK results, view images and metadata.
- **Heatmap**: simulated geo view (lat/lon) for infra testing.
- **Admin**: inspect schema, sample rows, delete by predicate, export.
"""
)
