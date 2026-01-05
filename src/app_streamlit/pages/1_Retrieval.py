from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import streamlit as st
import httpx

from app_streamlit.ui.components import render_grid
from app_streamlit.utils.images import dedup_hits_by_polygon, filter_hits_by_polygon, load_hit_image
from app_streamlit.utils.state import get_context


def score_generic(distance: Optional[float]) -> Optional[float]:
    if distance is None:
        return None
    d = float(distance)
    return float(1.0 / (1.0 + d))


def _maybe_filter_by_polygon(
    hits: List[Dict[str, Any]],
    query_wkt: str,
    mode: str,
) -> List[Dict[str, Any]]:
    if not query_wkt.strip():
        return hits
    return filter_hits_by_polygon(hits, query_wkt=query_wkt, mode=mode)


st.title("Retrieval")

ctx = get_context()
s = ctx.settings

left, right = st.columns([2, 1])

with left:
    query = st.text_input("Text query", value="a dog")
    k = st.slider("Top K (displayed)", min_value=1, max_value=50, value=15, step=1)
    cols = st.slider("Grid columns", min_value=2, max_value=8, value=5, step=1)

with right:
    st.markdown("### Data source")
    table_name = st.text_input("Table name", value=s.table_name)
    where = st.text_input("Optional filter (Lance predicate)", value="")

    st.markdown("### Post-processing")
    remove_semidups = st.toggle("Remove semi-duplicates", value=False)
    filter_wkt = st.text_input("Filter by pixel polygon (WKT)", value="")
    filter_mode = st.selectbox("Polygon filter mode", ["intersects", "within"])
    retrieval_k = st.slider(
        "Internal retrieval K",
        min_value=max(k, 20),
        max_value=300,
        value=min(120, 300),
        step=10,
        disabled=not remove_semidups,
    )

    st.markdown("### Debug")
    enable_diag = st.toggle("Enable distance diagnostics", value=True)
    diag_n = st.slider("Diagnostics N (fetch embeddings)", 1, 30, 10, 1, disabled=not enable_diag)

run = st.button("Search", type="primary")

if run:
    try:
        with st.spinner("Searching..."):
            hits = ctx.retriever.search(
                query_text=query,
                table_name=table_name,
                k=int(retrieval_k) if remove_semidups else int(k),
                where=where if where.strip() else None,
            )
    except httpx.ConnectError:
        st.error(
            "Retriever service is not reachable. Start it with `uv run retriever-service` "
            f"and verify APP_RETRIEVER_URL ({s.retriever_url})."
        )
        st.stop()
    except httpx.HTTPError as exc:
        st.error(f"Retriever request failed: {exc}")
        st.stop()

    for h in hits:
        h["score_01"] = score_generic(h.get("_distance"))

    hits = _maybe_filter_by_polygon(hits, query_wkt=filter_wkt, mode=filter_mode)

    if remove_semidups:
        hits = dedup_hits_by_polygon(hits)
        hits = hits[: int(k)]
    else:
        hits = hits[: int(k)]

    images: List[Any] = []
    valid_hits: List[Dict[str, Any]] = []
    missing = 0

    for h in hits:
        try:
            images.append(load_hit_image(h))
            valid_hits.append(h)
        except Exception:
            missing += 1

    if missing:
        st.warning(f"{missing} results had missing or unreadable image files.")

    render_grid(images, valid_hits, cols=int(cols))

    if enable_diag:
        with st.spinner("Computing diagnostics (fetching embeddings for top hits)..."):
            diag_rows: List[Dict[str, Any]] = []
            for h in valid_hits[: int(diag_n)]:
                image_id = int(h["image_id"])
                rows = ctx.vectordb.sample_rows(
                    table_name=table_name,
                    where=f"image_id = {image_id}",
                    limit=1,
                    columns=["image_id", "embedding"],
                )
                if not rows:
                    continue
                v = np.asarray(rows[0]["embedding"], dtype=np.float32)
                v = v / (np.linalg.norm(v) + 1e-12)

                diag_rows.append(
                    {
                        "image_id": image_id,
                        "_distance(lance)": float(h.get("_distance")),
                        "embedding_norm": float(np.linalg.norm(v)),
                    }
                )

            if diag_rows:
                df = pd.DataFrame(diag_rows)
                st.subheader("Distance diagnostics")
                st.dataframe(df, use_container_width=True)
                st.caption("Embedding norm sanity check for a few top hits.")
            else:
                st.info("No diagnostics rows available (could not fetch embeddings).")
