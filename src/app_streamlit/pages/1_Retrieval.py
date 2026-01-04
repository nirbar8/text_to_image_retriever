from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import streamlit as st

from app_streamlit.ui.components import render_grid
from app_streamlit.utils.images import load_image
from app_streamlit.utils.state import get_context


def score_generic(distance: Optional[float]) -> Optional[float]:
    if distance is None:
        return None
    d = float(distance)
    return float(1.0 / (1.0 + d))


def simple_semidup_filter(
    hits: List[Dict[str, Any]],
    key_fields: Tuple[str, ...] = ("image_id", "tile_id"),
) -> List[Dict[str, Any]]:
    seen: Set[Tuple[Any, ...]] = set()
    kept: List[Dict[str, Any]] = []
    for h in hits:
        key = tuple(h.get(k) for k in key_fields)
        if key in seen:
            continue
        seen.add(key)
        kept.append(h)
    return kept


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
    with st.spinner("Searching..."):
        hits = ctx.retriever.search(
            query_text=query,
            table_name=table_name,
            k=int(retrieval_k) if remove_semidups else int(k),
            where=where if where.strip() else None,
        )

    for h in hits:
        h["score_01"] = score_generic(h.get("_distance"))

    if remove_semidups:
        hits = simple_semidup_filter(hits)
        hits = hits[: int(k)]
    else:
        hits = hits[: int(k)]

    images: List[Any] = []
    valid_hits: List[Dict[str, Any]] = []
    missing = 0

    for h in hits:
        image_path = h.get("image_path")
        if not image_path:
            missing += 1
            continue
        try:
            images.append(load_image(image_path))
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
