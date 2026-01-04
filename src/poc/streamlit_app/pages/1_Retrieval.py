from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import streamlit as st

from poc.streamlit_app.ui.components import render_grid
from poc.streamlit_app.utils.images import load_image
from poc.streamlit_app.utils.state import get_context


def normalize_vec(v: np.ndarray) -> np.ndarray:
    n = float(np.linalg.norm(v) + 1e-12)
    return v / n


def score_generic(distance: Optional[float]) -> Optional[float]:
    if distance is None:
        return None
    d = float(distance)
    return float(1.0 / (1.0 + d))


def simple_semidup_filter(
    hits: List[Dict[str, Any]],
    key_fields: Tuple[str, ...] = ("image_id", "coco_file_name"),
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
    with st.spinner("Embedding query and searching..."):
        qvec = ctx.model.embed_texts([query])[0].tolist()
        q = normalize_vec(np.asarray(qvec, dtype=np.float32))
        qvec_normed = q.tolist()

        hits = ctx.db.vector_search(
            table_name=table_name,
            query_vec=qvec_normed,
            k=int(retrieval_k) if remove_semidups else int(k),
            where=where if where.strip() else None,
        )

    # Always show a stable 0-1 score for now
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
        try:
            images.append(load_image(h["image_path"]))
            valid_hits.append(h)
        except Exception:
            missing += 1

    if missing:
        st.warning(f"{missing} results had missing or unreadable image files.")

    render_grid(images, valid_hits, cols=int(cols))

    if enable_diag:
        with st.spinner("Computing diagnostics (fetching embeddings for top hits)..."):
            # Re-query for embeddings for top diag_n by image_id
            # We do a small per-row fetch to keep it simple and robust.
            diag_rows: List[Dict[str, Any]] = []
            for h in valid_hits[: int(diag_n)]:
                image_id = int(h["image_id"])
                rows = ctx.db.sample_rows(
                    table_name=table_name,
                    where=f"image_id = {image_id}",
                    limit=1,
                    columns=["image_id", "embedding"],
                )
                if not rows:
                    continue
                v = np.asarray(rows[0]["embedding"], dtype=np.float32)
                v = normalize_vec(v)

                cos = float(np.dot(q, v))
                l2 = float(np.linalg.norm(q - v))
                cos_dist = float(1.0 - cos)
                l2_from_cos = float(np.sqrt(max(0.0, 2.0 - 2.0 * cos)))

                diag_rows.append(
                    {
                        "image_id": image_id,
                        "_distance(lance)": float(h.get("_distance")),
                        "cos_sim(q·v)": cos,
                        "cos_dist(1-cos)": cos_dist,
                        "l2(||q-v||)": l2,
                        "l2_from_cos": l2_from_cos,
                    }
                )

            if diag_rows:
                df = pd.DataFrame(diag_rows)
                st.subheader("Distance diagnostics")
                st.dataframe(df, use_container_width=True)
                st.caption(
                    "Compare `_distance(lance)` to `l2(||q-v||)` and `cos_dist(1-cos)` to infer what Lance is returning."
                )
            else:
                st.info("No diagnostics rows available (could not fetch embeddings).")
