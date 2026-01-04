from __future__ import annotations

from typing import Any, Dict, List

import streamlit as st
from PIL import Image

from retriever.core.schemas import VECTOR_METADATA_COLUMNS


def render_hit_card(idx: int, image: Image.Image, hit: Dict[str, Any]) -> None:
    st.image(image, width="stretch")

    score01 = hit.get("score_01")
    dist = hit.get("_distance")

    st.markdown(f"**#{idx}**")
    st.markdown(f"score_01: `{score01}`  \n_distance: `{dist}`")

    meta = {k: hit.get(k) for k in VECTOR_METADATA_COLUMNS}
    st.json(meta, expanded=False)


def render_grid(images: List[Image.Image], hits: List[Dict[str, Any]], cols: int = 5) -> None:
    n = len(images)
    if n == 0:
        st.info("No results.")
        return

    cols = max(1, cols)
    rows = (n + cols - 1) // cols
    k = 0
    for _ in range(rows):
        c = st.columns(cols)
        for j in range(cols):
            if k >= n:
                break
            with c[j]:
                render_hit_card(k + 1, images[k], hits[k])
            k += 1
