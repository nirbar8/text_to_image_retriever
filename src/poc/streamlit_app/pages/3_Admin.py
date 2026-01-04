from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from poc.streamlit_app.utils.state import get_context


st.title("Admin")

ctx = get_context()
s = ctx.settings

table_name = st.text_input("Table name", value=s.table_name)

with st.expander("LanceDB predicate examples (copy/paste)", expanded=True):
    st.code(
        "\n".join(
            [
                "image_id = 123",
                "width >= 640 AND height >= 640",
                "lat IS NULL OR lon IS NULL",
                "image_id >= 1000 AND image_id < 2000",
                "(width < 200 AND height < 200) AND (lat IS NULL)",
            ]
        )
    )
    st.caption("Predicates are used in Admin preview/delete and as optional filters in Retrieval/Heatmap.")

c1, c2, c3 = st.columns(3)

with c1:
    st.subheader("Table info")

    if st.button("Refresh"):
        info = ctx.db.table_info(table_name)
        st.write(
            {
                "db_dir": str(info.db_dir),
                "table": info.table_name,
                "vector_col": info.vector_col,
                "vector_dim": info.vector_dim,
                "version": info.version,
                "rows": info.row_count,
            }
        )
        st.markdown("**Schema**")
        st.code(str(info.schema))

        if info.versions is not None:
            st.markdown("**Versions (first 10)**")
            st.write(info.versions[:10])

with c2:
    st.subheader("Preview rows")

    preview_where = st.text_input("Preview filter", value="")
    preview_limit = st.number_input("Limit", min_value=1, max_value=500, value=20, step=1)

    if st.button("Load preview"):
        rows = ctx.db.sample_rows(
            table_name=table_name,
            limit=int(preview_limit),
            where=preview_where if preview_where.strip() else None,
        )
        st.dataframe(pd.DataFrame(rows), use_container_width=True)

    st.divider()
    st.subheader("Duplicate check (small DB)")

    if st.button("Check duplicates by image_id"):
        rows = ctx.db.sample_rows(table_name=table_name, limit=300000, columns=["image_id"])
        df = pd.DataFrame(rows)
        vc = df["image_id"].value_counts()
        dups = vc[vc > 1].head(50)
        if len(dups) == 0:
            st.success("No duplicates found by image_id.")
        else:
            st.warning("Duplicates found (top 50):")
            st.dataframe(dups)

with c3:
    st.subheader("Delete rows")

    delete_where = st.text_input("Delete predicate", value="image_id = 0")
    confirm = st.checkbox("I understand this deletes data", value=False)

    if st.button("Delete", type="secondary", disabled=not confirm):
        res = ctx.db.delete_where(table_name=table_name, where=delete_where)
        st.success(res)

st.divider()
st.subheader("Export JSONL")

export_where = st.text_input("Export filter", value="")
export_max = st.number_input("Max rows (0 = no limit)", min_value=0, max_value=5_000_000, value=1000, step=100)
export_out = st.text_input("Output path", value="data/ui_export.jsonl")

if st.button("Export"):
    written = ctx.db.export_jsonl(
        table_name=table_name,
        out_path=Path(export_out),
        where=export_where if export_where.strip() else None,
        max_rows=int(export_max) if export_max > 0 else None,
    )
    st.success(f"Exported {written} rows to {export_out}")

with st.expander("Why row counts can look surprising"):
    st.markdown(
        """
Earlier you saw `rows_before` drop by 2 when deleting a single `image_id`.

That happens when the DB contains **duplicates** for the same `image_id`.
The updated consumer now enforces the invariant:
- exactly one row per `image_id` (by deleting existing rows for the batch before inserting).

If you re-index overlapping ranges, newer data replaces older data for that image_id.
        """.strip()
    )
