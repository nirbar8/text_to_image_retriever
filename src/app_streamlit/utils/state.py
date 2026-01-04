from __future__ import annotations

from dataclasses import dataclass

import streamlit as st

from app_streamlit.settings import AppSettings
from retriever.clients.retriever import RetrieverClient
from retriever.clients.vectordb import VectorDBClient


@dataclass(frozen=True)
class AppContext:
    settings: AppSettings
    retriever: RetrieverClient
    vectordb: VectorDBClient


@st.cache_resource(show_spinner=False)
def get_context() -> AppContext:
    s = AppSettings()
    retriever = RetrieverClient(s.retriever_url, timeout_s=s.timeout_s)
    vectordb = VectorDBClient(s.vectordb_url, timeout_s=s.timeout_s)
    return AppContext(settings=s, retriever=retriever, vectordb=vectordb)
