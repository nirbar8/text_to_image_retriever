from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import streamlit as st

from poc.config import Settings
from poc.lancedb_service import LanceDBService
from poc.pe_model import PECore


@dataclass(frozen=True)
class AppContext:
    settings: Settings
    db: LanceDBService
    model: PECore


@st.cache_resource(show_spinner=False)
def get_context() -> AppContext:
    s = Settings()
    db = LanceDBService(Path(s.lancedb_dir))
    model = PECore("PE-Core-B16-224")
    return AppContext(settings=s, db=db, model=model)
