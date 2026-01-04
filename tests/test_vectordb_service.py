from fastapi.testclient import TestClient

from retriever.services.vectordb.app import create_app
from retriever.services.vectordb.settings import VectorDBSettings


def test_vectordb_health_and_tables(tmp_path) -> None:
    settings = VectorDBSettings(db_dir=tmp_path / "lancedb")
    app = create_app(settings)
    client = TestClient(app)

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    tables = client.get("/tables")
    assert tables.status_code == 200
    assert "tables" in tables.json()
