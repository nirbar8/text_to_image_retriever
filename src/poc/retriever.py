from .config import Settings
from .pe_model import PECore
from .lancedb_store import LanceCfg, get_table, search

def run(query: str = "a dog", k: int = 5) -> None:
    s = Settings()

    model = PECore("PE-Core-B16-224")
    table = get_table(LanceCfg(s.lancedb_dir, s.table_name), embedding_dim=model.embed_dim)

    q = model.embed_texts([query])[0].tolist()
    hits = search(table, q, k=k)

    for i, h in enumerate(hits, 1):
        print(f"\n#{i} score={h.get('_distance')}")
        print("image_path:", h["image_path"])
        print("image_id:", h["image_id"], "size:", (h["width"], h["height"]))
        print("tile_id:", h.get("tile_id"), "lat/lon:", (h.get("lat"), h.get("lon")))

if __name__ == "__main__":
    run()
