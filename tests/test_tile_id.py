from retriever.core.tile_id import TileKey, canonical_tile_id, tile_id_hash


def test_tile_id_determinism() -> None:
    key = TileKey(source="sat", z=10, x=512, y=333)
    tid1 = canonical_tile_id(key)
    tid2 = canonical_tile_id(key)
    assert tid1 == tid2
    assert tile_id_hash(tid1) == tile_id_hash(tid2)
