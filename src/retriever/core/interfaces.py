from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, List, Optional, Protocol, Sequence


@dataclass(frozen=True)
class MessageEnvelope:
    payload: dict
    ack: Callable[[], None]


if TYPE_CHECKING:
    from PIL import Image


class TileStore(Protocol):
    def get_tile_image(self, request) -> "Image.Image":
        """Return a PIL image for the requested tile."""
        ...


class TilesRepository(Protocol):
    def upsert_tiles(self, tiles: Sequence[dict]) -> None:
        ...

    def list_tiles(self, limit: int = 1000, status: Optional[str] = None) -> List[dict]:
        ...

    def update_status(self, tile_ids: Sequence[str], status: str) -> None:
        ...

    def delete_tiles(self, tile_ids: Sequence[str]) -> None:
        ...


class MessageBus(Protocol):
    def publish(self, queue: str, message: dict) -> None:
        ...

    def consume(self, queue: str) -> Iterable[MessageEnvelope]:
        ...


class VectorIndexClient(Protocol):
    def upsert(self, table_name: str, rows: List[dict]) -> int:
        ...


class VectorQueryClient(Protocol):
    def query(
        self,
        table_name: str,
        query_vector: Sequence[float],
        k: int,
        where: Optional[str] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> List[dict]:
        ...

    def sample_rows(
        self,
        table_name: str,
        where: Optional[str] = None,
        limit: int = 10,
        columns: Optional[Sequence[str]] = None,
    ) -> List[dict]:
        ...

    def table_info(self, table_name: str) -> dict:
        ...

    def list_tables(self) -> List[str]:
        ...

    def delete_where(self, table_name: str, where: str) -> dict:
        ...
