"""Victor scheduler - periodically publishes ready tiles to embedding queues."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from retriever.adapters.message_bus_rmq import RmqMessageBusFactory
from retriever.adapters.message_bus_rmq_config import RmqConfig
from retriever.clients.tilesdb import TilesDBClient
from retriever.components.tiles_db.models import TileStatus
from retriever.components.victor.settings import VictorSettings
from retriever.core.interfaces import MessageBus

logger = logging.getLogger(__name__)

# Configuration parsing constants
_CONFIG_DELIMITER = ","
_MAPPING_SEPARATOR = "="
_MODEL_SEPARATOR = ":"

# Default values
_DEFAULT_QUEUE_NAME = "tiles.to_index"
_DEFAULT_PAGINATION_OFFSET = 0


@dataclass(frozen=True)
class EmbedderQueues:
    """Queue routing configuration for different embedder backends."""

    default_queue: str
    by_backend: Dict[str, str]
    by_backend_model: Dict[Tuple[str, str], str]

    def get_queue(self, embedder_model: Optional[str] = None) -> str:
        """Get queue for an embedder model.

        Args:
            embedder_model: Embedder model string (e.g., "pe_core", "clip:ViT-B-32")

        Returns:
            Queue name to publish to
        """
        if not embedder_model:
            return self.default_queue

        embedder_model = embedder_model.strip()
        if not embedder_model:
            return self.default_queue

        # Try backend:model format first (more specific)
        if _MODEL_SEPARATOR in embedder_model:
            backend, model = embedder_model.split(_MODEL_SEPARATOR, 1)
            queue = self.by_backend_model.get((backend.strip(), model.strip()))
            if queue:
                return queue

        # Try just backend (fallback to less specific)
        queue = self.by_backend.get(embedder_model)
        if queue:
            return queue

        # No mapping found, use default
        logger.warning(
            "No queue mapping found for embedder_model='%s', using default queue='%s'",
            embedder_model,
            self.default_queue,
        )
        return self.default_queue


def _parse_embedder_queues(raw: str) -> EmbedderQueues:
    """Parse embedder queue configuration string.

    Format: backend1=queue1,backend2:model2=queue2
    Example: pe_core=tiles.to_index.pe_core,clip:ViT-B-32=tiles.to_index.clip

    Args:
        raw: Configuration string with comma-separated mappings

    Returns:
        EmbedderQueues configuration object

    Raises:
        ValueError: If no valid queue mappings are found
    """
    by_backend: Dict[str, str] = {}
    by_backend_model: Dict[Tuple[str, str], str] = {}
    default_queue = _DEFAULT_QUEUE_NAME

    for mapping in raw.split(_CONFIG_DELIMITER):
        mapping = mapping.strip()
        if not mapping:
            continue

        if _MAPPING_SEPARATOR not in mapping:
            logger.warning("Invalid queue mapping (missing '%s'): %s", _MAPPING_SEPARATOR, mapping)
            continue

        backend_spec, queue = mapping.split(_MAPPING_SEPARATOR, 1)
        backend_spec = backend_spec.strip()
        queue = queue.strip()

        if not backend_spec or not queue:
            logger.warning("Invalid queue mapping (empty backend or queue): %s", mapping)
            continue

        if _MODEL_SEPARATOR in backend_spec:
            # Backend:model format (more specific)
            parts = backend_spec.split(_MODEL_SEPARATOR, 1)
            backend = parts[0].strip()
            model = parts[1].strip()
            
            if not backend or not model:
                logger.warning("Invalid backend:model format: %s", backend_spec)
                continue
                
            by_backend_model[(backend, model)] = queue
        else:
            # Just backend (less specific)
            by_backend[backend_spec] = queue
            # First backend queue becomes default
            if len(by_backend) == 1:
                default_queue = queue

    if not by_backend and not by_backend_model:
        raise ValueError(f"No valid queue mappings found in configuration: {raw}")

    return EmbedderQueues(
        default_queue=default_queue,
        by_backend=by_backend,
        by_backend_model=by_backend_model,
    )


class VictorScheduler:
    """Scheduler that publishes ready tiles to embedding queues."""

    def __init__(self, settings: VictorSettings):
        """Initialize the scheduler.

        Args:
            settings: Victor configuration settings
        """
        self.settings = settings
        self.tilesdb_client = TilesDBClient(settings.tilesdb_url)

        # Initialize message bus
        rmq_config = RmqConfig(
            host=settings.rmq_host,
            port=settings.rmq_port,
            user=settings.rmq_user,
            password=settings.rmq_pass,
        )
        factory = RmqMessageBusFactory()
        self.message_bus: MessageBus = factory.create(rmq_config, style="polling")

        # Parse queue configuration
        self.queue_config = _parse_embedder_queues(settings.embedder_queues)

        logger.info("Victor Scheduler initialized")
        logger.info("  TilesDB: %s", settings.tilesdb_url)
        logger.info("  RabbitMQ: %s:%s", settings.rmq_host, settings.rmq_port)
        logger.info("  Interval: %ss", settings.schedule_interval_seconds)
        logger.info("  Batch size: %s", settings.batch_size)
        logger.info("  Default queue: %s", self.queue_config.default_queue)

    def run_once(self) -> int:
        """Run a single scheduler iteration.

        Fetches tiles ready for indexing, updates their status to IN_PROCESS,
        and publishes them to the appropriate RabbitMQ queues.

        Returns:
            Number of tiles successfully published
        """
        try:
            logger.debug("Starting scheduler iteration")

            ready_tiles = self._fetch_ready_tiles()
            if not ready_tiles:
                logger.debug("No tiles ready for indexing")
                return 0

            logger.info(
                "Found %d tiles ready for indexing (batch_size=%d)",
                len(ready_tiles),
                self.settings.batch_size,
            )

            published_count = self._process_tiles(ready_tiles)
            logger.info("Published %d tiles", published_count)
            return published_count

        except Exception as e:
            logger.error("Scheduler iteration failed: %s", e, exc_info=True)
            return 0

    def _fetch_ready_tiles(self) -> list:
        """Fetch tiles that are ready for indexing from TilesDB.

        Returns:
            List of TileResponse objects with status READY_FOR_INDEXING
        """
        response = self.tilesdb_client.list_tiles(
            status=TileStatus.READY_FOR_INDEXING,
            limit=self.settings.batch_size,
            offset=_DEFAULT_PAGINATION_OFFSET,
        )
        return response.tiles

    def _process_tiles(self, tiles: list) -> int:
        """Process a batch of tiles: update status and publish to queues.

        Args:
            tiles: List of TileResponse objects to process

        Returns:
            Number of tiles successfully published
        """
        published_count = 0

        for tile in tiles:
            try:
                self._process_single_tile(tile)
                published_count += 1
            except Exception as e:
                logger.error("Failed to process tile_id='%s': %s", tile.tile_id, e)
                self._mark_tile_failed(tile.tile_id)

        return published_count

    def _process_single_tile(self, tile) -> None:
        """Process a single tile: update status and publish to queue.

        Args:
            tile: TileResponse object to process

        Raises:
            Exception: If tile processing fails
        """
        queue = self.queue_config.get_queue(tile.embedder_model)

        # Update status to in_process before publishing
        self.tilesdb_client.update_status(tile.tile_id, TileStatus.IN_PROCESS)

        # Publish tile to queue (schema auto-synced via model_dump)
        tile_message = tile.model_dump(mode="json")
        self.message_bus.publish(queue, tile_message)
        
        logger.debug("Published tile_id='%s' to queue='%s'", tile.tile_id, queue)

    def _mark_tile_failed(self, tile_id: str) -> None:
        """Mark a tile as failed in TilesDB.

        Args:
            tile_id: ID of the tile to mark as failed
        """
        try:
            self.tilesdb_client.update_status(tile_id, TileStatus.FAILED)
        except Exception as ex:
            logger.error("Failed to mark tile_id='%s' as FAILED: %s", tile_id, ex)
