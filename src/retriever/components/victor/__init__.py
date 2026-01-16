"""Victor scheduler - publishes ready tiles to embedding queues."""

from retriever.components.victor.scheduler import VictorScheduler
from retriever.components.victor.settings import VictorSettings

__all__ = ["VictorScheduler", "VictorSettings"]