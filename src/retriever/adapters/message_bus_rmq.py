from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from retriever.adapters.message_bus_rmq_callback import RabbitMQCallbackMessageBus
from retriever.adapters.message_bus_rmq_config import RmqConfig
from retriever.adapters.message_bus_rmq_polling import RabbitMQPollingMessageBus
from retriever.core.interfaces import MessageBus


@dataclass(frozen=True)
class RmqMessageBusFactory:
    default_style: str = "callback"

    def create(self, cfg: RmqConfig, style: Optional[str] = None) -> MessageBus:
        mode = (style or self.default_style).strip().lower()
        if mode in ("callback", "basic_consume"):
            return RabbitMQCallbackMessageBus(cfg)
        if mode in ("polling", "consume"):
            return RabbitMQPollingMessageBus(cfg)
        raise ValueError(f"Unknown RMQ consume style: {style}")
