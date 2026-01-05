from __future__ import annotations

import json
from collections import deque
from typing import Deque, Iterable, Optional

import pika

from retriever.adapters.message_bus_rmq_config import RmqConfig
from retriever.core.interfaces import MessageBus, MessageEnvelope


class RabbitMQCallbackMessageBus(MessageBus):
    """Callback-style consumer using basic_consume + process_data_events."""

    def __init__(self, cfg: RmqConfig):
        self._cfg = cfg

    def _params(self) -> pika.ConnectionParameters:
        creds = pika.PlainCredentials(self._cfg.user, self._cfg.password)
        return pika.ConnectionParameters(
            host=self._cfg.host,
            port=self._cfg.port,
            credentials=creds,
            heartbeat=int(self._cfg.heartbeat_s),
            blocked_connection_timeout=float(self._cfg.blocked_connection_timeout_s),
        )

    def publish(self, queue: str, message: dict) -> None:
        connection = pika.BlockingConnection(self._params())
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=True)

        body = json.dumps(message).encode("utf-8")
        channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),
        )
        connection.close()

    def consume(self, queue: str) -> Iterable[Optional[MessageEnvelope]]:
        queues = [name.strip() for name in queue.split(",") if name.strip()]
        if not queues:
            raise ValueError("No queue names provided to consume().")
        connection = pika.BlockingConnection(self._params())
        channel = connection.channel()
        for q in queues:
            channel.queue_declare(queue=q, durable=True)
        prefetch = int(self._cfg.prefetch_count)
        if prefetch > 0:
            channel.basic_qos(prefetch_count=prefetch)

        pending: Deque[MessageEnvelope] = deque()

        def _on_message(_ch, method, _properties, body) -> None:
            payload = json.loads(body.decode("utf-8"))
            delivery_tag: Optional[int]
            try:
                delivery_tag = int(method.delivery_tag)
            except Exception:
                delivery_tag = None

            if self._cfg.ack_debug:
                print(f"[debug] recv delivery_tag={delivery_tag}")

            def _ack(delivery_tag: Optional[int] = delivery_tag) -> None:
                if not channel.is_open or not connection.is_open:
                    return
                try:
                    if delivery_tag is not None:
                        channel.basic_ack(delivery_tag=delivery_tag)
                        if self._cfg.ack_debug:
                            print(f"[debug] ack delivery_tag={delivery_tag}")
                except Exception:
                    return

            pending.append(MessageEnvelope(payload=payload, ack=_ack))

        for q in queues:
            channel.basic_consume(queue=q, on_message_callback=_on_message, auto_ack=False)

        try:
            while True:
                connection.process_data_events(time_limit=1.0)
                if pending:
                    while pending:
                        yield pending.popleft()
                else:
                    yield None
        finally:
            try:
                channel.stop_consuming()
            except Exception:
                pass
            try:
                connection.close()
            except Exception:
                pass
