from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable

import pika

from retriever.core.interfaces import MessageBus, MessageEnvelope


@dataclass(frozen=True)
class RmqConfig:
    host: str
    port: int
    user: str
    password: str


class RabbitMQMessageBus(MessageBus):
    def __init__(self, cfg: RmqConfig):
        self._cfg = cfg

    def _params(self) -> pika.ConnectionParameters:
        creds = pika.PlainCredentials(self._cfg.user, self._cfg.password)
        return pika.ConnectionParameters(host=self._cfg.host, port=self._cfg.port, credentials=creds)

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

    def consume(self, queue: str) -> Iterable[MessageEnvelope]:
        connection = pika.BlockingConnection(self._params())
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_qos(prefetch_count=256)

        try:
            for method, properties, body in channel.consume(queue, inactivity_timeout=1.0):
                if method is None:
                    yield MessageEnvelope(payload={"_idle": True}, ack=lambda: None)
                    continue
                payload = json.loads(body.decode("utf-8"))

                def _ack() -> None:
                    channel.basic_ack(delivery_tag=method.delivery_tag)

                yield MessageEnvelope(payload=payload, ack=_ack)
        finally:
            try:
                channel.cancel()
            except Exception:
                pass
            try:
                connection.close()
            except Exception:
                pass
