from __future__ import annotations

import json
import time
from typing import Iterable, Optional

import pika

from retriever.adapters.message_bus_rmq_config import RmqConfig
from retriever.core.interfaces import MessageBus, MessageEnvelope


class RabbitMQPollingMessageBus(MessageBus):
    """Polling-style consumer using channel.consume()."""

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

        try:
            if len(queues) == 1:
                for method, _properties, body in channel.consume(queues[0], inactivity_timeout=1.0):
                    if method is None:
                        yield None
                        continue
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

                    def _nack(
                        requeue: bool = True,
                        delivery_tag: Optional[int] = delivery_tag,
                    ) -> None:
                        if not channel.is_open or not connection.is_open:
                            return
                        try:
                            if delivery_tag is not None:
                                channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
                                if self._cfg.ack_debug:
                                    print(
                                        f"[debug] nack delivery_tag={delivery_tag} requeue={requeue}"
                                    )
                        except Exception:
                            return

                    yield MessageEnvelope(payload=payload, ack=_ack, nack=_nack)
            else:
                while True:
                    got_message = False
                    for q in queues:
                        method, _properties, body = channel.basic_get(queue=q, auto_ack=False)
                        if method is None:
                            continue
                        got_message = True
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

                        def _nack(
                            requeue: bool = True,
                            delivery_tag: Optional[int] = delivery_tag,
                        ) -> None:
                            if not channel.is_open or not connection.is_open:
                                return
                            try:
                                if delivery_tag is not None:
                                    channel.basic_nack(
                                        delivery_tag=delivery_tag,
                                        requeue=requeue,
                                    )
                                    if self._cfg.ack_debug:
                                        print(
                                            f"[debug] nack delivery_tag={delivery_tag} "
                                            f"requeue={requeue}"
                                        )
                            except Exception:
                                return

                        yield MessageEnvelope(payload=payload, ack=_ack, nack=_nack)
                    if not got_message:
                        time.sleep(1.0)
                        yield None
        finally:
            try:
                channel.cancel()
            except Exception:
                pass
            try:
                connection.close()
            except Exception:
                pass
