import json
import pika
from dataclasses import dataclass

@dataclass(frozen=True)
class RmqConn:
    host: str
    port: int
    user: str
    password: str
    queue_name: str

def _params(cfg: RmqConn) -> pika.ConnectionParameters:
    creds = pika.PlainCredentials(cfg.user, cfg.password)
    return pika.ConnectionParameters(host=cfg.host, port=cfg.port, credentials=creds)

def publish_json(cfg: RmqConn, message: dict) -> None:
    connection = pika.BlockingConnection(_params(cfg))
    channel = connection.channel()
    channel.queue_declare(queue=cfg.queue_name, durable=True)

    body = json.dumps(message).encode("utf-8")
    channel.basic_publish(
        exchange="",
        routing_key=cfg.queue_name,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),  # persistent
    )
    connection.close()

def consume(cfg: RmqConn):
    connection = pika.BlockingConnection(_params(cfg))
    channel = connection.channel()
    channel.queue_declare(queue=cfg.queue_name, durable=True)
    channel.basic_qos(prefetch_count=256)
    return connection, channel
