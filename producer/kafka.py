import logging
import os
import time
from collections.abc import Iterable
from concurrent.futures import Future

from confluent_kafka import KafkaException, Producer

from producer.events import MovieEvent, encode_event
from producer.registry import registered_schema


log = logging.getLogger(__name__)
TOPIC = "movie-events"
PARTITIONS = 3
REPLICAS = 3
MIN_ISR = 2


def bootstrap_servers():
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")


class Publisher:
    def __init__(self, client: Producer, schema_id: int):
        self.client = client
        self.schema_id = schema_id

    @classmethod
    def connect(cls):
        schema = registered_schema()
        client = Producer({
            "bootstrap.servers": bootstrap_servers(),
            "enable.idempotence": True,
            "acks": "all",
            "retries": 20,
            "retry.backoff.ms": 200,
            "retry.backoff.max.ms": 4000,
            "delivery.timeout.ms": 30000,
            "socket.timeout.ms": 10000,
            "allow.auto.create.topics": False,
        })
        return cls(client, schema["id"])

    def enqueue(self, event: MovieEvent) -> Future:
        result = Future()
        payload = encode_event(event, self.schema_id)

        def delivered(error, message):
            if error is not None:
                log.error("ошибка записи event_id=%s error=%s", event.event_id, error)
                result.set_exception(KafkaException(error))
                return
            log.info("записано event_id=%s event_type=%s timestamp=%s",
                     event.event_id, event.event_type, event.timestamp.isoformat())
            result.set_result(None)

        deadline = time.monotonic() + 30
        delay = 0.05
        while True:
            try:
                self.client.produce(TOPIC, key=event.user_id.encode(), value=payload, on_delivery=delivered)
                return result
            except BufferError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("очередь продюсера заполнена") from None
                self.client.poll(delay)
                delay = min(2, delay * 2)

    def publish_batch(self, events: Iterable[MovieEvent]):
        results = [self.enqueue(event) for event in events]
        for result in results:
            while not result.done():
                self.client.poll(0.1)
            result.result()

    def ready(self):
        topic = self.client.list_topics(timeout=3).topics.get(TOPIC)
        return bool(topic and not topic.error and len(topic.partitions) >= PARTITIONS and all(
            p.leader >= 0 and len(p.isrs) >= MIN_ISR for p in topic.partitions.values()
        ))

    def close(self):
        self.client.flush(30)
