import logging
import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType

from producer.kafka import MIN_ISR, PARTITIONS, REPLICAS, TOPIC, bootstrap_servers
from producer.registry import SUBJECT, register_schema


log = logging.getLogger(__name__)


def ensure_topic(admin: AdminClient):
    spec = NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=REPLICAS,
                    config={"min.insync.replicas": str(MIN_ISR)})
    try:
        admin.create_topics([spec], request_timeout=8)[TOPIC].result()
    except KafkaException as error:
        if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise

    topic = admin.list_topics(timeout=5).topics.get(TOPIC)
    if not topic or topic.error or not topic.partitions:
        raise RuntimeError("ждём метаданные топика")
    if len(topic.partitions) < PARTITIONS or any(len(p.replicas) != REPLICAS for p in topic.partitions.values()):
        raise ValueError(f"проверь партиции и число реплик у {TOPIC}")
    if any(p.leader < 0 or len(p.isrs) < MIN_ISR for p in topic.partitions.values()):
        raise RuntimeError("ждём синхронизацию реплик")

    resource = ConfigResource(ResourceType.TOPIC, TOPIC)
    settings = admin.describe_configs([resource], request_timeout=8)[resource].result()
    if int(settings["min.insync.replicas"].value) != MIN_ISR:
        raise ValueError(f"у {TOPIC} должно быть min.insync.replicas={MIN_ISR}")


def main():
    logging.basicConfig(level=logging.INFO)
    admin = AdminClient({"bootstrap.servers": bootstrap_servers(), "socket.timeout.ms": 8000})
    for attempt in range(8):
        try:
            ensure_topic(admin)
            schema = register_schema()
            log.info("готовы topic=%s subject=%s schema_id=%s version=%s",
                     TOPIC, SUBJECT, schema["id"], schema["version"])
            return
        except (KafkaException, OSError, RuntimeError) as error:
            if attempt == 7:
                raise
            delay = min(2**attempt, 8)
            log.warning("повтор через %s с: %s", delay, error)
            time.sleep(delay)


if __name__ == "__main__":
    main()
