"""Outils Kafka partagés : création idempotente des topics et rapport de livraison."""
import logging
import time

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from common import config

log = logging.getLogger(__name__)


def topic_specs():
    """Topics RetailPulse et leurs paramètres (voir docs/architecture.md)."""
    compacted = {"cleanup.policy": "compact"}
    return [
        # Référentiels : une partition suffit (≈ 400 messages), compaction = dernier état par clé.
        NewTopic(config.TOPIC_PRODUCTS, num_partitions=1,
                 replication_factor=config.REPLICATION_FACTOR, config=compacted),
        NewTopic(config.TOPIC_CUSTOMERS, num_partitions=1,
                 replication_factor=config.REPLICATION_FACTOR, config=compacted),
        # Événements : clé order_id, partitions = unité de parallélisme de Spark.
        NewTopic(config.TOPIC_ORDERS, num_partitions=config.EVENT_TOPIC_PARTITIONS,
                 replication_factor=config.REPLICATION_FACTOR),
        NewTopic(config.TOPIC_ORDER_EVENTS, num_partitions=config.EVENT_TOPIC_PARTITIONS,
                 replication_factor=config.REPLICATION_FACTOR),
    ]


def wait_for_kafka(timeout_s=120):
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            admin.list_topics(timeout=5)
            return admin
        except KafkaException as exc:
            if time.monotonic() > deadline:
                raise
            log.info("Kafka indisponible (%s), nouvelle tentative…", exc)
            time.sleep(3)


def ensure_topics():
    """Crée les topics s'ils n'existent pas (même logique que le TP initial)."""
    admin = wait_for_kafka()
    futures = admin.create_topics(topic_specs())
    for topic, future in futures.items():
        try:
            future.result()
            log.info("Topic créé : %s", topic)
        except KafkaException as exc:
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                log.info("Topic déjà existant : %s", topic)
            else:
                raise


def make_producer(**overrides):
    settings = {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
    }
    settings.update(overrides)
    return Producer(settings)


class DeliveryTracker:
    """Callback de livraison qui compte les succès et les échecs."""

    def __init__(self):
        self.delivered = 0
        self.failed = 0

    def __call__(self, err, msg):
        if err is not None:
            self.failed += 1
            log.error("Échec de livraison (topic=%s, key=%s) : %s",
                      msg.topic(), msg.key(), err)
        else:
            self.delivered += 1
            log.debug("Livré : topic=%s partition=%s offset=%s",
                      msg.topic(), msg.partition(), msg.offset())
