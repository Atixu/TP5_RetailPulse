"""Configuration partagée, lue depuis les variables d'environnement.

Les mêmes noms sont utilisés dans docker-compose.yml et dans la ConfigMap Kubernetes.
"""
import logging
import os


def env(name, default=None, cast=str):
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Variable d'environnement manquante : {name}")
    return cast(value)


KAFKA_BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_PRODUCTS = env("TOPIC_PRODUCTS", "retail-products")
TOPIC_CUSTOMERS = env("TOPIC_CUSTOMERS", "retail-customers")
TOPIC_ORDERS = env("TOPIC_ORDERS", "retail-orders")
TOPIC_ORDER_EVENTS = env("TOPIC_ORDER_EVENTS", "retail-order-events")

# Nombre de partitions des topics d'événements (les référentiels en ont une seule).
EVENT_TOPIC_PARTITIONS = env("EVENT_TOPIC_PARTITIONS", "3", int)
REPLICATION_FACTOR = env("KAFKA_REPLICATION_FACTOR", "1", int)

POSTGRES_HOST = env("POSTGRES_HOST", "localhost")
POSTGRES_PORT = env("POSTGRES_PORT", "5432", int)
POSTGRES_DB = env("POSTGRES_DB", "retailpulse")
POSTGRES_USER = env("POSTGRES_USER", "retailpulse")
POSTGRES_PASSWORD = env("POSTGRES_PASSWORD", "")

CURRENCY = env("CURRENCY", "USD")


def setup_logging(name):
    logging.basicConfig(
        level=env("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    return logging.getLogger(name)
