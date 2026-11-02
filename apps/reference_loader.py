"""Reference Loader : Kafka (retail-products, retail-customers) -> PostgreSQL.

Rôle      : rendre les référentiels disponibles pour le producteur et l'enrichissement Spark.
Garantie  : at-least-once. L'offset Kafka est validé APRÈS le COMMIT PostgreSQL ;
            un rejeu est sans effet grâce à l'upsert sur l'identifiant DummyJSON.
Fin       : le Job s'arrête quand toutes les partitions ont été lues jusqu'au bout.
"""
import json
import sys

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition

from common import config, db
from common.config import env
from common.kafka_utils import wait_for_kafka

log = config.setup_logging("reference-loader")

GROUP_ID = env("REFERENCE_LOADER_GROUP", "reference-loader")
BATCH_SIZE = env("REFERENCE_LOADER_BATCH", "200", int)
REPLAY_FROM_START = env("REPLAY_FROM_START", "false").lower() == "true"


def rewind(consumer, partitions):
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING
    consumer.assign(partitions)

UPSERT_PRODUCT = """
INSERT INTO products (product_id, title, category, price, discount_pct, stock, brand,
                      rating, thumbnail, collected_at, updated_at)
VALUES (%(product_id)s, %(title)s, %(category)s, %(price)s, %(discount_pct)s, %(stock)s,
        %(brand)s, %(rating)s, %(thumbnail)s, %(collected_at)s, now())
ON CONFLICT (product_id) DO UPDATE SET
    title = EXCLUDED.title, category = EXCLUDED.category, price = EXCLUDED.price,
    discount_pct = EXCLUDED.discount_pct, stock = EXCLUDED.stock, brand = EXCLUDED.brand,
    rating = EXCLUDED.rating, thumbnail = EXCLUDED.thumbnail,
    collected_at = EXCLUDED.collected_at, updated_at = now()
"""

UPSERT_CUSTOMER = """
INSERT INTO customers (customer_id, first_name, last_name, email, age, city,
                       collected_at, updated_at)
VALUES (%(customer_id)s, %(first_name)s, %(last_name)s, %(email)s, %(age)s, %(city)s,
        %(collected_at)s, now())
ON CONFLICT (customer_id) DO UPDATE SET
    first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name,
    email = EXCLUDED.email, age = EXCLUDED.age, city = EXCLUDED.city,
    collected_at = EXCLUDED.collected_at, updated_at = now()
"""

STATEMENTS = {config.TOPIC_PRODUCTS: UPSERT_PRODUCT, config.TOPIC_CUSTOMERS: UPSERT_CUSTOMER}


def end_offsets(consumer, topics):
    """Offsets de fin (high watermark) de chaque partition au démarrage."""
    targets = {}
    metadata = consumer.list_topics(timeout=10)
    for topic in topics:
        for partition in metadata.topics[topic].partitions:
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            targets[(topic, partition)] = high
    return targets


def main():
    wait_for_kafka()
    topics = list(STATEMENTS)
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    conn = db.connect()
    counts = {topic: 0 for topic in topics}
    invalid = 0

    try:
        targets = end_offsets(consumer, topics)
        log.info("Offsets de fin à atteindre : %s", targets)
        if REPLAY_FROM_START:
            # Démonstration du rejeu : relire tout le topic, l'upsert absorbe les doublons.
            consumer.subscribe(topics, on_assign=rewind)
        else:
            consumer.subscribe(topics)
        # Partitions vides : rien à lire.
        done = {tp for tp, high in targets.items() if high == 0}

        while len(done) < len(targets):
            messages = consumer.consume(BATCH_SIZE, timeout=2.0)
            if not messages:
                # Rien de neuf : une partition assignée dont la position a atteint
                # l'offset de fin est terminée (cas d'un groupe déjà à jour).
                assigned = consumer.assignment()
                if assigned:
                    for tp in consumer.position(assigned):
                        if tp.offset >= targets[(tp.topic, tp.partition)]:
                            done.add((tp.topic, tp.partition))
                continue

            with conn.transaction():
                with conn.cursor() as cur:
                    for msg in messages:
                        if msg.error():
                            if msg.error().code() != KafkaError._PARTITION_EOF:
                                log.error("Erreur Kafka : %s", msg.error())
                            continue
                        try:
                            record = json.loads(msg.value())
                            cur.execute(STATEMENTS[msg.topic()], record)
                            counts[msg.topic()] += 1
                        except (ValueError, KeyError) as exc:
                            invalid += 1
                            log.warning("Message ignoré %s[%d]@%d : %s",
                                        msg.topic(), msg.partition(), msg.offset(), exc)
            # Le commit Kafka n'intervient qu'après le COMMIT SQL réussi.
            consumer.commit(asynchronous=False)
            for msg in messages:
                if not msg.error() and msg.offset() + 1 >= targets[(msg.topic(), msg.partition())]:
                    done.add((msg.topic(), msg.partition()))

        with conn.cursor() as cur:
            cur.execute("SELECT (SELECT count(*) FROM products), (SELECT count(*) FROM customers)")
            products, customers = cur.fetchone()
        log.info("Messages traités : %s, invalides : %d", counts, invalid)
        log.info("PostgreSQL contient %d produits et %d clients", products, customers)
    finally:
        consumer.close()
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
