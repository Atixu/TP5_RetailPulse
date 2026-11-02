"""RetailPulse : traitement Spark Structured Streaming.

Deux requêtes dans la même application, chacune avec son checkpoint :

1. orders-detail   : retail-orders + retail-order-events -> parse/valide/enrichit ->
                     orders, order_items, order_events, invalid_events (sans état).
2. sales-aggregates: retail-order-events -> PAYMENT_COMPLETED -> watermark ->
                     déduplication par event_id -> une ligne par catégorie ->
                     fenêtres de 5 min -> sales_aggregates (mode update + upsert).

Paramètres : voir les variables d'environnement lues ci-dessous (README).
"""
import logging
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

import transforms as T
from sink import PostgresSink

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("retailpulse.spark")
logging.getLogger("py4j").setLevel(logging.WARNING)

KAFKA = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_ORDERS = os.environ.get("TOPIC_ORDERS", "retail-orders")
TOPIC_ORDER_EVENTS = os.environ.get("TOPIC_ORDER_EVENTS", "retail-order-events")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/checkpoints/retailpulse")
WATERMARK = os.environ.get("WATERMARK_DELAY", "10 minutes")
WINDOW = os.environ.get("WINDOW_DURATION", "5 minutes")
TRIGGER = os.environ.get("TRIGGER_INTERVAL", "5 seconds")
MAX_OFFSETS_PER_TRIGGER = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "earliest")

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "retailpulse")
PG_USER = os.environ.get("POSTGRES_USER", "retailpulse")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
CONNINFO = (f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} "
            f"user={PG_USER} password={PG_PASSWORD}")


class ProgressListener(StreamingQueryListener):
    """Journalise et persiste la progression de chaque micro-batch (débit, durée)."""

    def __init__(self, sink):
        self.sink = sink

    def onQueryStarted(self, event):
        log.info("Requête démarrée : %s (run %s)", event.name, event.runId)

    def onQueryProgress(self, event):
        p = event.progress
        if p.numInputRows:
            log.info("PROGRESS %s batch=%s rows=%s in=%.1f/s proc=%.1f/s duration=%sms wm=%s",
                     p.name, p.batchId, p.numInputRows, p.inputRowsPerSecond,
                     p.processedRowsPerSecond, p.batchDuration,
                     (p.eventTime or {}).get("watermark"))
            self.sink.write_progress(p)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log.warning("Requête terminée : %s exception=%s", event.id, event.exception)


def read_reference(spark, query):
    return (spark.read.format("jdbc")
            .option("url", JDBC_URL).option("query", query)
            .option("user", PG_USER).option("password", PG_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .load())


def load_catalog(spark, timeout_s=600):
    """Lit les référentiels via JDBC (attend que le Reference Loader les ait chargés)."""
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            products = read_reference(spark, "SELECT product_id, category FROM products").collect()
            customers = read_reference(spark, "SELECT customer_id FROM customers").collect()
            if products and customers:
                return [(r.product_id, r.category) for r in products], \
                    [r.customer_id for r in customers]
            log.info("Référentiels vides, attente du Reference Loader…")
        except Exception as exc:  # noqa: BLE001 - PostgreSQL pas encore prêt
            log.info("Lecture des référentiels impossible (%s), nouvelle tentative…", exc)
        if time.monotonic() > deadline:
            raise RuntimeError("Référentiels indisponibles")
        time.sleep(5)


def kafka_stream(spark, topics):
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", KAFKA)
              .option("subscribe", ",".join(topics))
              .option("startingOffsets", STARTING_OFFSETS)
              .option("failOnDataLoss", "false"))
    if MAX_OFFSETS_PER_TRIGGER:
        reader = reader.option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    return reader.load()


def main():
    spark = (SparkSession.builder.appName("retailpulse-streaming")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "3"))
             .config("spark.sql.streaming.metricsEnabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

    sink = PostgresSink(CONNINFO)
    spark.streams.addListener(ProgressListener(sink))

    catalog, customer_ids = load_catalog(spark)
    log.info("Catalogue chargé : %d produits, %d clients", len(catalog), len(customer_ids))
    categories = T.category_map(catalog)

    # --- Requête 1 : détail des commandes + événements invalides -------------------
    detail = T.enrich_and_validate(
        T.parse_kafka(kafka_stream(spark, [TOPIC_ORDERS, TOPIC_ORDER_EVENTS])),
        categories, customer_ids, TOPIC_ORDERS)
    (detail.writeStream.queryName("orders-detail")
     .outputMode("append")
     .option("checkpointLocation", f"{CHECKPOINT_DIR}/orders-detail")
     .trigger(processingTime=TRIGGER)
     .foreachBatch(sink.write_orders)
     .start())

    # --- Requête 2 : agrégats de CA par catégorie et fenêtre de 5 minutes ---------
    payments = (
        T.enrich_and_validate(
            T.parse_kafka(kafka_stream(spark, [TOPIC_ORDER_EVENTS])),
            categories, customer_ids, TOPIC_ORDERS)
        .filter(F.col("reject_reason").isNull())
        # Règle de CA : seuls les paiements réussis sont comptabilisés.
        .filter(F.col("event_type") == T.PAYMENT_COMPLETED)
        # Retard toléré ; au-delà, l'événement est ignoré et l'état des fenêtres purgé.
        .withWatermark("event_timestamp", WATERMARK)
        # Un event_id rejoué dans la limite du watermark n'est compté qu'une fois.
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    aggregates = T.windowed_sales(T.explode_by_category(payments), WINDOW)
    (aggregates.writeStream.queryName("sales-aggregates")
     .outputMode("update")
     .option("checkpointLocation", f"{CHECKPOINT_DIR}/sales-aggregates")
     .trigger(processingTime=TRIGGER)
     .foreachBatch(sink.write_aggregates)
     .start())

    log.info("Requêtes démarrées (checkpoint=%s, watermark=%s, fenêtre=%s, trigger=%s)",
             CHECKPOINT_DIR, WATERMARK, WINDOW, TRIGGER)
    # Si une requête échoue, le processus s'arrête en erreur : Docker/Kubernetes le
    # redémarre et Spark reprend depuis le checkpoint.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    try:
        main()
    except Exception:  # noqa: BLE001
        log.exception("Arrêt du job Spark sur erreur")
        sys.exit(1)
