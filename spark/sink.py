"""Écriture des micro-batches dans PostgreSQL (foreachBatch) : upserts idempotents.

Chaque micro-batch est écrit dans UNE transaction. En cas d'échec (PostgreSQL
indisponible), on réessaie avec backoff ; au-delà, l'exception fait échouer la
requête, Spark ne valide pas le batch dans son commit log et le rejoue au
redémarrage depuis le checkpoint. Le rejeu est sans effet grâce aux clés :
  orders (order_id), order_items (order_id, product_id), order_events (event_id),
  invalid_events (topic, partition, offset, reason),
  sales_aggregates (category, window_start, window_end).
"""
import logging
import os
import time

import psycopg

log = logging.getLogger("retailpulse.sink")

MAX_ATTEMPTS = int(os.environ.get("SINK_MAX_ATTEMPTS", "6"))

STATUS_BY_EVENT = {
    "ORDER_CREATED": "CREATED",
    "PAYMENT_COMPLETED": "PAID",
    "PAYMENT_FAILED": "PAYMENT_FAILED",
    "ORDER_CANCELLED": "CANCELLED",
}

UPSERT_ORDER = """
INSERT INTO orders (order_id, customer_id, created_at, status, total_amount, currency)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (order_id) DO UPDATE SET
    created_at = LEAST(orders.created_at, EXCLUDED.created_at),
    status = CASE WHEN status_rank(EXCLUDED.status) > status_rank(orders.status)
                  THEN EXCLUDED.status ELSE orders.status END,
    updated_at = clock_timestamp()
"""

UPSERT_ITEM = """
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
VALUES (%s, %s, %s, %s)
ON CONFLICT (order_id, product_id) DO NOTHING
"""

INSERT_EVENT = """
INSERT INTO order_events (event_id, order_id, event_type, event_timestamp, amount, currency,
                          kafka_topic, kafka_partition, kafka_offset)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
"""

INSERT_INVALID = """
INSERT INTO invalid_events (kafka_topic, kafka_partition, kafka_offset, raw_value, reason)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset, reason) DO NOTHING
"""

UPSERT_AGGREGATE = """
INSERT INTO sales_aggregates (category, window_start, window_end, order_count,
                              total_quantity, total_revenue, max_event_time, updated_at)
VALUES (%s, %s, %s, %s, %s, round(%s::numeric, 2), %s, clock_timestamp())
ON CONFLICT (category, window_start, window_end) DO UPDATE SET
    order_count = EXCLUDED.order_count,
    total_quantity = EXCLUDED.total_quantity,
    total_revenue = EXCLUDED.total_revenue,
    max_event_time = EXCLUDED.max_event_time,
    updated_at = clock_timestamp()
"""

LOG_AGGREGATE_WRITE = """
INSERT INTO aggregate_writes (batch_id, category, window_start, max_event_time)
VALUES (%s, %s, %s, %s)
"""

INSERT_PROGRESS = """
INSERT INTO stream_progress (query_name, run_id, batch_id, batch_timestamp, num_input_rows,
                             input_rows_per_s, processed_rows_per_s, batch_duration_ms, watermark)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
"""


class PostgresSink:
    def __init__(self, conninfo):
        self.conninfo = conninfo

    def _run(self, label, batch_id, work, attempts=MAX_ATTEMPTS):
        """Exécute `work(cursor)` dans une transaction, avec retry et backoff exponentiel."""
        for attempt in range(1, attempts + 1):
            try:
                with psycopg.connect(self.conninfo, connect_timeout=5) as conn:
                    with conn.cursor() as cur:
                        work(cur)
                return
            except psycopg.OperationalError as exc:
                if attempt == attempts:
                    log.error("[%s] batch %s : PostgreSQL indisponible, abandon", label, batch_id)
                    raise
                delay = min(2 ** attempt, 30)
                log.warning("[%s] batch %s : tentative %d/%d échouée (%s), retry dans %ss",
                            label, batch_id, attempt, attempts, exc, delay)
                time.sleep(delay)

    # -- requête 1 : détail des commandes ---------------------------------------------
    def write_orders(self, batch_df, batch_id):
        rows = batch_df.collect()
        if not rows:
            return
        valid = sorted((r for r in rows if r.reject_reason is None),
                       key=lambda r: (r.event_timestamp, STATUS_BY_EVENT[r.event_type]))
        invalid = [r for r in rows if r.reject_reason is not None]

        def work(cur):
            cur.executemany(UPSERT_ORDER, [
                (r.order_id, r.customer_id, r.event_timestamp, STATUS_BY_EVENT[r.event_type],
                 r.amount, r.currency) for r in valid])
            cur.executemany(UPSERT_ITEM, [
                (r.order_id, item.product_id, item.quantity, item.unit_price)
                for r in valid for item in r.items])
            cur.executemany(INSERT_EVENT, [
                (r.event_id, r.order_id, r.event_type, r.event_timestamp, r.amount, r.currency,
                 r.kafka_topic, r.kafka_partition, r.kafka_offset) for r in valid])
            cur.executemany(INSERT_INVALID, [
                (r.kafka_topic, r.kafka_partition, r.kafka_offset, r.raw_value, r.reject_reason)
                for r in invalid])

        self._run("orders-detail", batch_id, work)
        log.info("[orders-detail] batch %s : %d valides, %d rejetés",
                 batch_id, len(valid), len(invalid))

    # -- requête 2 : agrégats par catégorie et fenêtre -------------------------------
    def write_aggregates(self, batch_df, batch_id):
        rows = batch_df.collect()
        if not rows:
            return

        def work(cur):
            cur.executemany(UPSERT_AGGREGATE, [
                (r.category, r.window_start, r.window_end, r.order_count, r.total_quantity,
                 r.total_revenue, r.max_event_time) for r in rows])
            cur.executemany(LOG_AGGREGATE_WRITE, [
                (batch_id, r.category, r.window_start, r.max_event_time) for r in rows])

        self._run("sales-aggregates", batch_id, work)
        log.info("[sales-aggregates] batch %s : %d fenêtres/catégories upsertées",
                 batch_id, len(rows))

    def write_progress(self, progress):
        def work(cur):
            cur.execute(INSERT_PROGRESS, (
                progress.name, str(progress.runId), progress.batchId, progress.timestamp,
                progress.numInputRows, progress.inputRowsPerSecond,
                progress.processedRowsPerSecond, progress.batchDuration,
                (progress.eventTime or {}).get("watermark")))
        try:
            self._run("progress", progress.batchId, work, attempts=1)
        except psycopg.Error as exc:  # la supervision ne doit jamais casser le pipeline
            log.warning("Progression non enregistrée : %s", exc)
