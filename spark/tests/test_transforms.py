"""Tests des transformations Spark en mode batch (mêmes fonctions que le streaming).

Exécution : docker compose run --rm --no-deps spark python3 -m pytest -q tests
"""
import json
from datetime import datetime, timezone

import pytest
from pyspark.sql import SparkSession

import transforms as T

ORDERS, EVENTS = "retail-orders", "retail-order-events"
CATALOG = [(1, "beauty"), (2, "beauty"), (3, "groceries")]
CUSTOMERS = [10, 11]


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield session
    session.stop()


def event(event_type="PAYMENT_COMPLETED", ts="2026-11-02T10:02:00.000Z", **overrides):
    items = overrides.pop("items", [
        {"product_id": 1, "quantity": 2, "unit_price": 10.0},
        {"product_id": 2, "quantity": 1, "unit_price": 5.0},
        {"product_id": 3, "quantity": 3, "unit_price": 1.5},
    ])
    payload = {
        "event_id": overrides.pop("event_id", "EVT-1"), "event_type": event_type,
        "event_timestamp": ts, "order_id": "ORD-1", "customer_id": 10, "items": items,
        "amount": round(sum(i["quantity"] * i["unit_price"] for i in items), 2),
        "currency": "USD", "schema_version": 1,
    }
    payload.update(overrides)
    return payload


def kafka_df(spark, messages):
    rows = [(topic, 0, offset, value if isinstance(value, bytes) else json.dumps(value).encode())
            for offset, (topic, value) in enumerate(messages)]
    return spark.createDataFrame(rows, "topic string, partition int, offset long, value binary")


def validated(spark, messages):
    df = T.parse_kafka(kafka_df(spark, messages))
    return T.enrich_and_validate(df, T.category_map(CATALOG), CUSTOMERS, ORDERS)


def reasons(spark, messages):
    return [r.reject_reason for r in validated(spark, messages).orderBy("kafka_offset").collect()]


def test_valid_events_are_accepted_and_enriched(spark):
    rows = validated(spark, [(EVENTS, event()), (ORDERS, event("ORDER_CREATED"))]).collect()
    assert [r.reject_reason for r in rows] == [None, None]
    assert [i.category for i in rows[0]["items"]] == ["beauty", "beauty", "groceries"]
    assert rows[0].event_timestamp == datetime(2026, 11, 2, 10, 2)  # UTC naïf (session UTC)


def test_invalid_events_are_flagged_with_a_reason(spark):
    assert reasons(spark, [
        (EVENTS, b"{pas du json"),
        (EVENTS, {"event_type": "PAYMENT_COMPLETED", "order_id": "X"}),
        (EVENTS, event(event_type="REFUND")),
        (ORDERS, event()),                              # paiement publié dans retail-orders
        (EVENTS, event(items=[])),
        (EVENTS, event(amount=999.0)),
        (EVENTS, event(customer_id=404)),
        (EVENTS, event(items=[{"product_id": 999, "quantity": 1, "unit_price": 1.0}])),
    ]) == [
        "JSON_INVALIDE", "CHAMP_OBLIGATOIRE_MANQUANT", "TYPE_INCONNU", "TOPIC_INCOHERENT",
        "ITEMS_INVALIDES", "MONTANT_INCOHERENT", "CLIENT_INCONNU", "PRODUIT_INCONNU",
    ]


def test_explode_by_category_counts_each_order_once_per_category(spark):
    per_cat = {r.category: r for r in T.explode_by_category(
        validated(spark, [(EVENTS, event())])).collect()}
    assert set(per_cat) == {"beauty", "groceries"}
    assert per_cat["beauty"].quantity == 3 and per_cat["beauty"].revenue == pytest.approx(25.0)
    assert per_cat["groceries"].quantity == 3 and per_cat["groceries"].revenue == pytest.approx(4.5)


def test_windowed_sales_uses_five_minute_tumbling_windows(spark):
    messages = [
        (EVENTS, event(event_id="A", ts="2026-11-02T10:01:00.000Z")),
        (EVENTS, event(event_id="B", ts="2026-11-02T10:04:59.000Z")),
        (EVENTS, event(event_id="C", ts="2026-11-02T10:05:00.000Z")),
    ]
    result = T.windowed_sales(T.explode_by_category(validated(spark, messages))) \
        .filter("category = 'beauty'").orderBy("window_start").collect()
    assert [(r.window_start.minute, r.window_end.minute, r.order_count) for r in result] == \
        [(0, 5, 2), (5, 10, 1)]
    assert result[0].total_quantity == 6
    assert float(result[0].total_revenue) == pytest.approx(50.0)
    assert result[0].max_event_time == datetime(2026, 11, 2, 10, 4, 59)


def test_completed_event_time_is_utc(spark):
    row = validated(spark, [(EVENTS, event(ts="2026-11-02T23:59:59.999Z"))]).first()
    assert row.event_timestamp.replace(tzinfo=timezone.utc).isoformat().startswith(
        "2026-11-02T23:59:59.999")
