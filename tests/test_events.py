"""Tests du contrat d'événements et de la simulation commerciale."""
import re
from datetime import datetime, timezone

import events

PRODUCTS = [{"product_id": i, "price": 10.0 * i, "discount_pct": 10.0} for i in range(1, 11)]
CUSTOMERS = [{"customer_id": i} for i in range(1, 6)]
REQUIRED = {"event_id", "event_type", "event_timestamp", "order_id", "customer_id",
            "items", "amount", "currency", "source", "schema_version"}


def test_timestamp_format_is_iso_utc_with_milliseconds():
    moment = datetime(2026, 11, 2, 10, 15, 0, 123456, tzinfo=timezone.utc)
    assert events.format_ts(moment) == "2026-11-02T10:15:00.123Z"


def test_discounted_price():
    assert events.discounted_price({"price": 100, "discount_pct": 12.5}) == 87.5


def test_order_items_are_distinct_and_amount_is_consistent():
    rng = events.default_rng(42)
    for _ in range(200):
        order = events.new_order(CUSTOMERS, PRODUCTS, rng, max_items=4, max_quantity=3)
        ids = [i["product_id"] for i in order["items"]]
        assert 1 <= len(ids) <= 4 and len(ids) == len(set(ids))
        assert all(1 <= i["quantity"] <= 3 for i in order["items"])
        assert order["amount"] == round(sum(i["quantity"] * i["unit_price"]
                                            for i in order["items"]), 2)
        assert re.fullmatch(r"ORD-[0-9A-F]{12}", order["order_id"])


def test_lifecycle_respects_contract_and_compensation_rule():
    rng = events.default_rng(1)
    seen_ids = set()
    outcomes = {"PAYMENT_COMPLETED": 0, "PAYMENT_FAILED": 0, "ORDER_CANCELLED": 0}
    for _ in range(500):
        order = events.new_order(CUSTOMERS, PRODUCTS, rng)
        lifecycle = events.order_lifecycle(order, rng, success_rate=0.8,
                                           cancel_after_fail_rate=0.5)
        types = [e["event_type"] for _, e in lifecycle]
        assert types[0] == "ORDER_CREATED" and lifecycle[0][0] == "orders"
        assert all(kind == "order_events" for kind, _ in lifecycle[1:])
        # Une annulation ne suit jamais un paiement réussi : pas de compensation de CA.
        assert types[1:] in (["PAYMENT_COMPLETED"], ["PAYMENT_FAILED"],
                             ["PAYMENT_FAILED", "ORDER_CANCELLED"])
        for _, event in lifecycle:
            assert REQUIRED <= event.keys()
            assert event["order_id"] == order["order_id"]
            assert event["event_id"] not in seen_ids
            seen_ids.add(event["event_id"])
        for t in types[1:]:
            outcomes[t] += 1
    assert 0.72 < outcomes["PAYMENT_COMPLETED"] / 500 < 0.88
    assert outcomes["ORDER_CANCELLED"] <= outcomes["PAYMENT_FAILED"]
