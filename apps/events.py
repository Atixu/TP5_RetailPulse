"""Contrat des événements RetailPulse (schema_version 1) et fonctions de génération.

Format temporel : ISO-8601 UTC à la milliseconde avec suffixe « Z »
(ex. 2026-11-02T10:15:00.123Z). `amount` est le montant PAYÉ, calculé
à partir des prix unitaires REMISÉS (price × (1 − discount_pct / 100)).
"""
import random
import uuid
from datetime import datetime, timezone

SCHEMA_VERSION = 1

ORDER_CREATED = "ORDER_CREATED"
PAYMENT_COMPLETED = "PAYMENT_COMPLETED"
PAYMENT_FAILED = "PAYMENT_FAILED"
ORDER_CANCELLED = "ORDER_CANCELLED"
EVENT_TYPES = (ORDER_CREATED, PAYMENT_COMPLETED, PAYMENT_FAILED, ORDER_CANCELLED)

PAYMENT_FAILURE_REASONS = ("CARD_DECLINED", "INSUFFICIENT_FUNDS", "FRAUD_SUSPECTED", "TIMEOUT")


def utc_now():
    return datetime.now(timezone.utc)


def format_ts(moment):
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
        f"{moment.microsecond // 1000:03d}Z"


def discounted_price(product):
    return round(float(product["price"]) * (1 - float(product["discount_pct"]) / 100), 2)


def build_items(products, rng, max_items=4, max_quantity=3):
    """Sélectionne 1..max_items produits DISTINCTS (un produit = une ligne)."""
    chosen = rng.sample(products, k=rng.randint(1, min(max_items, len(products))))
    return [
        {
            "product_id": int(product["product_id"]),
            "quantity": rng.randint(1, max_quantity),
            "unit_price": discounted_price(product),
        }
        for product in chosen
    ]


def order_amount(items):
    return round(sum(item["quantity"] * item["unit_price"] for item in items), 2)


def make_event(event_type, order, moment, **extra):
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": format_ts(moment),
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "items": order["items"],
        "amount": order["amount"],
        "currency": order["currency"],
        "source": "retailpulse-producer",
        "schema_version": SCHEMA_VERSION,
    }
    event.update(extra)
    return event


def new_order(customers, products, rng, currency="USD", max_items=4, max_quantity=3):
    items = build_items(products, rng, max_items, max_quantity)
    return {
        "order_id": f"ORD-{uuid.uuid4().hex[:12].upper()}",
        "customer_id": int(rng.choice(customers)["customer_id"]),
        "items": items,
        "amount": order_amount(items),
        "currency": currency,
    }


def order_lifecycle(order, rng, success_rate=0.8, cancel_after_fail_rate=0.5, now=None):
    """Retourne [(topic_kind, event), …] pour le cycle de vie complet d'une commande.

    topic_kind vaut "orders" (ORDER_CREATED) ou "order_events" (paiement / annulation).
    Règle de compensation : ORDER_CANCELLED n'est émis qu'après PAYMENT_FAILED ;
    un paiement complété n'est jamais annulé, le CA n'a donc jamais à être corrigé.
    """
    now = now or utc_now()
    events = [("orders", make_event(ORDER_CREATED, order, now))]
    if rng.random() < success_rate:
        events.append(("order_events", make_event(
            PAYMENT_COMPLETED, order, now, payment_method=rng.choice(("CARD", "PAYPAL")))))
    else:
        events.append(("order_events", make_event(
            PAYMENT_FAILED, order, now, failure_reason=rng.choice(PAYMENT_FAILURE_REASONS))))
        if rng.random() < cancel_after_fail_rate:
            events.append(("order_events", make_event(ORDER_CANCELLED, order, now)))
    return events


def default_rng(seed=None):
    return random.Random(seed)
