"""Injection d'événements contrôlés pour les scénarios de test (§13 du cahier des charges).

Exemples :
  python inject_event.py --type PAYMENT_COMPLETED --product-id 1 --quantity 2
  python inject_event.py --type PAYMENT_COMPLETED --duplicate          # même event_id deux fois
  python inject_event.py --type PAYMENT_COMPLETED --late-minutes 30    # événement tardif
  python inject_event.py --type PAYMENT_FAILED
  python inject_event.py --invalid                                     # JSON invalide + champ manquant
  python inject_event.py --unknown-product                             # impossible à enrichir

L'événement publié est affiché en JSON sur stdout (dernière ligne) pour les scripts.
"""
import argparse
import json
from datetime import timedelta

from common import config, db
from common.kafka_utils import DeliveryTracker, ensure_topics, make_producer
import events

log = config.setup_logging("inject")


def pick_reference(product_id):
    with db.connect() as conn, conn.cursor() as cur:
        if product_id is None:
            cur.execute("SELECT product_id, price, discount_pct, category FROM products "
                        "ORDER BY product_id LIMIT 1")
        else:
            cur.execute("SELECT product_id, price, discount_pct, category FROM products "
                        "WHERE product_id = %s", (product_id,))
        row = cur.fetchone()
        cur.execute("SELECT customer_id FROM customers ORDER BY customer_id LIMIT 1")
        customer = cur.fetchone()
    if row is None or customer is None:
        raise SystemExit("Référentiels absents ou produit inconnu")
    product = {"product_id": row[0], "price": float(row[1]), "discount_pct": float(row[2])}
    return product, row[3], customer[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--type", default=events.PAYMENT_COMPLETED, choices=events.EVENT_TYPES)
    parser.add_argument("--product-id", type=int)
    parser.add_argument("--quantity", type=int, default=1)
    parser.add_argument("--late-minutes", type=float, default=0,
                        help="antidater event_timestamp de N minutes")
    parser.add_argument("--duplicate", action="store_true",
                        help="publier deux fois exactement le même message")
    parser.add_argument("--invalid", action="store_true",
                        help="publier un message non JSON et un message sans event_id")
    parser.add_argument("--unknown-product", action="store_true",
                        help="référencer un product_id absent du catalogue")
    args = parser.parse_args()

    ensure_topics()
    producer = make_producer()
    tracker = DeliveryTracker()
    topic = config.TOPIC_ORDERS if args.type == events.ORDER_CREATED else config.TOPIC_ORDER_EVENTS

    if args.invalid:
        producer.produce(topic, key="INVALID", value=b"{ceci n'est pas du JSON", callback=tracker)
        producer.produce(topic, key="INVALID", callback=tracker,
                         value=json.dumps({"event_type": args.type, "order_id": "ORD-INVALID"}))
        producer.flush(10)
        print(json.dumps({"published": tracker.delivered, "kind": "invalid"}))
        return

    product, category, customer_id = pick_reference(args.product_id)
    item = {"product_id": product["product_id"], "quantity": args.quantity,
            "unit_price": events.discounted_price(product)}
    if args.unknown_product:
        item["product_id"] = 999999
    # Préfixe ORD-LATE pour les événements antidatés : les contrôles SQL les reconnaissent.
    prefix = "ORD-LATE" if args.late_minutes else "ORD-TEST"
    order = {
        "order_id": f"{prefix}-{events.uuid.uuid4().hex[:8].upper()}",
        "customer_id": customer_id,
        "items": [item],
        "amount": events.order_amount([item]),
        "currency": config.CURRENCY,
    }
    moment = events.utc_now() - timedelta(minutes=args.late_minutes)
    event = events.make_event(args.type, order, moment, test=True)
    payload = json.dumps(event)

    for _ in range(2 if args.duplicate else 1):
        producer.produce(topic, key=event["order_id"], value=payload, callback=tracker)
    remaining = producer.flush(10)
    if remaining or tracker.failed:
        raise SystemExit("Échec de publication")
    log.info("Publié %d fois sur %s (catégorie %s)", tracker.delivered, topic, category)
    print(json.dumps({**event, "category": category, "copies": tracker.delivered}))


if __name__ == "__main__":
    main()
