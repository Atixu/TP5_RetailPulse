"""Producteur : simulation commerciale continue -> retail-orders / retail-order-events.

Rôle      : générer des commandes à partir des référentiels PostgreSQL et publier
            leur cycle de vie dans Kafka (clé = order_id).
Paramètres (variables d'environnement) :
  MIN_INTERVAL_S / MAX_INTERVAL_S  pause aléatoire entre deux commandes (0.5 / 2.0)
  RATE                             si > 0 : commandes/s visées (mode benchmark, ignore les pauses)
  MAX_ORDERS                       arrêt après N commandes (0 = infini)
  PAYMENT_SUCCESS_RATE             probabilité de PAYMENT_COMPLETED (0.8)
  CANCEL_AFTER_FAIL_RATE           probabilité d'ORDER_CANCELLED après un échec (0.5)
  MAX_ITEMS / MAX_QUANTITY         taille maximale d'une commande (4 / 3)
  RANDOM_SEED                      graine pour rejouer un scénario identique
Arrêt     : SIGTERM ou SIGINT -> fin de la boucle, flush() puis bilan.
"""
import json
import signal
import sys
import time

from common import config, db
from common.config import env
from common.kafka_utils import DeliveryTracker, ensure_topics, make_producer
import events

log = config.setup_logging("producer")

MIN_INTERVAL_S = env("MIN_INTERVAL_S", "0.5", float)
MAX_INTERVAL_S = env("MAX_INTERVAL_S", "2.0", float)
RATE = env("RATE", "0", float)
MAX_ORDERS = env("MAX_ORDERS", "0", int)
PAYMENT_SUCCESS_RATE = env("PAYMENT_SUCCESS_RATE", "0.8", float)
CANCEL_AFTER_FAIL_RATE = env("CANCEL_AFTER_FAIL_RATE", "0.5", float)
MAX_ITEMS = env("MAX_ITEMS", "4", int)
MAX_QUANTITY = env("MAX_QUANTITY", "3", int)
RANDOM_SEED = env("RANDOM_SEED", "", lambda v: int(v) if v else None)
REFERENCE_WAIT_S = env("REFERENCE_WAIT_S", "600", int)

TOPICS = {"orders": config.TOPIC_ORDERS, "order_events": config.TOPIC_ORDER_EVENTS}

running = True


def stop(signum, _frame):
    global running
    log.info("Signal %s reçu : arrêt propre demandé", signal.Signals(signum).name)
    running = False


def load_references():
    """Attend que le Reference Loader ait rempli les tables puis les charge."""
    deadline = time.monotonic() + REFERENCE_WAIT_S
    with db.connect() as conn:
        while True:
            with conn.cursor() as cur:
                cur.execute("SELECT product_id, price, discount_pct FROM products")
                products = [
                    {"product_id": r[0], "price": float(r[1]), "discount_pct": float(r[2])}
                    for r in cur.fetchall()
                ]
                cur.execute("SELECT customer_id FROM customers")
                customers = [{"customer_id": r[0]} for r in cur.fetchall()]
            if products and customers:
                return products, customers
            if time.monotonic() > deadline or not running:
                raise RuntimeError("Référentiels vides : lancer collector puis reference-loader")
            log.info("Référentiels encore vides, attente…")
            time.sleep(5)


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    ensure_topics()
    products, customers = load_references()
    log.info("Référentiels chargés : %d produits, %d clients", len(products), len(customers))

    rng = events.default_rng(RANDOM_SEED)
    producer = make_producer()
    tracker = DeliveryTracker()
    produced_orders = produced_events = 0
    by_type = {event_type: 0 for event_type in events.EVENT_TYPES}
    started = time.monotonic()

    try:
        while running and (MAX_ORDERS == 0 or produced_orders < MAX_ORDERS):
            order = events.new_order(customers, products, rng, config.CURRENCY,
                                     MAX_ITEMS, MAX_QUANTITY)
            for kind, event in events.order_lifecycle(
                    order, rng, PAYMENT_SUCCESS_RATE, CANCEL_AFTER_FAIL_RATE):
                producer.produce(TOPICS[kind], key=event["order_id"],
                                 value=json.dumps(event), callback=tracker)
                produced_events += 1
                by_type[event["event_type"]] += 1
            produced_orders += 1
            producer.poll(0)

            if produced_orders % 50 == 0:
                log.info("%d commandes, %d événements (livrés=%d, échecs=%d)",
                         produced_orders, produced_events, tracker.delivered, tracker.failed)

            if RATE > 0:
                # Cadence régulière : on rattrape le planning théorique.
                delay = started + produced_orders / RATE - time.monotonic()
            else:
                delay = rng.uniform(MIN_INTERVAL_S, MAX_INTERVAL_S)
            if delay > 0:
                time.sleep(delay)
    finally:
        remaining = producer.flush(30)
        elapsed = time.monotonic() - started
        log.info(json.dumps({
            "orders": produced_orders,
            "events": produced_events,
            "by_type": by_type,
            "delivered": tracker.delivered,
            "failed": tracker.failed + remaining,
            "elapsed_s": round(elapsed, 1),
            "events_per_s": round(produced_events / elapsed, 2) if elapsed else 0,
        }))
    return 0 if tracker.failed + remaining == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
