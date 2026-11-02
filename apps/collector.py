"""API Collector : DummyJSON -> Kafka (retail-products, retail-customers).

Rôle      : collecter les référentiels produits et clients, les valider, les normaliser
            et les publier dans Kafka (clé = identifiant DummyJSON).
Entrées   : https://dummyjson.com/products, https://dummyjson.com/users (paginés).
Sorties   : topics retail-products et retail-customers ; rapport JSON sur stdout.
Paramètres: DUMMYJSON_BASE_URL, PAGE_SIZE, HTTP_TIMEOUT_S, KAFKA_BOOTSTRAP_SERVERS.
"""
import json
import sys
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common import config
from common.config import env
from common.kafka_utils import DeliveryTracker, ensure_topics, make_producer

log = config.setup_logging("collector")

BASE_URL = env("DUMMYJSON_BASE_URL", "https://dummyjson.com")
PAGE_SIZE = env("PAGE_SIZE", "50", int)
HTTP_TIMEOUT_S = env("HTTP_TIMEOUT_S", "10", float)


class IncompleteCollection(Exception):
    pass


def make_session():
    retry = Retry(
        total=5,
        backoff_factor=1,  # 1s, 2s, 4s, 8s…
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def fetch_all(session, resource, page_size=PAGE_SIZE):
    """Parcourt toutes les pages (limit/skip) d'une ressource DummyJSON.

    Vérifie la cohérence de la réponse : champ `total` stable, tableau présent,
    nombre d'éléments final égal à `total` et identifiants uniques.
    """
    items, skip, total = [], 0, None
    while total is None or skip < total:
        response = session.get(
            f"{BASE_URL}/{resource}",
            params={"limit": page_size, "skip": skip},
            timeout=HTTP_TIMEOUT_S,
        )
        response.raise_for_status()
        payload = response.json()
        page = payload.get(resource)
        if not isinstance(page, list) or "total" not in payload:
            raise IncompleteCollection(f"Réponse inattendue pour {resource} (skip={skip})")
        if total is not None and payload["total"] != total:
            raise IncompleteCollection(f"Le total de {resource} a changé pendant la collecte")
        total = payload["total"]
        if not page:
            break  # protection contre une boucle infinie si l'API renvoie moins que total
        items.extend(page)
        skip += len(page)
        log.info("%s : %d/%d récupérés", resource, len(items), total)

    ids = [item.get("id") for item in items]
    if len(items) != total or len(set(ids)) != len(ids):
        raise IncompleteCollection(
            f"{resource} : {len(items)} reçus pour total={total}, {len(set(ids))} ids distincts"
        )
    return items


def normalize_product(raw, collected_at):
    """Retourne le produit normalisé, ou lève ValueError si un champ requis est invalide."""
    product_id = raw.get("id")
    title = (raw.get("title") or "").strip()
    category = (raw.get("category") or "").strip().lower()
    price = raw.get("price")
    if not isinstance(product_id, int) or not title or not category:
        raise ValueError("id, title ou category manquant")
    if not isinstance(price, (int, float)) or price < 0:
        raise ValueError(f"prix invalide : {price!r}")
    discount = raw.get("discountPercentage") or 0
    if not 0 <= discount <= 100:
        raise ValueError(f"remise invalide : {discount!r}")
    return {
        "product_id": product_id,
        "title": title,
        "description": raw.get("description"),
        "category": category,
        "price": round(float(price), 2),
        "discount_pct": round(float(discount), 2),
        "rating": raw.get("rating"),
        "stock": raw.get("stock"),
        "brand": (raw.get("brand") or None),
        "thumbnail": raw.get("thumbnail"),
        "source": "dummyjson",
        "collected_at": collected_at,
    }


def normalize_customer(raw, collected_at):
    customer_id = raw.get("id")
    first_name = (raw.get("firstName") or "").strip()
    last_name = (raw.get("lastName") or "").strip()
    email = (raw.get("email") or "").strip().lower()
    if not isinstance(customer_id, int) or not first_name or not last_name:
        raise ValueError("id, firstName ou lastName manquant")
    if "@" not in email:
        raise ValueError(f"email invalide : {email!r}")
    address = raw.get("address") or {}
    return {
        "customer_id": customer_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "age": raw.get("age"),
        "city": address.get("city"),
        "source": "dummyjson",
        "collected_at": collected_at,
    }


def normalize_all(raw_items, normalizer, collected_at):
    valid, rejected = [], []
    for raw in raw_items:
        try:
            valid.append(normalizer(raw, collected_at))
        except ValueError as exc:
            rejected.append({"id": raw.get("id"), "reason": str(exc)})
            log.warning("Enregistrement rejeté (id=%s) : %s", raw.get("id"), exc)
    return valid, rejected


def publish(producer, tracker, topic, records, key_field):
    for record in records:
        producer.produce(
            topic,
            key=str(record[key_field]),
            value=json.dumps(record, ensure_ascii=False),
            callback=tracker,
        )
        producer.poll(0)


def main():
    collected_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    report = {"collected_at": collected_at, "source": BASE_URL}

    ensure_topics()
    session = make_session()
    producer = make_producer()
    tracker = DeliveryTracker()

    try:
        for resource, topic, normalizer, key in (
            ("products", config.TOPIC_PRODUCTS, normalize_product, "product_id"),
            ("users", config.TOPIC_CUSTOMERS, normalize_customer, "customer_id"),
        ):
            raw = fetch_all(session, resource)
            valid, rejected = normalize_all(raw, normalizer, collected_at)
            publish(producer, tracker, topic, valid, key)
            report[resource] = {
                "received": len(raw),
                "published": len(valid),
                "rejected": rejected,
                "topic": topic,
            }
    finally:
        # Fermeture propre : on attend l'acquittement de tous les messages en file.
        remaining = producer.flush(30)
        session.close()

    report["delivered"] = tracker.delivered
    report["delivery_failures"] = tracker.failed + remaining
    print(json.dumps(report, ensure_ascii=False, indent=2))

    if report["delivery_failures"]:
        log.error("%d message(s) non livré(s)", report["delivery_failures"])
        return 1
    log.info("Collecte terminée : %d messages livrés", tracker.delivered)
    return 0


if __name__ == "__main__":
    sys.exit(main())
