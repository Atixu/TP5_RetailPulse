"""Tests de normalisation et de pagination du collecteur DummyJSON (sans réseau)."""
import pytest

import collector

NOW = "2026-11-02T10:00:00+00:00"


def test_normalize_product_keeps_source_id_and_normalizes_fields():
    raw = {"id": 7, "title": " Lipstick ", "category": "Beauty", "price": 12.499,
           "discountPercentage": 10.5, "rating": 4.2, "stock": 3, "thumbnail": "t.png"}
    product = collector.normalize_product(raw, NOW)
    assert product["product_id"] == 7
    assert product["title"] == "Lipstick"
    assert product["category"] == "beauty"
    assert product["price"] == 12.5
    assert product["brand"] is None  # certains produits DummyJSON n'ont pas de marque
    assert product["collected_at"] == NOW


@pytest.mark.parametrize("raw", [
    {"id": 1, "title": "", "category": "x", "price": 1},
    {"id": 1, "title": "t", "category": "x", "price": -1},
    {"id": "1", "title": "t", "category": "x", "price": 1},
    {"id": 1, "title": "t", "category": "x", "price": 1, "discountPercentage": 150},
])
def test_normalize_product_rejects_invalid_records(raw):
    with pytest.raises(ValueError):
        collector.normalize_product(raw, NOW)


def test_normalize_customer_extracts_city_and_lowercases_email():
    raw = {"id": 1, "firstName": "Emily", "lastName": "Johnson",
           "email": "Emily.Johnson@X.dummyjson.com", "age": 29, "address": {"city": "Phoenix"}}
    customer = collector.normalize_customer(raw, NOW)
    assert customer == {
        "customer_id": 1, "first_name": "Emily", "last_name": "Johnson",
        "email": "emily.johnson@x.dummyjson.com", "age": 29, "city": "Phoenix",
        "source": "dummyjson", "collected_at": NOW,
    }


def test_normalize_all_separates_valid_and_rejected():
    valid, rejected = collector.normalize_all(
        [{"id": 1, "firstName": "A", "lastName": "B", "email": "a@b"},
         {"id": 2, "firstName": "A", "lastName": "B", "email": "invalid"}],
        collector.normalize_customer, NOW)
    assert [c["customer_id"] for c in valid] == [1]
    assert rejected[0]["id"] == 2


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class FakeSession:
    """Simule l'API paginée : `total` éléments servis par pages de `limit`."""

    def __init__(self, total, served=None):
        self.total = total
        self.served = total if served is None else served
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append(params)
        skip, limit = params["skip"], params["limit"]
        ids = range(skip + 1, min(skip + limit, self.served) + 1)
        return FakeResponse({"products": [{"id": i} for i in ids],
                             "total": self.total, "skip": skip, "limit": limit})


def test_fetch_all_follows_pagination_until_total():
    session = FakeSession(total=120)
    items = collector.fetch_all(session, "products", page_size=50)
    assert len(items) == 120
    assert [c["skip"] for c in session.calls] == [0, 50, 100]


def test_fetch_all_detects_incomplete_collection():
    with pytest.raises(collector.IncompleteCollection):
        collector.fetch_all(FakeSession(total=120, served=90), "products", page_size=50)
