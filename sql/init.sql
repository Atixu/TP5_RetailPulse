-- RetailPulse : schéma PostgreSQL
-- Exécuté automatiquement au premier démarrage de PostgreSQL (docker-entrypoint-initdb.d).
-- Toutes les instructions sont idempotentes (IF NOT EXISTS) pour pouvoir être rejouées.

-- ---------------------------------------------------------------------------
-- Référentiels (alimentés par le Reference Loader depuis Kafka)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS products (
    product_id    INTEGER PRIMARY KEY,              -- identifiant DummyJSON (clé stable d'upsert)
    title         TEXT          NOT NULL,
    category      TEXT          NOT NULL,
    price         NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    discount_pct  NUMERIC(5,2)  NOT NULL DEFAULT 0 CHECK (discount_pct BETWEEN 0 AND 100),
    stock         INTEGER,
    brand         TEXT,                             -- absent pour certains produits DummyJSON
    rating        NUMERIC(3,2),
    thumbnail     TEXT,
    collected_at  TIMESTAMPTZ   NOT NULL,
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);
-- Filtres et regroupements par catégorie (dashboard, contrôles).
CREATE INDEX IF NOT EXISTS idx_products_category ON products (category);

CREATE TABLE IF NOT EXISTS customers (
    customer_id   INTEGER PRIMARY KEY,              -- identifiant DummyJSON
    first_name    TEXT NOT NULL,
    last_name     TEXT NOT NULL,
    email         TEXT NOT NULL UNIQUE,             -- unique dans DummyJSON, garde-fou de qualité
    age           INTEGER,
    city          TEXT,
    collected_at  TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Détail des commandes simulées (alimenté par Spark, requête « orders-detail »)
-- ---------------------------------------------------------------------------
-- Précédence des statuts : un événement arrivé en retard ne fait jamais régresser
-- une commande (ex. ORDER_CREATED traité après PAYMENT_COMPLETED).
CREATE OR REPLACE FUNCTION status_rank(status TEXT) RETURNS INTEGER
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE status
        WHEN 'CREATED' THEN 1
        WHEN 'PAYMENT_FAILED' THEN 2
        WHEN 'PAID' THEN 3
        WHEN 'CANCELLED' THEN 4
        ELSE 0
    END
$$;

CREATE TABLE IF NOT EXISTS orders (
    order_id      TEXT PRIMARY KEY,
    customer_id   INTEGER NOT NULL REFERENCES customers (customer_id),
    created_at    TIMESTAMPTZ NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('CREATED', 'PAID', 'PAYMENT_FAILED', 'CANCELLED')),
    total_amount  NUMERIC(12,2) NOT NULL,
    currency      CHAR(3) NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Historique par client et tri « dernières commandes » du dashboard.
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_created_at  ON orders (created_at DESC);

CREATE TABLE IF NOT EXISTS order_items (
    order_id      TEXT    NOT NULL REFERENCES orders (order_id),
    product_id    INTEGER NOT NULL REFERENCES products (product_id),
    quantity      INTEGER NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    PRIMARY KEY (order_id, product_id)
);

-- Journal des événements reçus : la PK sur event_id est la seconde barrière anti-doublon
-- (la première est la déduplication Spark par watermark). Sert aussi à mesurer la latence.
CREATE TABLE IF NOT EXISTS order_events (
    event_id        TEXT PRIMARY KEY,
    order_id        TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    amount          NUMERIC(12,2),
    currency        CHAR(3),
    kafka_topic     TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS idx_order_events_order_id ON order_events (order_id);
CREATE INDEX IF NOT EXISTS idx_order_events_ingested_at ON order_events (ingested_at);

-- Messages rejetés (JSON invalide, champ obligatoire absent, produit inconnu…).
-- L'unicité (topic, partition, offset) rend l'écriture idempotente en cas de rejeu Spark.
CREATE TABLE IF NOT EXISTS invalid_events (
    id              BIGSERIAL PRIMARY KEY,
    kafka_topic     TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    raw_value       TEXT,
    reason          TEXT NOT NULL,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset, reason)
);

-- ---------------------------------------------------------------------------
-- Résultats analytiques (alimentés par Spark, requête « sales-aggregates »)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sales_aggregates (
    category        TEXT        NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    order_count     INTEGER       NOT NULL,         -- commandes payées contenant la catégorie
    total_quantity  INTEGER       NOT NULL,
    total_revenue   NUMERIC(14,2) NOT NULL,         -- montant payé (prix remisés), devise USD
    max_event_time  TIMESTAMPTZ   NOT NULL,         -- sert au calcul de latence
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_sales_aggregates UNIQUE (category, window_start, window_end)
);
-- Courbe temporelle du dashboard (tri / filtre sur les fenêtres récentes).
CREATE INDEX IF NOT EXISTS idx_sales_aggregates_window_start ON sales_aggregates (window_start DESC);

-- ---------------------------------------------------------------------------
-- Supervision : progression des micro-batches Spark (StreamingQueryListener)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stream_progress (
    query_name          TEXT        NOT NULL,
    run_id              TEXT        NOT NULL,
    batch_id            BIGINT      NOT NULL,
    batch_timestamp     TIMESTAMPTZ NOT NULL,
    num_input_rows      BIGINT      NOT NULL,
    input_rows_per_s    DOUBLE PRECISION,
    processed_rows_per_s DOUBLE PRECISION,
    batch_duration_ms   BIGINT,
    watermark           TEXT,
    PRIMARY KEY (query_name, run_id, batch_id)
);

-- Journal des écritures d'agrégats (append-only) : sales_aggregates étant upsertée,
-- updated_at y est écrasé ; ce journal conserve chaque écriture pour mesurer la latence
-- « dernier événement pris en compte -> agrégat visible » (tools/benchmark.py).
CREATE TABLE IF NOT EXISTS aggregate_writes (
    id              BIGSERIAL PRIMARY KEY,
    batch_id        BIGINT      NOT NULL,
    category        TEXT        NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    max_event_time  TIMESTAMPTZ NOT NULL,
    written_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS idx_aggregate_writes_written_at ON aggregate_writes (written_at);
