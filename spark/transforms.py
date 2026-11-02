"""Transformations Spark pures (sans E/S) : parsing, validation, enrichissement, agrégation.

Elles s'appliquent aussi bien à un DataFrame streaming qu'à un DataFrame batch,
ce qui permet de les tester avec pytest (tests/test_spark_transforms.py).
"""
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

ORDER_CREATED = "ORDER_CREATED"
PAYMENT_COMPLETED = "PAYMENT_COMPLETED"
PAYMENT_FAILED = "PAYMENT_FAILED"
ORDER_CANCELLED = "ORDER_CANCELLED"
EVENT_TYPES = [ORDER_CREATED, PAYMENT_COMPLETED, PAYMENT_FAILED, ORDER_CANCELLED]

ITEM_SCHEMA = StructType([
    StructField("product_id", LongType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
])

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    # ISO-8601 UTC, ex. 2026-11-02T10:15:00.123Z (format par défaut de from_json)
    StructField("event_timestamp", TimestampType()),
    StructField("order_id", StringType()),
    StructField("customer_id", LongType()),
    StructField("items", ArrayType(ITEM_SCHEMA)),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("schema_version", IntegerType()),
])


def parse_kafka(df):
    """value (octets) -> chaîne -> struct JSON typée, en conservant les métadonnées Kafka."""
    return (
        df.select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("value").cast("string").alias("raw_value"),
        )
        .withColumn("e", F.from_json("raw_value", EVENT_SCHEMA))
        .select("kafka_topic", "kafka_partition", "kafka_offset", "raw_value", "e.*")
    )


def category_map(catalog_rows):
    """Construit une colonne map<product_id, category> à partir du catalogue PostgreSQL."""
    if not catalog_rows:
        return F.create_map().cast("map<bigint,string>")
    pairs = []
    for product_id, category in catalog_rows:
        pairs += [F.lit(int(product_id)).cast("bigint"), F.lit(category)]
    return F.create_map(*pairs)


def enrich_and_validate(df, categories, customer_ids, orders_topic):
    """Ajoute `items` enrichis (catégorie) et une colonne `reject_reason` (null = valide).

    Règles de validation (dans l'ordre, la première qui échoue est retenue) :
      JSON_INVALIDE, CHAMP_OBLIGATOIRE_MANQUANT, TYPE_INCONNU, TOPIC_INCOHERENT,
      ITEMS_INVALIDES, MONTANT_INCOHERENT, CLIENT_INCONNU, PRODUIT_INCONNU.
    """
    enriched_items = F.transform(
        "items",
        lambda i: F.struct(
            i["product_id"].alias("product_id"),
            i["quantity"].alias("quantity"),
            i["unit_price"].alias("unit_price"),
            categories[i["product_id"]].alias("category"),
        ),
    )
    df = df.withColumn("items", enriched_items)

    all_null = (F.col("event_id").isNull() & F.col("event_type").isNull()
                & F.col("order_id").isNull())
    missing = (F.col("event_id").isNull() | F.col("event_type").isNull()
               | F.col("event_timestamp").isNull() | F.col("order_id").isNull()
               | F.col("customer_id").isNull() | F.col("amount").isNull()
               | F.col("currency").isNull())
    bad_items = (
        F.col("items").isNull() | (F.size("items") == 0)
        | F.exists("items", lambda i: i["product_id"].isNull() | i["quantity"].isNull()
                   | (i["quantity"] <= 0) | i["unit_price"].isNull() | (i["unit_price"] < 0))
    )
    computed_amount = F.aggregate(
        "items", F.lit(0.0).cast("double"),
        lambda acc, i: acc + i["quantity"] * i["unit_price"])
    customers = F.array(*[F.lit(int(c)).cast("bigint") for c in customer_ids]) \
        if customer_ids else F.array().cast("array<bigint>")

    reason = (
        F.when(all_null, "JSON_INVALIDE")
        .when(missing, "CHAMP_OBLIGATOIRE_MANQUANT")
        .when(~F.col("event_type").isin(EVENT_TYPES), "TYPE_INCONNU")
        .when((F.col("event_type") == ORDER_CREATED) != (F.col("kafka_topic") == orders_topic),
              "TOPIC_INCOHERENT")
        .when(bad_items, "ITEMS_INVALIDES")
        .when(F.abs(computed_amount - F.col("amount")) > 0.05, "MONTANT_INCOHERENT")
        .when(~F.array_contains(customers, F.col("customer_id")), "CLIENT_INCONNU")
        .when(F.exists("items", lambda i: i["category"].isNull()), "PRODUIT_INCONNU")
    )
    return df.withColumn("reject_reason", reason)


def explode_by_category(df):
    """Une ligne par (événement, catégorie distincte) avec quantité et CA de la catégorie.

    Permet un order_count exact (une commande compte une fois par catégorie)
    sans count-distinct, non supporté par les agrégations streaming.
    """
    categories = F.array_distinct(F.transform("items", lambda i: i["category"]))
    return (
        df.withColumn("category", F.explode(categories))
        .withColumn("cat_items", F.filter("items", lambda i: i["category"] == F.col("category")))
        .withColumn("quantity", F.aggregate(
            "cat_items", F.lit(0).cast("long"), lambda acc, i: acc + i["quantity"]))
        .withColumn("revenue", F.aggregate(
            "cat_items", F.lit(0.0).cast("double"),
            lambda acc, i: acc + i["quantity"] * i["unit_price"]))
        .select("event_id", "order_id", "event_timestamp", "category", "quantity", "revenue")
    )


def windowed_sales(per_category, window_duration="5 minutes"):
    """Chiffre d'affaires par catégorie et fenêtre fixe (tumbling) de 5 minutes."""
    return (
        per_category.groupBy(F.window("event_timestamp", window_duration), "category")
        .agg(
            F.count(F.lit(1)).alias("order_count"),
            F.sum("quantity").alias("total_quantity"),
            F.sum("revenue").alias("total_revenue"),
            F.max("event_timestamp").alias("max_event_time"),
        )
        .select(
            "category",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "order_count", "total_quantity", "total_revenue", "max_event_time",
        )
    )
