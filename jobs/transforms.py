"""Pure DataFrame logic (no Delta / Kafka imports) so it can be unit-tested with plain Spark."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Explicit schema = a contract. Fields not listed here (e.g. the new `channel`) are ignored by from_json.
EVENT_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("card_id", T.StringType()),
    T.StructField("merchant_id", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("event_time", T.StringType()),
])


def parse_bronze(bronze: DataFrame) -> DataFrame:
    """Bronze (raw JSON string + Kafka metadata) -> typed columns. Never drops rows."""
    p = bronze.withColumn("e", F.from_json(F.col("raw_value"), EVENT_SCHEMA))
    all_null = (
        F.col("e.transaction_id").isNull() & F.col("e.card_id").isNull() & F.col("e.merchant_id").isNull()
        & F.col("e.amount").isNull() & F.col("e.event_time").isNull()
    )
    return p.select(
        "raw_value", "kafka_partition", "kafka_offset", "kafka_ts",
        (F.col("e").isNull() | all_null).alias("is_unparseable"),
        F.col("e.transaction_id").alias("transaction_id"),
        F.col("e.card_id").alias("card_id"),
        F.col("e.merchant_id").alias("merchant_id"),
        F.col("e.amount").alias("amount"),
        F.to_timestamp(F.col("e.event_time")).alias("event_time"),
    )


def with_reject_reason(parsed: DataFrame) -> DataFrame:
    """reject_reason is NULL for valid rows, otherwise says why the row goes to quarantine."""
    reason = (
        F.when(F.col("is_unparseable"), "unparseable_json")
        .when(F.col("transaction_id").isNull() | F.col("card_id").isNull() | F.col("merchant_id").isNull(), "missing_field")
        .when(F.col("amount").isNull(), "null_amount")
        .when(F.col("amount") <= 0, "non_positive_amount")
        .when(F.col("event_time").isNull(), "bad_event_time")
    )
    return parsed.withColumn("reject_reason", reason)


def to_silver(valid: DataFrame) -> DataFrame:
    return valid.select(
        "transaction_id", "card_id", "merchant_id", "amount", "event_time",
        F.to_date("event_time").alias("event_date"),
        "kafka_partition", "kafka_offset",
    )


def load_merchants(spark, path: str) -> DataFrame:
    return (
        spark.read.option("header", True).csv(path)
        .select(
            "merchant_id",
            F.col("merchant_name"),
            F.col("category").alias("merchant_category"),
            F.col("risk_tier").alias("merchant_risk_tier"),
        )
    )


def enrich_merchants(df: DataFrame, merchants: DataFrame) -> DataFrame:
    """Left join so a payment is never lost just because the merchant is unknown."""
    j = df.join(F.broadcast(merchants), "merchant_id", "left")
    for c in ("merchant_name", "merchant_category", "merchant_risk_tier"):
        j = j.withColumn(c, F.coalesce(F.col(c), F.lit("UNKNOWN")))
    return j


def flag_fraud(
    silver: DataFrame,
    velocity_threshold: int = 5,
    velocity_seconds: int = 300,
    spike_multiplier: float = 5.0,
    min_history: int = 5,
) -> DataFrame:
    """Rule-based flags. Grain: one row per (transaction, rule).

    VELOCITY     : card has >= velocity_threshold payments in the trailing velocity_seconds (incl. this one)
    AMOUNT_SPIKE : amount > spike_multiplier x the card's average of its PRIOR payments (needs min_history)
    """
    base = silver.select("transaction_id", "card_id", "merchant_id", "amount", "event_time")

    w_vel = Window.partitionBy("card_id").orderBy(F.col("event_time").cast("long")).rangeBetween(-velocity_seconds, 0)
    velocity = (
        base.withColumn("txns_in_window", F.count(F.lit(1)).over(w_vel))
        .filter(F.col("txns_in_window") >= velocity_threshold)
        .select(*base.columns, F.lit("VELOCITY").alias("rule"), F.col("txns_in_window").cast("double").alias("metric"))
    )

    w_hist = Window.partitionBy("card_id").orderBy("event_time", "transaction_id").rowsBetween(Window.unboundedPreceding, -1)
    spike = (
        base.withColumn("prior_avg", F.avg("amount").over(w_hist))
        .withColumn("prior_cnt", F.count("amount").over(w_hist))
        .filter((F.col("prior_cnt") >= min_history) & (F.col("amount") > spike_multiplier * F.col("prior_avg")))
        .select(*base.columns, F.lit("AMOUNT_SPIKE").alias("rule"), (F.col("amount") / F.col("prior_avg")).alias("metric"))
    )
    return velocity.unionByName(spike)
