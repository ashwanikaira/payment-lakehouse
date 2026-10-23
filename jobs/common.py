"""Shared paths, settings and the Spark session factory used by every job."""
import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

# ---- Delta tables (plain paths: no metastore needed) ---------------------------------
BRONZE = str(DATA / "lake" / "bronze" / "payments")
SILVER = str(DATA / "lake" / "silver" / "payments")
QUARANTINE = str(DATA / "lake" / "silver" / "quarantine_payments")
GOLD_VELOCITY = str(DATA / "lake" / "gold" / "card_velocity_5m")
GOLD_MERCHANT_DAILY = str(DATA / "lake" / "gold" / "merchant_daily")
GOLD_FRAUD_FLAGS = str(DATA / "lake" / "gold" / "fraud_flags")
MERCHANTS_CSV = str(DATA / "reference" / "merchants.csv")

TABLES = {
    "bronze": BRONZE,
    "silver": SILVER,
    "quarantine": QUARANTINE,
    "velocity": GOLD_VELOCITY,
    "merchant_daily": GOLD_MERCHANT_DAILY,
    "fraud_flags": GOLD_FRAUD_FLAGS,
}


def checkpoint(name: str) -> str:
    return str(DATA / "checkpoints" / name)


# ---- Settings (override with environment variables) ----------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "payments")
# NOTE: for stateful streaming queries this value is stored in the checkpoint. Changing it later
# requires a fresh checkpoint for that query (a classic real-world gotcha).
SHUFFLE_PARTITIONS = os.getenv("SHUFFLE_PARTITIONS", "8")
WATERMARK = os.getenv("WATERMARK", "15 minutes")
DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "1g")
AUTO_MERGE = os.getenv("AUTO_MERGE", "false")  # "true" lets MERGE add new columns (schema evolution)

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


def get_spark(app_name: str, kafka: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", DRIVER_MEMORY)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.schema.autoMerge.enabled", AUTO_MERGE)
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
    )
    # configure_spark_with_delta_pip adds the Delta jar; extra_packages adds the Kafka connector.
    spark = configure_spark_with_delta_pip(builder, extra_packages=[KAFKA_PACKAGE] if kafka else None).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
