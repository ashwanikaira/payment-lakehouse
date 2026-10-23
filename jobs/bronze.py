"""BRONZE: Kafka -> Delta, raw and replayable.

Rules of Bronze: do NOT parse, do NOT filter, do NOT fix. Keep the original bytes (as a string)
plus where they came from (partition/offset/timestamp). If any later logic is wrong we can
rebuild Silver/Gold from here.

Run:  python jobs/bronze.py
"""
from pyspark.sql import functions as F

from common import BRONZE, KAFKA_BOOTSTRAP, KAFKA_TOPIC, checkpoint, get_spark

spark = get_spark("bronze", kafka=True)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")   # only applies on the FIRST run; afterwards the checkpoint decides
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 20000)   # back-pressure: cap one micro-batch
    .load()
)

bronze = raw.select(
    F.col("value").cast("string").alias("raw_value"),
    F.col("key").cast("string").alias("kafka_key"),
    F.col("topic").alias("kafka_topic"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
    F.col("timestamp").alias("kafka_ts"),
    F.current_timestamp().alias("ingest_ts"),
    F.to_date(F.current_timestamp()).alias("ingest_date"),
)

query = (
    bronze.writeStream.format("delta")
    .outputMode("append")
    .partitionBy("ingest_date")
    .option("checkpointLocation", checkpoint("bronze"))
    # A short trigger = fresh data but MANY SMALL FILES (you will fix that with OPTIMIZE later).
    .trigger(processingTime="5 seconds")
    .start(BRONZE)
)

print("bronze streaming query started; Spark UI: http://localhost:4040  (Ctrl+C to stop)")
query.awaitTermination()
