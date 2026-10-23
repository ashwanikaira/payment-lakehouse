"""SILVER: clean, validated, de-duplicated, enriched. Bad rows go to quarantine with a reason.

Two streaming queries read Bronze:
  1. quarantine : rows that fail validation  -> quarantine_payments
  2. silver     : valid rows -> enrich (broadcast join) -> watermark dedup -> MERGE into Silver

Two layers of de-duplication (say this in interviews):
  * dropDuplicatesWithinWatermark : stream state, removes dups arriving close together
  * insert-only MERGE on transaction_id : the idempotent sink; makes crash-restarts and
    replays safe even if the stream state doesn't remember the first copy

Run (after Bronze has written at least one batch):  python jobs/silver.py
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import BRONZE, MERCHANTS_CSV, QUARANTINE, SILVER, WATERMARK, checkpoint, get_spark
from transforms import enrich_merchants, load_merchants, parse_bronze, to_silver, with_reject_reason

spark = get_spark("silver")

bronze_stream = spark.readStream.format("delta").load(BRONZE)
parsed = with_reject_reason(parse_bronze(bronze_stream))

# ---- 1) quarantine ----------------------------------------------------------------------
quarantine_q = (
    parsed.filter(F.col("reject_reason").isNotNull())
    .select("raw_value", "reject_reason", "kafka_partition", "kafka_offset", "kafka_ts",
            F.current_timestamp().alias("quarantined_ts"))
    .writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint("silver_quarantine"))
    .trigger(processingTime="10 seconds")
    .start(QUARANTINE)
)

# ---- 2) valid -> enrich -> dedup -> MERGE ----------------------------------------------
merchants = load_merchants(spark, MERCHANTS_CSV)
valid = to_silver(parsed.filter(F.col("reject_reason").isNull()))
enriched = enrich_merchants(valid, merchants)

deduped = (
    enriched.withWatermark("event_time", WATERMARK)
    .dropDuplicatesWithinWatermark(["transaction_id"])
)


def upsert_silver(batch_df, batch_id: int) -> None:
    """Runs once per micro-batch. Must be idempotent: Spark may re-run a batch after a crash."""
    batch_df = batch_df.dropDuplicates(["transaction_id"]).withColumn("silver_ts", F.current_timestamp())
    batch_df.persist()
    n = batch_df.count()
    sess = batch_df.sparkSession

    if not DeltaTable.isDeltaTable(sess, SILVER):
        batch_df.write.format("delta").partitionBy("event_date").mode("append").save(SILVER)
    else:
        (
            DeltaTable.forPath(sess, SILVER).alias("t")
            .merge(
                batch_df.alias("s"),
                # event_date in the condition lets Delta prune partitions instead of scanning the table
                "t.transaction_id = s.transaction_id AND t.event_date = s.event_date",
            )
            .whenNotMatchedInsertAll()   # INSERT-ONLY: keeps Silver a clean append-only stream source for Gold
            .execute()
        )
    print(f"[silver] batch {batch_id}: {n} candidate rows")
    batch_df.unpersist()


silver_q = (
    deduped.writeStream.foreachBatch(upsert_silver)
    .outputMode("append")
    .option("checkpointLocation", checkpoint("silver"))
    .trigger(processingTime="10 seconds")
    .start()
)

print("silver + quarantine queries started (Ctrl+C to stop)")
spark.streams.awaitAnyTermination()
