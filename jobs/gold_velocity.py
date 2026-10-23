"""GOLD (streaming): card_velocity_5m = per card, per 5-minute event-time window.

Grain: one row per (card_id, window_start).
Update-mode streaming aggregation + foreachBatch MERGE: each micro-batch emits the windows that changed,
and we upsert them so the table always holds the latest running totals.

Run (after Silver has written data):  python jobs/gold_velocity.py
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import GOLD_VELOCITY, SILVER, WATERMARK, checkpoint, get_spark

spark = get_spark("gold_velocity")

silver_stream = spark.readStream.format("delta").load(SILVER)

agg = (
    silver_stream.withWatermark("event_time", WATERMARK)
    .groupBy(F.window("event_time", "5 minutes").alias("w"), "card_id")
    .agg(
        F.count("*").alias("txn_count"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.max("amount").alias("max_amount"),
    )
    .select(
        "card_id",
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "txn_count", "total_amount", "max_amount",
    )
)


def upsert_velocity(batch_df, batch_id: int) -> None:
    sess = batch_df.sparkSession
    if not DeltaTable.isDeltaTable(sess, GOLD_VELOCITY):
        batch_df.write.format("delta").mode("append").save(GOLD_VELOCITY)
        return
    (
        DeltaTable.forPath(sess, GOLD_VELOCITY).alias("t")
        .merge(batch_df.alias("s"), "t.card_id = s.card_id AND t.window_start = s.window_start")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


query = (
    agg.writeStream.foreachBatch(upsert_velocity)
    .outputMode("update")
    .option("checkpointLocation", checkpoint("gold_velocity"))
    .trigger(processingTime="15 seconds")
    .start()
)

print("gold_velocity started (Ctrl+C to stop)")
query.awaitTermination()
