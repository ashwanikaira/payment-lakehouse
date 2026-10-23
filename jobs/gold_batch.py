"""GOLD (batch): merchant_daily and fraud_flags, rebuilt from Silver on each run.

  merchant_daily grain : one row per (merchant_id, event_date)
  fraud_flags grain    : one row per (transaction_id, rule)

Full overwrite = simple and idempotent (run it as often as you like). In production you would make this
incremental (MERGE only the affected dates/cards) - put that in your README's "what I'd do differently".

Run:  python jobs/gold_batch.py
"""
from pyspark.sql import functions as F

from common import GOLD_FRAUD_FLAGS, GOLD_MERCHANT_DAILY, SILVER, get_spark
from transforms import flag_fraud

spark = get_spark("gold_batch")
silver = spark.read.format("delta").load(SILVER)

merchant_daily = (
    silver.groupBy("merchant_id", "merchant_category", "event_date")
    .agg(
        F.count("*").alias("txn_count"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.countDistinct("card_id").alias("distinct_cards"),
    )
)
(merchant_daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_MERCHANT_DAILY))

fraud = flag_fraud(silver).withColumn("flagged_at", F.current_timestamp())
(fraud.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_FRAUD_FLAGS))

print("merchant_daily rows:", spark.read.format("delta").load(GOLD_MERCHANT_DAILY).count())
print("fraud_flags by rule:")
spark.read.format("delta").load(GOLD_FRAUD_FLAGS).groupBy("rule").count().show()
