"""Reconciliation report: does every Bronze row end up accounted for?

    bronze_rows     = valid + invalid
    valid           = valid_distinct + duplicates_in_bronze
    valid_distinct  = silver_rows + dropped_late_or_in_flight
    invalid         = quarantine_rows

Hard invariants:  duplicates_in_silver == 0   and   quarantine_rows == invalid
'dropped_late_or_in_flight' = events older than the watermark (dropped by design) + anything not processed yet.

Run:  python jobs/reconcile.py
"""
import json

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import BRONZE, GOLD_FRAUD_FLAGS, GOLD_MERCHANT_DAILY, GOLD_VELOCITY, QUARANTINE, ROOT, SILVER, get_spark
from transforms import parse_bronze, with_reject_reason

spark = get_spark("reconcile")


def read(path):
    return spark.read.format("delta").load(path) if DeltaTable.isDeltaTable(spark, path) else None


bronze = read(BRONZE)
if bronze is None:
    raise SystemExit("Bronze table not found - start jobs/bronze.py first")

parsed = with_reject_reason(parse_bronze(bronze)).cache()
bronze_rows = parsed.count()
invalid = parsed.filter(F.col("reject_reason").isNotNull())
valid = parsed.filter(F.col("reject_reason").isNull())
invalid_rows = invalid.count()
valid_rows = valid.count()
valid_distinct = valid.select("transaction_id").distinct().count()

silver = read(SILVER)
quarantine = read(QUARANTINE)
silver_rows = silver.count() if silver is not None else 0
silver_distinct = silver.select("transaction_id").distinct().count() if silver is not None else 0
quarantine_rows = quarantine.count() if quarantine is not None else 0

print("\n=== RECONCILIATION ===")
print(f"bronze rows                     : {bronze_rows}")
print(f"  invalid (should be quarantine): {invalid_rows}   | quarantine table: {quarantine_rows}   "
      f"{'OK' if invalid_rows == quarantine_rows else 'MISMATCH (stream still catching up?)'}")
print(f"  valid                         : {valid_rows}")
print(f"    duplicates in bronze        : {valid_rows - valid_distinct}")
print(f"    valid distinct              : {valid_distinct}")
print(f"silver rows                     : {silver_rows}")
print(f"  duplicates in silver          : {silver_rows - silver_distinct}   "
      f"{'OK' if silver_rows == silver_distinct else '<-- BUG: Silver must have zero duplicates'}")
print(f"  dropped late or in flight     : {valid_distinct - silver_rows}")

print("\nrejects by reason:")
invalid.groupBy("reject_reason").count().orderBy("reject_reason").show(truncate=False)

print("bronze max offset per partition (compare: docker exec redpanda rpk topic describe payments -p  -> HIGH-WATERMARK = max_offset + 1)")
bronze.groupBy("kafka_partition").agg(F.max("kafka_offset").alias("max_offset"), F.count("*").alias("rows")).orderBy("kafka_partition").show()

for name, path in [("gold card_velocity_5m", GOLD_VELOCITY), ("gold merchant_daily", GOLD_MERCHANT_DAILY)]:
    df = read(path)
    print(f"{name}: {df.count() if df is not None else 'not built yet'} rows")
ff = read(GOLD_FRAUD_FLAGS)
if ff is not None:
    print("gold fraud_flags by rule:")
    ff.groupBy("rule").count().show()

stats_file = ROOT / "data" / "producer_stats.jsonl"
if stats_file.exists():
    totals = {}
    for line in stats_file.read_text().splitlines():
        for k, v in json.loads(line)["by_kind"].items():
            totals[k] = totals.get(k, 0) + v
    print("what the producer says it sent (all runs):", totals)
    print("(late events beyond the watermark should roughly explain 'dropped late or in flight')")
