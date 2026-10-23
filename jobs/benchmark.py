"""Measure the effect of tuning knobs on the SAME query, using REAL timings (these are the only numbers
you may quote on your resume).

Query: Bronze -> parse -> validate -> join merchants -> aggregate by category/day, written to the 'noop' sink
(so we time compute, not disk writes).

Variants:  A) 200 shuffle partitions, sort-merge join      (Spark defaults)
           B) 8 shuffle partitions,   sort-merge join
           C) 200 shuffle partitions, broadcast join
           D) 8 shuffle partitions,   broadcast join

Adaptive Query Execution is turned OFF so you can see the manual settings' effect. In an interview, mention that
AQE (on by default in Spark 3.5) often fixes the shuffle-partition problem for you.

You need enough rows to see a difference: produce ~500k-1M events first
    python producer/producer.py --rate 0 --count 500000
Run:  python jobs/benchmark.py
"""
import statistics
import time

from pyspark.sql import functions as F

from common import BRONZE, MERCHANTS_CSV, get_spark
from transforms import load_merchants, parse_bronze, to_silver, with_reject_reason

RUNS = 3

spark = get_spark("benchmark")
spark.conf.set("spark.sql.adaptive.enabled", "false")

merchants = load_merchants(spark, MERCHANTS_CSV)
valid = to_silver(
    with_reject_reason(parse_bronze(spark.read.format("delta").load(BRONZE))).filter(F.col("reject_reason").isNull())
)
rows = valid.count()
print(f"benchmarking on {rows} valid rows" + ("  (WARNING: <200k rows, differences will be noise)" if rows < 200_000 else ""))


def build(use_broadcast: bool):
    j = valid.join(F.broadcast(merchants) if use_broadcast else merchants, "merchant_id", "left")
    return j.groupBy("merchant_category", "event_date").agg(F.count("*").alias("n"), F.sum("amount").alias("total"))


variants = [
    ("A: 200 partitions, sort-merge join", 200, False),
    ("B: 8 partitions, sort-merge join", 8, False),
    ("C: 200 partitions, broadcast join", 200, True),
    ("D: 8 partitions, broadcast join", 8, True),
]

results = []
for name, shuffle, bc in variants:
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle))
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760" if bc else "-1")
    build(bc).write.format("noop").mode("overwrite").save()          # warm-up (not counted)
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        build(bc).write.format("noop").mode("overwrite").save()
        times.append(time.perf_counter() - t0)
    results.append((name, statistics.median(times), min(times)))

print("\n| variant | median s | best s |\n|---|---|---|")
for name, med, best in results:
    print(f"| {name} | {med:.2f} | {best:.2f} |")
print(f"\n(rows: {rows}, runs per variant: {RUNS}, AQE off) -> paste this table into docs/MEASUREMENTS.md")
