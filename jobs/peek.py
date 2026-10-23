"""Look inside any table.   python jobs/peek.py silver 10
Tables: bronze | silver | quarantine | velocity | merchant_daily | fraud_flags
"""
import sys

from common import TABLES, get_spark

if len(sys.argv) < 2 or sys.argv[1] not in TABLES:
    sys.exit(f"usage: python jobs/peek.py <{'|'.join(TABLES)}> [rows]")

n = int(sys.argv[2]) if len(sys.argv) > 2 else 10
spark = get_spark("peek")
df = spark.read.format("delta").load(TABLES[sys.argv[1]])
print(f"{sys.argv[1]}: {df.count()} rows")
df.printSchema()
df.show(n, truncate=False)
