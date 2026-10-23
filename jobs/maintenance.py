"""Compact small files and clean up old files. Prints file counts before/after - paste them in your README.

    python jobs/maintenance.py            # OPTIMIZE (compaction) + VACUUM
    python jobs/maintenance.py --zorder   # also Z-ORDER Silver by card_id

Safest to run while the streams are stopped. If you run it live, an OPTIMIZE can occasionally hit a
ConcurrentModificationException with a writer - a real-world behaviour worth understanding.
"""
import sys

from delta.tables import DeltaTable

from common import BRONZE, QUARANTINE, SILVER, get_spark

zorder = "--zorder" in sys.argv
spark = get_spark("maintenance")


def files(dt):
    d = dt.detail().select("numFiles", "sizeInBytes").first()
    return d["numFiles"], round(d["sizeInBytes"] / 1e6, 2)


for name, path in [("bronze", BRONZE), ("silver", SILVER), ("quarantine", QUARANTINE)]:
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"{name}: not found, skipping")
        continue
    dt = DeltaTable.forPath(spark, path)
    before = files(dt)
    if zorder and name == "silver":
        dt.optimize().executeZOrderBy("card_id")
    else:
        dt.optimize().executeCompaction()
    dt.vacuum()  # default retention = 7 days, so it will not delete much on a fresh laptop table
    after = files(dt)
    print(f"{name:11s} files: {before[0]:>5} -> {after[0]:>5}   size MB: {before[1]} -> {after[1]}")
