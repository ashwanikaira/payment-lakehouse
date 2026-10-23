# Real-Time Payments Lakehouse (local, $0)

A **personal project** that simulates card-payment events and processes them in near real time:

```
producer.py  ->  Redpanda topic "payments" (3 partitions)
                       |
                 bronze.py   raw JSON + Kafka metadata, append-only, replayable
                       |
                 silver.py   parse -> validate -> enrich -> dedup -> MERGE
                       |        \-> quarantine_payments (bad rows + reason)
                       |
         +-------------+--------------+
   gold_velocity.py (stream)     gold_batch.py
   card_velocity_5m              merchant_daily, fraud_flags
```

All data is synthetic. Stack: Redpanda (Kafka API) in Docker, PySpark 3.5.1 Structured Streaming, Delta Lake 3.2.0, Python.

> **Honesty note for your CV and interviews:** call this a *personal project built on simulated data*. Every number you quote
> must come from your own runs (see `docs/MEASUREMENTS.md`).

## Step 0 - Prerequisites

- **8 GB RAM or more** (three Spark JVMs run at once). Docker, Python 3.10/3.11, **Java 11 or 17** (`java -version`).
- **Windows users: use WSL2 (Ubuntu)** with Docker Desktop's WSL integration. Spark on native Windows needs extra Hadoop
  tooling and causes avoidable problems. macOS/Linux work as is.

## Step 1 - Setup

```bash
cd payments-lakehouse
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Step 2 - Start Kafka (Redpanda) and create the topic

```bash
docker compose up -d
docker exec redpanda rpk topic create payments -p 3
docker exec redpanda rpk topic list
```

Spark and Python run on your machine, so they connect to the **external** listener `localhost:19092` (default in the code).

## Step 3 - Create the merchant reference data

```bash
python scripts/make_merchants.py
```

It writes merchants M1..M290. The producer also uses M291..M300, so ten merchants won't match (they become `UNKNOWN` in Silver).

## Step 4 - Smoke-test the producer

```bash
python producer/producer.py --rate 20 --count 2000 --seed 1
docker exec redpanda rpk topic consume payments -n 3        # look at a few raw messages
docker exec redpanda rpk topic describe payments -p         # HIGH-WATERMARK per partition
```

The producer injects ~2% duplicates, ~3% late events (10-40 min old), ~1% corrupt records. Useful flags:
`--rate 0` (full speed), `--burst-every 300` (same-card bursts for the fraud rule), `--late-rate 0.2 --late-min 30 --late-max 45`, `--with-channel`.

## Step 5 - Bronze (raw and replayable)

Open **terminal 2** (activate the venv there too):

```bash
python jobs/bronze.py
```

The first run downloads the Delta and Kafka jars (a minute or two). Leave it running. Then, from terminal 1, keep data flowing:

```bash
python producer/producer.py --rate 20 --burst-every 300
```

Check Bronze from a third terminal:

```bash
python jobs/peek.py bronze 5
```

You should see `raw_value` (JSON text, bad rows included), `kafka_partition`, `kafka_offset`, `kafka_ts`, `ingest_ts`.

**Say in interviews:** Bronze never parses or rejects; it keeps the original bytes plus lineage so I can rebuild everything downstream.

## Step 6 - Silver (clean, trusted)

Wait until `data/lake/bronze/payments/_delta_log` exists, then in **terminal 3**:

```bash
python jobs/silver.py
```

What it does (`jobs/silver.py`, logic in `jobs/transforms.py`):

1. `from_json` with an explicit schema, then a `reject_reason` per row: `unparseable_json`, `missing_field`, `null_amount`, `non_positive_amount`, `bad_event_time`.
2. Invalid rows go to `quarantine_payments`.
3. Valid rows are enriched with the merchant table using a **broadcast join** (left join, unknown merchant = `UNKNOWN`).
4. `withWatermark("event_time", "15 minutes").dropDuplicatesWithinWatermark(["transaction_id"])` removes close-together duplicates.
5. `foreachBatch` does an **insert-only MERGE** on `transaction_id` (plus `event_date` for partition pruning), so a re-run batch can't create duplicates.

```bash
python jobs/peek.py silver 5
python jobs/peek.py quarantine 5
```

## Step 7 - Gold

Terminal 4 (streaming, per-card 5-minute windows):

```bash
python jobs/gold_velocity.py
```

Terminal 5 (batch, re-run whenever you like):

```bash
python jobs/gold_batch.py
python jobs/peek.py fraud_flags 10
python jobs/peek.py merchant_daily 10
```

| Table | Grain | Notes |
|---|---|---|
| `card_velocity_5m` | one row per card per 5-min window | update-mode aggregation + MERGE |
| `merchant_daily` | one row per merchant per day | batch aggregate |
| `fraud_flags` | one row per (transaction, rule) | `VELOCITY`: >=5 payments in 5 minutes; `AMOUNT_SPIKE`: amount > 5x the card's prior average (needs 5 prior payments) |

The fraud logic is **rule-based, not ML**. Say so, and say it will have false positives (with heavy-tailed amounts, ~1-2% of payments trip the spike rule).

## Step 8 - Verify with the reconciliation report

```bash
python jobs/reconcile.py
```

It proves the counts add up:

```
bronze = valid + invalid        invalid == quarantine
valid  = valid_distinct + duplicates_in_bronze
valid_distinct = silver + dropped_late_or_in_flight
duplicates_in_silver must be 0
```

If streams are still catching up, wait a minute and run it again.

## Step 9 - Break it on purpose (your interview stories)

Record what happened in `docs/PROBLEMS_I_HIT.md`. Run `reconcile.py` before and after each experiment.

**A. Crash recovery.** With the producer running, kill Silver hard (`Ctrl+C` or `kill -9` its process). Keep producing for a minute.
Restart `python jobs/silver.py`. Run `reconcile.py`: `duplicates in silver` must be 0 and Silver must catch up.
*Why:* the checkpoint resumes from the last committed offsets, and the MERGE ignores rows already inserted.

**B. Producer outage and catch-up.** Stop the producer for 2 minutes, restart it. Compare Bronze's max offset per partition
(`reconcile.py`) with `docker exec redpanda rpk topic describe payments -p`. Caught up when `HIGH-WATERMARK = max_offset + 1`.

**C. Schema change.** Run `python producer/producer.py --rate 20 --with-channel` for a minute. Observe: Bronze keeps the raw JSON
(including `channel`), Silver silently **ignores** `channel` because its schema doesn't list it. To adopt the field:
add `T.StructField("channel", T.StringType())` to `EVENT_SCHEMA` in `jobs/transforms.py`, add `"channel"` to the columns in
`to_silver`, then restart Silver with `AUTO_MERGE=true python jobs/silver.py` so MERGE may add the column. If Spark complains about
the checkpoint or state, reset Silver and rebuild it from Bronze (experiment F). That replay is exactly why Bronze exists.

**D. Late data and the watermark.** Run `python producer/producer.py --rate 20 --count 3000 --late-rate 0.2 --late-min 30 --late-max 45`.
In `reconcile.py`, `dropped late or in flight` grows. Compare it with the producer's own count of late events. Write down the trade-off:
a longer watermark keeps more late data but holds more state; a shorter one is cheaper but drops more.

**E. Small files.** Let Bronze run for 10+ minutes (5-second triggers make many tiny files), stop the jobs, then:

```bash
python jobs/maintenance.py            # prints files before -> after for each table
```

**F. Replay from Bronze.** Stop Silver and Gold, then rebuild everything downstream:

```bash
rm -rf data/lake/silver data/lake/gold data/checkpoints/silver data/checkpoints/silver_quarantine data/checkpoints/gold_velocity
python jobs/silver.py     # reads all of Bronze again
```

Check that Silver's row count matches what it had before (`reconcile.py`).

## Step 10 - Measure (real numbers only)

Produce enough data first (about 500k rows), then benchmark four variants (shuffle partitions x broadcast join):

```bash
python producer/producer.py --rate 0 --count 500000
python jobs/benchmark.py
```

Paste the table into `docs/MEASUREMENTS.md`. Gotcha you can cite: `spark.sql.shuffle.partitions` is stored in a stateful query's
checkpoint, so you must reset that query's checkpoint to change it.

## Step 11 - Tests

```bash
pytest -q
```

`tests/test_producer.py` checks the messy-data generator; `tests/test_transforms.py` checks validation reasons and fraud rules with local Spark.

## Step 12 - Publish on GitHub

1. `git init`, commit, push (the `.gitignore` already excludes data and checkpoints).
2. Add an architecture diagram (draw.io, export PNG into `docs/`) and your real numbers.
3. Finish `docs/PROBLEMS_I_HIT.md` and `docs/MEASUREMENTS.md`; keep a "What I would do differently in production" section
   (schema registry, monitoring/alerting, orchestration, late-event side table, incremental Gold, CI).
4. Rehearse with `docs/INTERVIEW_NOTES.md`.

## Folder map

```
docker-compose.yml        Redpanda
producer/producer.py      messy synthetic events -> Kafka
scripts/make_merchants.py merchant reference CSV
jobs/common.py            paths, settings, Spark session
jobs/transforms.py        pure logic: parse, validate, enrich, fraud rules
jobs/bronze.py silver.py gold_velocity.py gold_batch.py
jobs/reconcile.py peek.py maintenance.py benchmark.py
tests/                    pytest
docs/                     interview notes, problems log, measurements
```

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `NoBrokersAvailable` / connection refused | `docker compose up -d` not running, or wrong port: use `localhost:19092` |
| `JAVA_HOME` / `Unsupported class file major version` | Use Java 11 or 17 |
| Jar download fails on first run | Needs internet access to Maven Central once (jars are cached afterwards) |
| Silver fails: path is not a Delta table | Start `bronze.py` first and wait for its first batch |
| `Detected a data update ... not supported` | Something other than an insert changed Silver/Bronze (don't run update/delete MERGE on a stream source) |
| Checkpoint / state schema error after changing code | Reset that query's checkpoint folder and replay from Bronze |
| Out of memory / very slow | Close other apps, lower the producer rate, set `SPARK_DRIVER_MEMORY=2g` |
| `OPTIMIZE` fails with ConcurrentModificationException | Stop the streams, then run maintenance |
