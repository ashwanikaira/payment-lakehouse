# Interview notes - Real-Time Payments Lakehouse

Open with: *"This is a personal project I built to practice production-style streaming patterns. The data is synthetic."*
Then: **why -> data -> architecture -> hardest problem -> result -> what I'd improve.** (about 2 minutes)

## 2-minute pitch (edit into your own words)
"I wanted hands-on practice with streaming and the lakehouse pattern, so I built a pipeline that simulates card payments.
A Python producer writes JSON events to a 3-partition Kafka topic (Redpanda), and it deliberately injects duplicates, late
events, corrupt records and a schema change. Spark Structured Streaming lands the raw events in a Bronze Delta table without
touching them. Silver parses with an explicit schema, sends bad rows to a quarantine table with a reason, enriches with a
merchant table using a broadcast join, de-duplicates, and writes with an insert-only MERGE so restarts are idempotent. Gold has
a 5-minute card-velocity table and batch tables for merchant daily totals and rule-based fraud flags. I tested crash recovery,
late data and schema changes on purpose, and I measured tuning changes with a small benchmark."

## Why each choice (and the trade-off)
| Choice | Why | Trade-off / honest limit |
|---|---|---|
| Kafka-compatible log | decouples producer and consumer, replay by offset, buffers when Spark is down | local single broker, no replication |
| Bronze = raw string | never lose data; can rebuild Silver/Gold when rules change | stores bad rows too, costs storage |
| Delta Lake | ACID, MERGE, schema enforcement, time travel, streaming source/sink | tied to Delta ecosystem |
| Explicit schema in `from_json` | a contract; new fields don't break the job | new fields are silently ignored until you add them |
| Quarantine table | bad data is visible and fixable, not lost or crashing the job | someone has to look at it |
| Watermark 15 min | bounds state size, lets Spark drop old dedup state | events later than 15 min are dropped (counted in reconcile) |
| Two-layer dedup (state + MERGE) | state handles close duplicates; MERGE makes the sink idempotent after crashes/replays | MERGE cost grows with table; I prune with event_date |
| Insert-only MERGE in Silver | keeps Silver a valid streaming source for Gold | can't correct rows in place |
| Update-mode agg + MERGE for velocity | always-current running totals | more moving parts than append mode |
| Rule-based fraud flags | explainable, simple, testable | high false-positive rate; real systems add ML + feedback |

## Questions to rehearse (answer from what you actually saw)
1. **Exactly-once or at-least-once?** Kafka -> Spark is at-least-once on replay; with checkpoints + idempotent sink (MERGE on key) the *result* is effectively exactly-once.
2. **What does the checkpoint store?** Source offsets per micro-batch, the batch/commit log, and operator state (dedup/aggregation state).
3. **What if Spark crashes mid-batch?** On restart it re-runs the uncommitted batch from the last committed offsets; MERGE ignores rows already inserted.
4. **Watermark: what is it and what did it cost you?** max event time seen minus 15 min; older events are dropped by stateful operators. Quote your own numbers from `reconcile.py`.
5. **Why `foreachBatch` + MERGE instead of a plain append?** Streaming sinks can't MERGE natively; foreachBatch gives you a batch DataFrame per micro-batch.
6. **How did you handle schema change?** Bronze unaffected (raw). Silver ignored `channel` until I updated the schema; show what you did (AUTO_MERGE, replay).
7. **Small-file problem?** 5-second triggers create many tiny files; OPTIMIZE compacts them. Give your before/after numbers.
8. **How would you scale 100x?** More Kafka partitions + executors on a real cluster (Databricks/EMR), larger trigger intervals, Auto Optimize / liquid clustering, partition and Z-order by access pattern, state store tuning, schema registry.
9. **Broadcast join - why and when not?** Small dimension fits in memory so no shuffle of the big side; not when the dimension is large.
10. **Why did changing `spark.sql.shuffle.partitions` need a new checkpoint?** Stateful operators store state partitioned by that number.
11. **What would you do differently in production?** Schema registry + Avro/Protobuf, monitoring and alerting (lag, batch duration, quarantine rate), orchestration of maintenance jobs, CI tests, late-event side table, secrets management, incremental Gold, data contracts.
12. **How do you know the pipeline is correct?** The reconciliation report (counts add up, zero duplicates in Silver) plus unit tests on transforms.
