# Measurements

Paste the real output of `python jobs/benchmark.py` here (and the maintenance output). Only numbers in this file may appear
on your resume - and write them the way you measured them, e.g. "cut a 500k-row aggregation from 9.1s to 6.4s by
using a broadcast join and 8 shuffle partitions on my laptop (local[*], AQE off)".

## Machine
- CPU / RAM / OS:
- Spark 3.5.1, delta-spark 3.2.0, rows benchmarked:

## Benchmark (jobs/benchmark.py)
| variant | median s | best s |
|---|---|---|
| (paste here) | | |

## File compaction (jobs/maintenance.py)
| table | files before | files after |
|---|---|---|
| bronze | | |
| silver | | |
