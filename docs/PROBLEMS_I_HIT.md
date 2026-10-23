# Problems I hit and how I fixed them

Fill this in AS you do the Step 9 experiments in the README. Only write what actually happened on your machine -
real details (error text, numbers, what you tried first) are what make these answers believable in an interview.

Answer shape for interviews: **problem -> how I noticed -> what I tried -> what fixed it -> what I learned.**

## 1. Crash and restart (Silver killed mid-run)
- What I did:
- What I observed (reconcile output before/after):
- Why there were no duplicates (checkpoint + MERGE):

## 2. Producer outage and catch-up
- Offsets / high-watermark before and after:
- How long Bronze took to catch up:

## 3. Schema change (`channel`)
- What Bronze did:
- What Silver did (silently ignored?):
- How I adopted the new field (AUTO_MERGE, schema change, replay from Bronze):

## 4. Late events beyond the watermark
- Numbers: sent late / reached Silver / dropped:
- The trade-off (watermark length vs state size vs completeness):
- What I would do in production (e.g. route late events to a side table):

## 5. Small files in Bronze
- numFiles before OPTIMIZE:
- numFiles after OPTIMIZE:

## 6. Replay from Bronze
- What I deleted, what I re-ran, how long it took, did the counts match?

## 7. Anything else that broke (checkpoint/shuffle-partition mismatch, Java version, ports, memory...)
