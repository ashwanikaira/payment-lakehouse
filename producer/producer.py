"""Synthetic payment-event producer that deliberately creates MESSY data.

What it injects (all rates are configurable):
  * duplicates   - same payload sent twice (producer retry / at-least-once delivery)
  * late events  - event_time is 10-40 minutes in the past
  * corrupt      - null amount, negative amount, or truncated/invalid JSON
  * bursts       - one card makes several payments within seconds (fraud-velocity signal)
  * --with-channel adds a NEW field `channel` (a schema change the consumer did not expect)

Run:  python producer/producer.py --rate 20            (Ctrl+C to stop)
      python producer/producer.py --rate 20 --count 2000 --seed 1
"""
import argparse
import json
import os
import random
import time
import uuid
from collections import Counter, namedtuple
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

Message = namedtuple("Message", "kind key payload")

ROOT = Path(__file__).resolve().parents[1]
STATS_FILE = ROOT / "data" / "producer_stats.jsonl"


@dataclass
class Config:
    dup_rate: float = 0.02
    late_rate: float = 0.03
    corrupt_rate: float = 0.01
    late_min: int = 10        # minutes
    late_max: int = 40        # minutes
    burst_every: int = 0      # 0 = no bursts; N = a burst every N generated events
    burst_size: int = 7
    with_channel: bool = False
    n_cards: int = 5000
    n_merchants: int = 300    # note: merchants.csv only has M1..M290


def make_event(rng: random.Random, now: datetime, cfg: Config) -> dict:
    ev = {
        "transaction_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "card_id": f"C{rng.randint(1, cfg.n_cards)}",
        "merchant_id": f"M{rng.randint(1, cfg.n_merchants)}",
        "amount": max(0.01, round(rng.lognormvariate(3, 1), 2)),
        "event_time": now.isoformat(),
    }
    if cfg.with_channel:
        ev["channel"] = rng.choice(["web", "pos", "app"])
    return ev


def generate(rng: random.Random, now: datetime, cfg: Config, seq: int) -> list:
    """Return the list of Messages produced for one 'tick'."""
    ev = make_event(rng, now, cfg)
    key = ev["card_id"]
    kind = "normal"
    payload = None

    roll = rng.random()
    if roll < cfg.corrupt_rate:
        sub = rng.random()
        if sub < 1 / 3:
            ev["amount"] = None
            kind = "corrupt_null_amount"
        elif sub < 2 / 3:
            ev["amount"] = -abs(ev["amount"])
            kind = "corrupt_negative_amount"
        else:
            kind = "corrupt_json"
            key = None
            payload = '{"transaction_id": "%s", "card_id": ' % ev["transaction_id"]  # truncated JSON
    elif roll < cfg.corrupt_rate + cfg.late_rate:
        late_by = timedelta(minutes=rng.uniform(cfg.late_min, cfg.late_max))
        ev["event_time"] = (now - late_by).isoformat()
        kind = "late"

    if payload is None:
        payload = json.dumps(ev)

    msgs = [Message(kind, key, payload)]

    if kind in ("normal", "late") and rng.random() < cfg.dup_rate:
        msgs.append(Message("duplicate", key, payload))  # byte-for-byte the same event

    if cfg.burst_every and seq > 0 and seq % cfg.burst_every == 0:
        card = f"C{rng.randint(1, cfg.n_cards)}"
        for i in range(cfg.burst_size):
            b = make_event(rng, now - timedelta(seconds=cfg.burst_size - i), cfg)
            b["card_id"] = card
            msgs.append(Message("burst", card, json.dumps(b)))

    return msgs


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:19092"))
    ap.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "payments"))
    ap.add_argument("--rate", type=float, default=20, help="events per second (0 = as fast as possible)")
    ap.add_argument("--count", type=int, default=0, help="stop after N ticks (0 = run until Ctrl+C)")
    ap.add_argument("--seed", type=int, default=None, help="fix for reproducible runs (default: random)")
    ap.add_argument("--dup-rate", type=float, default=0.02)
    ap.add_argument("--late-rate", type=float, default=0.03)
    ap.add_argument("--corrupt-rate", type=float, default=0.01)
    ap.add_argument("--late-min", type=int, default=10)
    ap.add_argument("--late-max", type=int, default=40)
    ap.add_argument("--burst-every", type=int, default=0, help="inject a same-card burst every N events")
    ap.add_argument("--with-channel", action="store_true", help="add the new `channel` field (schema change)")
    args = ap.parse_args()

    from confluent_kafka import Producer  # imported here so unit tests need no Kafka client

    cfg = Config(args.dup_rate, args.late_rate, args.corrupt_rate, args.late_min, args.late_max,
                 args.burst_every, 7, args.with_channel)
    rng = random.Random(args.seed)
    producer = Producer({"bootstrap.servers": args.bootstrap, "acks": "all", "linger.ms": 20})

    errors = Counter()

    def on_delivery(err, _msg):
        if err is not None:
            errors[str(err)] += 1

    stats = Counter()
    seq = 0
    next_t = time.monotonic()
    last_print = time.monotonic()
    print(f"producing to {args.bootstrap} topic={args.topic} rate={args.rate}/s  (Ctrl+C to stop)")
    try:
        while args.count == 0 or seq < args.count:
            seq += 1
            for m in generate(rng, datetime.now(timezone.utc), cfg, seq):
                while True:
                    try:
                        producer.produce(args.topic, key=m.key, value=m.payload, on_delivery=on_delivery)
                        break
                    except BufferError:
                        producer.poll(0.5)
                stats[m.kind] += 1
            producer.poll(0)
            if args.rate > 0:
                next_t += 1.0 / args.rate
                delay = next_t - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
            if time.monotonic() - last_print > 5:
                last_print = time.monotonic()
                print(f"  sent so far: {sum(stats.values())}  {dict(stats)}")
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(15)
        STATS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with STATS_FILE.open("a") as f:
            f.write(json.dumps({"finished_at": datetime.now(timezone.utc).isoformat(),
                                "total": sum(stats.values()), "by_kind": dict(stats),
                                "delivery_errors": dict(errors)}) + "\n")
        print(f"done. total={sum(stats.values())} by_kind={dict(stats)} delivery_errors={dict(errors)}")


if __name__ == "__main__":
    main()
