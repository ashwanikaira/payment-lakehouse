import json
import random
from collections import Counter
from datetime import datetime, timezone

from producer import Config, generate

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def run(cfg, n=20000, seed=1):
    rng = random.Random(seed)
    out = []
    for i in range(1, n + 1):
        out.extend(generate(rng, NOW, cfg, i))
    return out


def test_same_seed_same_output():
    a = run(Config(), n=200, seed=5)
    b = run(Config(), n=200, seed=5)
    assert a == b


def test_message_mix_is_close_to_configured_rates():
    kinds = Counter(m.kind for m in run(Config()))
    n = 20000
    assert 0.010 * n < kinds["duplicate"] < 0.035 * n
    assert 0.015 * n < kinds["late"] < 0.045 * n
    assert 0.003 * n < sum(v for k, v in kinds.items() if k.startswith("corrupt")) < 0.020 * n


def test_duplicates_are_byte_identical_to_an_earlier_message():
    msgs = run(Config(), n=2000)
    seen = set()
    for m in msgs:
        if m.kind == "duplicate":
            assert m.payload in seen
        else:
            seen.add(m.payload)


def test_corrupt_kinds_are_really_corrupt():
    for m in run(Config(corrupt_rate=1.0, late_rate=0, dup_rate=0), n=300):
        if m.kind == "corrupt_json":
            try:
                json.loads(m.payload)
                raise AssertionError("expected invalid JSON")
            except json.JSONDecodeError:
                pass
        elif m.kind == "corrupt_null_amount":
            assert json.loads(m.payload)["amount"] is None
        else:
            assert json.loads(m.payload)["amount"] < 0


def test_late_events_are_10_to_40_minutes_old():
    for m in run(Config(late_rate=1.0, corrupt_rate=0, dup_rate=0), n=200):
        ev = json.loads(m.payload)
        age = (NOW - datetime.fromisoformat(ev["event_time"])).total_seconds() / 60
        assert 10 <= age <= 40


def test_burst_is_same_card_within_seconds():
    msgs = [m for m in run(Config(burst_every=50, burst_size=7), n=200) if m.kind == "burst"]
    assert len(msgs) == 7 * 4  # bursts at seq 50,100,150,200
    first = [json.loads(m.payload) for m in msgs[:7]]
    assert len({e["card_id"] for e in first}) == 1
    assert len({e["transaction_id"] for e in first}) == 7


def test_channel_only_when_requested():
    with_ch = [json.loads(m.payload) for m in run(Config(with_channel=True, corrupt_rate=0), n=50)]
    assert all("channel" in e for e in with_ch)
    without = [json.loads(m.payload) for m in run(Config(corrupt_rate=0), n=50)]
    assert all("channel" not in e for e in without)
