"""Unit tests for the pure Spark logic. Needs Java + pyspark (no Kafka, no Delta)."""
from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from transforms import flag_fraud, parse_bronze, with_reject_reason

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def bronze_df(spark, payloads):
    schema = T.StructType([
        T.StructField("raw_value", T.StringType()), T.StructField("kafka_partition", T.IntegerType()),
        T.StructField("kafka_offset", T.LongType()), T.StructField("kafka_ts", T.TimestampType()),
    ])
    return spark.createDataFrame([(p, 0, i, T0) for i, p in enumerate(payloads)], schema)


def test_validation_reasons(spark):
    good = '{"transaction_id":"t1","card_id":"C1","merchant_id":"M1","amount":12.5,"event_time":"2026-01-01T12:00:00+00:00"}'
    null_amt = '{"transaction_id":"t2","card_id":"C1","merchant_id":"M1","amount":null,"event_time":"2026-01-01T12:00:00+00:00"}'
    neg_amt = '{"transaction_id":"t3","card_id":"C1","merchant_id":"M1","amount":-4,"event_time":"2026-01-01T12:00:00+00:00"}'
    no_card = '{"transaction_id":"t4","merchant_id":"M1","amount":5,"event_time":"2026-01-01T12:00:00+00:00"}'
    bad_time = '{"transaction_id":"t5","card_id":"C1","merchant_id":"M1","amount":5,"event_time":"not-a-time"}'
    garbage = '{"transaction_id": "t6", "card_id": '
    extra_field = '{"transaction_id":"t7","card_id":"C1","merchant_id":"M1","amount":9,"event_time":"2026-01-01T12:00:00+00:00","channel":"web"}'

    rows = with_reject_reason(parse_bronze(bronze_df(spark, [good, null_amt, neg_amt, no_card, bad_time, garbage, extra_field]))).collect()
    reason = {r["raw_value"]: r["reject_reason"] for r in rows}

    assert reason[good] is None
    assert reason[null_amt] == "null_amount"
    assert reason[neg_amt] == "non_positive_amount"
    assert reason[no_card] == "missing_field"
    assert reason[bad_time] == "bad_event_time"
    assert reason[garbage] is not None            # unparseable_json or missing_field - either way it is quarantined
    assert reason[extra_field] is None            # unknown extra fields are ignored, not rejected


def silver_rows(spark, rows):
    schema = T.StructType([
        T.StructField("transaction_id", T.StringType()), T.StructField("card_id", T.StringType()),
        T.StructField("merchant_id", T.StringType()), T.StructField("amount", T.DoubleType()),
        T.StructField("event_time", T.TimestampType()),
    ])
    return spark.createDataFrame(rows, schema)


def test_velocity_rule_flags_fifth_and_sixth_payment_in_five_minutes(spark):
    offsets = [0, 60, 120, 180, 240, 270]  # seconds
    rows = [(f"v{i}", "C1", "M1", 10.0, T0 + timedelta(seconds=s)) for i, s in enumerate(offsets)]
    out = flag_fraud(silver_rows(spark, rows)).filter("rule = 'VELOCITY'").collect()
    assert sorted(r["transaction_id"] for r in out) == ["v4", "v5"]


def test_amount_spike_rule_needs_history_and_flags_big_jump(spark):
    rows = [(f"s{i}", "C2", "M1", 10.0, T0 + timedelta(hours=i)) for i in range(6)]
    rows.append(("spike", "C2", "M1", 100.0, T0 + timedelta(hours=7)))
    rows.append(("early", "C3", "M1", 1000.0, T0))  # first payment of a card: no history, must NOT be flagged
    out = flag_fraud(silver_rows(spark, rows)).filter("rule = 'AMOUNT_SPIKE'").collect()
    assert [r["transaction_id"] for r in out] == ["spike"]
    assert out[0]["metric"] == pytest.approx(10.0)
