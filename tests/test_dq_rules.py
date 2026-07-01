"""Pytest coverage for DQ rules, reconciliation, and SCD2 logic."""

from datetime import date, datetime, timezone

import pytest
from pyspark.sql import Row, SparkSession

from src import config, load, reconcile, transform


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("recon-lake-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def _txn_row(
    transaction_id="T1",
    account_id="ACC1",
    amount=100.0,
    currency="USD",
    transaction_date="2026-06-01",
    status="SETTLED",
    updated_at="2026-06-01T10:00:00",
):
    return Row(
        transaction_id=transaction_id,
        account_id=account_id,
        amount=amount,
        currency=currency,
        transaction_date=transaction_date,
        status=status,
        updated_at=updated_at,
    )


def _txn_df(spark, rows):
    return spark.createDataFrame(rows, schema=config.TRANSACTION_SCHEMA)


RUN_DATE = date(2026, 7, 1)


def test_null_id_rejected(spark):
    df = _txn_df(spark, [_txn_row(transaction_id=None)])
    result = transform.apply_dq_rules(df, RUN_DATE)
    assert result.collect()[0]["dq_flag"] == config.DQ_FLAG_NULL_ID


def test_negative_amount_rejected(spark):
    df = _txn_df(spark, [_txn_row(amount=-50.0)])
    result = transform.apply_dq_rules(df, RUN_DATE)
    assert result.collect()[0]["dq_flag"] == config.DQ_FLAG_NEGATIVE_AMOUNT


def test_future_date_rejected(spark):
    df = _txn_df(spark, [_txn_row(transaction_date="2026-12-31")])
    result = transform.apply_dq_rules(df, RUN_DATE)
    assert result.collect()[0]["dq_flag"] == config.DQ_FLAG_INVALID_DATE


def test_invalid_date_string_rejected(spark):
    df = _txn_df(spark, [_txn_row(transaction_date="not-a-date")])
    result = transform.apply_dq_rules(df, RUN_DATE)
    assert result.collect()[0]["dq_flag"] == config.DQ_FLAG_INVALID_DATE


def test_duplicate_dedup_keeps_latest(spark):
    rows = [
        _txn_row(transaction_id="T4", amount=100.0, updated_at="2026-06-01T10:00:00"),
        _txn_row(transaction_id="T4", amount=200.0, updated_at="2026-06-02T10:00:00"),
    ]
    df = _txn_df(spark, rows)
    df = transform.apply_dq_rules(df, RUN_DATE)
    df = transform.apply_dedup(df)
    results = {r["amount"]: r["dq_flag"] for r in df.collect()}
    assert results[200.0] == config.DQ_FLAG_OK
    assert results[100.0] == config.DQ_FLAG_DUPLICATE


def test_reconciliation_flags_known_break(spark):
    processed_rows = [
        Row(
            transaction_id="T5",
            account_id="ACC1",
            amount=100.0,
            currency="USD",
            transaction_date=date(2026, 6, 1),
            status="SETTLED",
            updated_at=datetime(2026, 6, 1, 10, 0, 0),
        ),
        Row(
            transaction_id="T6",
            account_id="ACC2",
            amount=200.0,
            currency="USD",
            transaction_date=date(2026, 6, 1),
            status="SETTLED",
            updated_at=datetime(2026, 6, 1, 10, 0, 0),
        ),
    ]
    processed_df = spark.createDataFrame(processed_rows)

    ledger_rows = [
        Row(
            transaction_id="T6",
            cleared_flag="Y",
            cleared_date=date(2026, 6, 1),
            ledger_amount=250.0,
            ledger_status="SETTLED",
        )
    ]
    ledger_df = spark.createDataFrame(ledger_rows)

    result = reconcile.reconcile(processed_df, ledger_df)
    statuses = {r["transaction_id"]: r["recon_status"] for r in result.collect()}
    assert statuses["T5"] == config.RECON_UNRECONCILED
    assert statuses["T6"] == config.RECON_AMOUNT_MISMATCH


def _curated_input_row(transaction_id, amount, status, recon_status):
    return Row(
        transaction_id=transaction_id,
        account_id="ACC1",
        amount=amount,
        currency="USD",
        transaction_date=date(2026, 6, 1),
        status=status,
        updated_at=datetime(2026, 6, 1, 10, 0, 0),
        recon_status=recon_status,
    )


def test_scd2_second_run_produces_exactly_two_rows_for_changed_id(spark, tmp_path):
    curated_path = str(tmp_path / "curated")

    run1_ts = datetime(2026, 7, 1, 9, 0, 0, tzinfo=timezone.utc)
    incoming_1 = spark.createDataFrame([_curated_input_row("T1", 100.0, "SETTLED", config.RECON_MATCHED)])
    counts_1 = load.apply_scd2(spark, incoming_1, run_ts=run1_ts, curated_path=curated_path)
    assert counts_1 == {"inserted": 1, "closed": 0, "unchanged": 0, "new_ids": 1}

    run2_ts = datetime(2026, 7, 1, 15, 0, 0, tzinfo=timezone.utc)
    incoming_2 = spark.createDataFrame([_curated_input_row("T1", 200.0, "SETTLED", config.RECON_MATCHED)])
    counts_2 = load.apply_scd2(spark, incoming_2, run_ts=run2_ts, curated_path=curated_path)
    assert counts_2 == {"inserted": 1, "closed": 1, "unchanged": 0, "new_ids": 0}

    curated_df = spark.read.parquet(curated_path)
    rows = curated_df.filter(curated_df.transaction_id == "T1").collect()
    assert len(rows) == 2

    closed = [r for r in rows if not r["is_current"]][0]
    current = [r for r in rows if r["is_current"]][0]

    assert closed["effective_start_date"] == run1_ts.replace(tzinfo=None)
    assert closed["effective_end_date"] == run2_ts.replace(tzinfo=None)
    assert current["effective_start_date"] == run2_ts.replace(tzinfo=None)
    assert current["amount"] == 200.0
