"""CLI orchestration: ingest -> DQ/quarantine -> reconcile -> SCD2 load.

    python -m src.pipeline --scenario {initial,updated} [--generate]
"""

import argparse
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src import config, generate_data, ingest, load, reconcile, transform

logger = logging.getLogger("recon-lake.pipeline")


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("recon-lake")
        .master("local[*]")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def parse_run_ts(value: str) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    return datetime.fromisoformat(value)


def _build_quarantine_df(txn_corrupt_df, ledger_corrupt_df, dq_quarantine_df):
    """Unify all quarantine sources into one schema:
    (transaction_id, raw_payload, dq_flag, stage)."""
    txn_quarantine = (
        txn_corrupt_df.withColumn("transaction_id", F.lit(None).cast("string"))
        .withColumn("dq_flag", F.lit(config.DQ_FLAG_CORRUPT_JSON))
        .withColumn("stage", F.lit("INGEST_TRANSACTIONS"))
        .select("transaction_id", "raw_payload", "dq_flag", "stage")
    )
    ledger_quarantine = (
        ledger_corrupt_df.withColumn("transaction_id", F.lit(None).cast("string"))
        .withColumn("dq_flag", F.lit(config.DQ_FLAG_CORRUPT_JSON))
        .withColumn("stage", F.lit("INGEST_LEDGER"))
        .select("transaction_id", "raw_payload", "dq_flag", "stage")
    )
    dq_quarantine = (
        dq_quarantine_df.withColumn(
            "raw_payload",
            F.to_json(
                F.struct(
                    "transaction_id",
                    "account_id",
                    "amount",
                    "currency",
                    "transaction_date",
                    "status",
                    "updated_at",
                )
            ),
        )
        .withColumn("stage", F.lit("TRANSFORM"))
        .select("transaction_id", "raw_payload", "dq_flag", "stage")
    )
    return txn_quarantine.unionByName(ledger_quarantine).unionByName(dq_quarantine)


def write_quarantine(df, path: str, run_scenario: str) -> None:
    df = df.withColumn("run_scenario", F.lit(run_scenario)).withColumn("quarantined_at", F.current_timestamp())
    df.write.mode("overwrite").partitionBy("dq_flag").parquet(path)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="recon-lake pipeline: ingest -> DQ -> reconcile -> SCD2 load")
    parser.add_argument("--scenario", choices=["initial", "updated"], required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run-ts", type=str, default=None, help="ISO8601 override for deterministic runs")
    parser.add_argument("--generate", action="store_true", help="regenerate synthetic input data before running")
    parser.add_argument("--raw-dir", default=config.RAW_DIR)
    parser.add_argument("--reference-dir", default=config.REFERENCE_DIR)
    parser.add_argument("--curated-dir", default=config.CURATED_DIR)
    parser.add_argument("--quarantine-dir", default=config.QUARANTINE_DIR)
    parser.add_argument("--summary-file", default=config.SUMMARY_FILE)
    args = parser.parse_args(argv)

    configure_logging()
    run_ts = parse_run_ts(args.run_ts)

    if args.generate:
        generate_data.main(scenario=args.scenario, seed=args.seed)

    spark = build_spark_session()
    try:
        txn_path = f"{args.raw_dir}/transactions_{args.scenario}.json"
        ledger_path = f"{args.reference_dir}/ledger.json"

        txn_df, txn_corrupt_df = ingest.load_transactions(spark, txn_path)
        ledger_df, ledger_corrupt_df = ingest.load_ledger(spark, ledger_path)
        txn_clean_count = txn_df.count()
        txn_corrupt_count = txn_corrupt_df.count()
        ingested_total = txn_clean_count + txn_corrupt_count
        logger.info("[INGEST] transactions clean=%d corrupt=%d", txn_clean_count, txn_corrupt_count)
        logger.info("[INGEST] ledger clean=%d corrupt=%d", ledger_df.count(), ledger_corrupt_df.count())

        clean_df, dq_quarantine_df = transform.run_dq_pipeline(txn_df, run_date=run_ts.date())
        dq_passed_count = clean_df.count()
        dq_quarantined_count = dq_quarantine_df.count()
        logger.info("[DQ] passed=%d quarantined=%d", dq_passed_count, dq_quarantined_count)

        reconciled_df = reconcile.reconcile(clean_df, ledger_df)
        reconciled_df.cache()

        all_quarantine_df = _build_quarantine_df(txn_corrupt_df, ledger_corrupt_df, dq_quarantine_df)
        all_quarantine_df.cache()
        write_quarantine(all_quarantine_df, args.quarantine_dir, args.scenario)

        recon_breakdown = {row["recon_status"]: row["count"] for row in reconciled_df.groupBy("recon_status").count().collect()}
        reconciled_count = sum(v for k, v in recon_breakdown.items() if k != config.RECON_UNRECONCILED)
        unreconciled_count = recon_breakdown.get(config.RECON_UNRECONCILED, 0)
        logger.info("[RECON] reconciled=%d unreconciled=%d", reconciled_count, unreconciled_count)

        summary = reconcile.build_summary(ingested_total, all_quarantine_df, reconciled_df, args.scenario, run_ts)
        summary_text = reconcile.format_summary(summary)
        print(summary_text)
        reconcile.write_summary(summary_text, args.summary_file)

        scd2_counts = load.apply_scd2(spark, reconciled_df, run_ts=run_ts, curated_path=args.curated_dir)
        logger.info(
            "[SCD2] inserted=%d closed=%d unchanged=%d new_ids=%d",
            scd2_counts["inserted"],
            scd2_counts["closed"],
            scd2_counts["unchanged"],
            scd2_counts["new_ids"],
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
