"""SCD Type-2 merge logic against the curated Parquet lakehouse layer.

Write strategy: full read-merge-rewrite. Each run reads the ENTIRE existing
curated table (if any), computes the complete new state in Spark (untouched
history + newly-closed rows + untouched current rows + newly-current rows),
and overwrites the whole `data/curated` directory, partitioned by
`transaction_date`.

This is a deliberate simplification versus partition-overwrite-of-only-
affected-partitions: an updated record's business `transaction_date` is
unrelated to which pipeline run touched it, so a changed transaction_id can
share a partition with many untouched rows. Correctly rewriting only
affected partitions would require carefully including every co-partitioned
untouched row in that partition's rewrite (Spark's dynamic partition
overwrite replaces a partition's entire file set with only what is written
for it) -- easy to get subtly wrong. At this project's ~200-row demo scale,
full read-merge-rewrite is simpler to implement correctly and trivially easy
to verify, at the cost of rewriting more data than a production Delta/
Iceberg `MERGE INTO` would need to. See README for further discussion.

`effective_start_date` / `effective_end_date` are populated from an actual
wall-clock run timestamp (passed in as `run_ts`, captured once in
pipeline.py) -- NOT from a coarse calendar date -- so that two runs on the
same calendar day still produce strictly increasing timestamps and a
closed row's `effective_end_date` is always > its `effective_start_date`.
`transaction_date` (the partition column) is the transaction's own business
date and is unrelated to `run_ts`; it never changes across a given
transaction_id's SCD2 versions.
"""

import os
from datetime import datetime
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config

CURATED_COLUMNS = [
    "transaction_id",
    "account_id",
    "amount",
    "currency",
    "transaction_date",
    "status",
    "recon_status",
    "effective_start_date",
    "effective_end_date",
    "is_current",
]


def read_existing_curated(spark: SparkSession, path: str = config.CURATED_DIR) -> Optional[DataFrame]:
    """Return None if no curated Parquet exists yet at `path` (first run),
    else the existing curated DataFrame. Checked via a plain directory
    existence/non-emptiness check rather than try/except on spark.read,
    which could mask genuine read errors.

    The DataFrame is cached and eagerly materialized (`.count()`) before
    being returned: `apply_scd2` later overwrites this same `path`, and
    without caching, Spark's lazy re-evaluation of the read during the
    write action can race against the overwrite deleting the source files
    out from under it (a FileNotFoundException on the old part-files).
    Caching forces the full read to complete and be served from memory
    for every downstream use, so the write never touches the source files
    again.
    """
    if not (os.path.isdir(path) and os.listdir(path)):
        return None
    df = spark.read.parquet(path).cache()
    df.count()
    return df


def scd2_merge(existing_df: Optional[DataFrame], incoming_df: DataFrame, run_ts: datetime) -> Tuple[DataFrame, dict]:
    """Core SCD2 logic. `incoming_df` is this run's reconciled, DQ-clean
    rows only. Returns (full_new_curated_df, counts)."""
    run_ts_lit = F.lit(run_ts).cast("timestamp")
    sentinel_lit = F.lit(config.SCD2_END_DATE_SENTINEL).cast("timestamp")

    incoming_base = incoming_df.select(
        "transaction_id", "account_id", "amount", "currency", "transaction_date", "status", "recon_status"
    )

    if existing_df is None:
        result = (
            incoming_base.withColumn("effective_start_date", run_ts_lit)
            .withColumn("effective_end_date", sentinel_lit)
            .withColumn("is_current", F.lit(True))
            .select(*CURATED_COLUMNS)
        )
        inserted = result.count()
        counts = {"inserted": inserted, "closed": 0, "unchanged": 0, "new_ids": inserted}
        return result, counts

    current_rows = existing_df.filter(F.col("is_current"))
    historical_rows = existing_df.filter(~F.col("is_current"))

    joined = incoming_base.alias("new").join(current_rows.alias("cur"), on="transaction_id", how="left")

    changed_mask = F.col("cur.transaction_id").isNotNull() & (
        ~F.col("new.amount").eqNullSafe(F.col("cur.amount"))
        | ~F.col("new.status").eqNullSafe(F.col("cur.status"))
        | ~F.col("new.recon_status").eqNullSafe(F.col("cur.recon_status"))
    )
    unchanged_mask = F.col("cur.transaction_id").isNotNull() & ~changed_mask
    new_id_mask = F.col("cur.transaction_id").isNull()

    changed_incoming = joined.filter(changed_mask).select("new.*")
    new_ids_incoming = joined.filter(new_id_mask).select("new.*")
    unchanged_count = joined.filter(unchanged_mask).count()

    ids_to_close = changed_incoming.select("transaction_id").distinct()

    closed_rows = (
        current_rows.join(ids_to_close, on="transaction_id", how="inner")
        .withColumn("is_current", F.lit(False))
        .withColumn("effective_end_date", run_ts_lit)
    )
    kept_current_rows = current_rows.join(ids_to_close, on="transaction_id", how="left_anti")

    new_current_rows = (
        changed_incoming.unionByName(new_ids_incoming)
        .withColumn("effective_start_date", run_ts_lit)
        .withColumn("effective_end_date", sentinel_lit)
        .withColumn("is_current", F.lit(True))
    )

    result = (
        historical_rows.unionByName(closed_rows)
        .unionByName(kept_current_rows)
        .unionByName(new_current_rows)
        .select(*CURATED_COLUMNS)
    )

    counts = {
        "inserted": new_current_rows.count(),
        "closed": closed_rows.count(),
        "unchanged": unchanged_count,
        "new_ids": new_ids_incoming.count(),
    }
    return result, counts


def apply_scd2(
    spark: SparkSession, incoming_df: DataFrame, run_ts: datetime, curated_path: str = config.CURATED_DIR
) -> dict:
    """Orchestrator: read existing curated data, merge, write, return
    stage counts for pipeline.py logging."""
    existing_df = read_existing_curated(spark, curated_path)
    result, counts = scd2_merge(existing_df, incoming_df, run_ts)
    result.write.mode("overwrite").partitionBy("transaction_date").parquet(curated_path)
    return counts
