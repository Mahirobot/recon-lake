"""Strict-schema JSON ingestion with corrupt-record capture.

Records that fail to parse against the explicit schema are captured in a
separate DataFrame rather than silently dropped by Spark's default
DROPMALFORMED-ish behavior.
"""

import logging
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StringType, StructField

from src import config

logger = logging.getLogger(__name__)


def read_json_strict(
    spark: SparkSession, path: str, schema: StructType, corrupt_col: str = config.CORRUPT_RECORD_COL
) -> DataFrame:
    """Read NDJSON at `path` against an explicit schema, capturing rows that
    fail to parse into `corrupt_col` instead of dropping them.

    The corrupt-record column MUST be part of the schema passed to
    `.schema()` or Spark silently ignores `columnNameOfCorruptRecord`.
    """
    schema_with_corrupt = StructType(schema.fields + [StructField(corrupt_col, StringType(), True)])
    return (
        spark.read.schema(schema_with_corrupt)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", corrupt_col)
        .json(path)
    )


def split_corrupt_records(
    df: DataFrame, corrupt_col: str = config.CORRUPT_RECORD_COL
) -> Tuple[DataFrame, DataFrame]:
    """Split `df` into (clean_df, corrupt_df).

    clean_df drops the corrupt-record column and any row where it is set.
    corrupt_df keeps only rows where the corrupt-record column is set.
    """
    corrupt_df = df.filter(col(corrupt_col).isNotNull())
    clean_df = df.filter(col(corrupt_col).isNull()).drop(corrupt_col)
    return clean_df, corrupt_df


def load_transactions(spark: SparkSession, path: str) -> Tuple[DataFrame, DataFrame]:
    """Read transaction NDJSON with the strict transaction schema.

    Returns (clean_df, corrupt_df). corrupt_df has columns
    (corrupt_record, source_file).
    """
    # Spark disallows querying a JSON/CSV-sourced DataFrame when the only
    # referenced column is the corrupt-record column (Since Spark 2.3), so
    # the raw read is cached/materialized before it's split and counted.
    raw_df = read_json_strict(spark, path, config.TRANSACTION_SCHEMA).cache()
    raw_df.count()
    clean_df, corrupt_df = split_corrupt_records(raw_df)
    corrupt_df = corrupt_df.select(col(config.CORRUPT_RECORD_COL).alias("raw_payload")).withColumn(
        "source_file", lit(path)
    )
    corrupt_count = corrupt_df.count()
    if corrupt_count:
        logger.warning("Ingestion: %d corrupt record(s) found in %s", corrupt_count, path)
    return clean_df, corrupt_df


def load_ledger(spark: SparkSession, path: str) -> Tuple[DataFrame, DataFrame]:
    """Read ledger NDJSON with the strict ledger schema.

    Returns (clean_df, corrupt_df). corrupt_df has columns
    (corrupt_record, source_file).
    """
    raw_df = read_json_strict(spark, path, config.LEDGER_SCHEMA).cache()
    raw_df.count()
    clean_df, corrupt_df = split_corrupt_records(raw_df)
    corrupt_df = corrupt_df.select(col(config.CORRUPT_RECORD_COL).alias("raw_payload")).withColumn(
        "source_file", lit(path)
    )
    corrupt_count = corrupt_df.count()
    if corrupt_count:
        logger.warning("Ingestion: %d corrupt record(s) found in %s", corrupt_count, path)
    return clean_df, corrupt_df


if __name__ == "__main__":
    spark = SparkSession.builder.appName("recon-lake-ingest-standalone").master("local[*]").getOrCreate()
    txns, txn_corrupt = load_transactions(spark, f"{config.RAW_DIR}/transactions_initial.json")
    print(f"Transactions: {txns.count()} clean, {txn_corrupt.count()} corrupt")
    txns.show(5, truncate=False)
    ledger, ledger_corrupt = load_ledger(spark, f"{config.REFERENCE_DIR}/ledger.json")
    print(f"Ledger: {ledger.count()} clean, {ledger_corrupt.count()} corrupt")
    ledger.show(5, truncate=False)
    spark.stop()
