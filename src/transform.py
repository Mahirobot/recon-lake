"""Data-quality rules, quarantine split, and dq_flag audit column.

Functions here are pure (DataFrame in, DataFrame out) and unit-testable in
isolation, with no I/O and no dependency on a particular SparkSession beyond
the one implicitly carried by the input DataFrame.
"""

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src import config


def apply_dq_rules(df: DataFrame, run_date: date) -> DataFrame:
    """Add a `dq_flag` column and replace the string `transaction_date` /
    `updated_at` columns with parsed Date/Timestamp columns.

    Exactly one flag is assigned per row via a single when/otherwise chain,
    evaluated in priority order: NULL_ID > NEGATIVE_AMOUNT > INVALID_DATE >
    OK. Rows are not dropped here — only flagged. Cross-row rules (dedup)
    are handled separately in `apply_dedup`.
    """
    parsed_date = F.to_date(F.col("transaction_date"), config.TRANSACTION_DATE_FORMAT)
    parsed_updated_at = F.to_timestamp(F.col("updated_at"), config.TIMESTAMP_FORMAT)

    df = df.withColumn("transaction_date", parsed_date).withColumn("updated_at", parsed_updated_at)

    run_date_lit = F.lit(run_date)

    dq_flag = (
        F.when(F.col("transaction_id").isNull(), F.lit(config.DQ_FLAG_NULL_ID))
        .when(F.col("amount").isNull() | (F.col("amount") <= 0), F.lit(config.DQ_FLAG_NEGATIVE_AMOUNT))
        .when(
            F.col("transaction_date").isNull()
            | F.col("updated_at").isNull()
            | (F.col("transaction_date") > run_date_lit),
            F.lit(config.DQ_FLAG_INVALID_DATE),
        )
        .otherwise(F.lit(config.DQ_FLAG_OK))
    )
    return df.withColumn("dq_flag", dq_flag)


def apply_dedup(df: DataFrame) -> DataFrame:
    """Among rows flagged OK, keep the one with the latest `updated_at` per
    `transaction_id`; reflag the rest as DUPLICATE. Rows already flagged by
    an earlier rule are left untouched and excluded from the dedup window
    (their transaction_id/updated_at may be null or otherwise unsafe to
    partition/order by).
    """
    ok_df = df.filter(F.col("dq_flag") == config.DQ_FLAG_OK)
    other_df = df.filter(F.col("dq_flag") != config.DQ_FLAG_OK)

    window = Window.partitionBy("transaction_id").orderBy(F.desc("updated_at"), F.asc("transaction_id"))
    ok_df = ok_df.withColumn("dedup_rank", F.row_number().over(window))
    ok_df = ok_df.withColumn(
        "dq_flag",
        F.when(F.col("dedup_rank") > 1, F.lit(config.DQ_FLAG_DUPLICATE)).otherwise(F.col("dq_flag")),
    ).drop("dedup_rank")

    return ok_df.unionByName(other_df)


def split_quarantine(df: DataFrame) -> "tuple[DataFrame, DataFrame]":
    """Return (clean_df, quarantine_df) split on dq_flag == OK."""
    clean_df = df.filter(F.col("dq_flag") == config.DQ_FLAG_OK)
    quarantine_df = df.filter(F.col("dq_flag") != config.DQ_FLAG_OK)
    return clean_df, quarantine_df


def run_dq_pipeline(df: DataFrame, run_date: date) -> "tuple[DataFrame, DataFrame]":
    """Orchestrate apply_dq_rules -> apply_dedup -> split_quarantine."""
    df = apply_dq_rules(df, run_date)
    df = apply_dedup(df)
    return split_quarantine(df)
