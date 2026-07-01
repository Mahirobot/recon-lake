"""Reconciliation of DQ-clean transactions against the reference ledger, and
the reconciliation summary (counts) written to console + file.
"""

import os
from datetime import datetime
from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src import config


def reconcile(processed_df: DataFrame, ledger_df: DataFrame) -> DataFrame:
    """Left-join processed (DQ-clean) transactions to the ledger on
    transaction_id and add `recon_status`.

    Priority order (single when/otherwise chain, so exactly one status is
    ever assigned): UNRECONCILED (no ledger row) > AMOUNT_MISMATCH >
    STATUS_MISMATCH > MATCHED. Uses null-safe equality so a mismatch is
    never masked by a NULL comparison.
    """
    joined = processed_df.alias("p").join(ledger_df.alias("l"), on="transaction_id", how="left")

    recon_status = (
        F.when(F.col("l.transaction_id").isNull(), F.lit(config.RECON_UNRECONCILED))
        .when(~F.col("p.amount").eqNullSafe(F.col("l.ledger_amount")), F.lit(config.RECON_AMOUNT_MISMATCH))
        .when(~F.col("p.status").eqNullSafe(F.col("l.ledger_status")), F.lit(config.RECON_STATUS_MISMATCH))
        .otherwise(F.lit(config.RECON_MATCHED))
    )

    return joined.select(
        F.col("p.transaction_id").alias("transaction_id"),
        F.col("p.account_id").alias("account_id"),
        F.col("p.amount").alias("amount"),
        F.col("p.currency").alias("currency"),
        F.col("p.transaction_date").alias("transaction_date"),
        F.col("p.status").alias("status"),
        F.col("p.updated_at").alias("updated_at"),
        recon_status.alias("recon_status"),
    )


def _counts_by_column(df: DataFrame, column: str) -> Dict[str, int]:
    rows = df.groupBy(column).count().collect()
    return {row[column]: row["count"] for row in rows}


def build_summary(
    ingested_total: int,
    quarantine_df: DataFrame,
    reconciled_df: DataFrame,
    run_scenario: str,
    run_ts: datetime,
) -> dict:
    """Compute all counts for the reconciliation summary as a plain dict."""
    quarantined_by_flag = _counts_by_column(quarantine_df, "dq_flag") if quarantine_df is not None else {}
    quarantined_total = sum(quarantined_by_flag.values())

    recon_status_breakdown = _counts_by_column(reconciled_df, "recon_status")
    dq_passed = reconciled_df.count()

    matched = recon_status_breakdown.get(config.RECON_MATCHED, 0)
    amount_mismatch = recon_status_breakdown.get(config.RECON_AMOUNT_MISMATCH, 0)
    status_mismatch = recon_status_breakdown.get(config.RECON_STATUS_MISMATCH, 0)
    unreconciled = recon_status_breakdown.get(config.RECON_UNRECONCILED, 0)

    return {
        "run_scenario": run_scenario,
        "run_timestamp": run_ts.isoformat(),
        "ingested_total": ingested_total,
        "dq_passed": dq_passed,
        "quarantined_total": quarantined_total,
        "quarantined_by_flag": {flag: quarantined_by_flag.get(flag, 0) for flag in config.ALL_DQ_FLAGS},
        "recon_status_breakdown": {
            config.RECON_MATCHED: matched,
            config.RECON_AMOUNT_MISMATCH: amount_mismatch,
            config.RECON_STATUS_MISMATCH: status_mismatch,
            config.RECON_UNRECONCILED: unreconciled,
        },
        "reconciled_total": matched + amount_mismatch + status_mismatch,
        "unreconciled_total": unreconciled,
    }


def format_summary(summary: dict) -> str:
    """Pure string formatter used identically for console print and file
    write, so the two never drift apart."""
    lines = []
    lines.append(
        f"=== Reconciliation Summary (scenario={summary['run_scenario']}, run={summary['run_timestamp']}) ==="
    )
    lines.append(f"Ingested records:       {summary['ingested_total']:>6}")
    lines.append(f"DQ passed:              {summary['dq_passed']:>6}")
    lines.append(f"Quarantined (total):    {summary['quarantined_total']:>6}")
    for flag, count in summary["quarantined_by_flag"].items():
        lines.append(f"  {flag:<20}{count:>6}")
    lines.append("Reconciliation:")
    for status, count in summary["recon_status_breakdown"].items():
        lines.append(f"  {status:<20}{count:>6}")
    lines.append("  " + "-" * 26)
    lines.append(f"  Reconciled (Matched+Mismatch): {summary['reconciled_total']:>6}")
    lines.append(f"  Unreconciled:                  {summary['unreconciled_total']:>6}")
    lines.append("=" * 60)
    return "\n".join(lines)


def write_summary(summary_text: str, path: str = config.SUMMARY_FILE) -> None:
    """Plain Python file I/O (not Spark) — this is a small text report, not
    distributed data. Overwrites each run so the file always reflects the
    latest run only."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(summary_text + "\n")
