"""Central configuration: paths, explicit Spark schemas, and shared constants."""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

# --------------------------------------------------------------------------
# Paths (relative to project root; override via CLI flags in pipeline.py for
# test isolation)
# --------------------------------------------------------------------------
RAW_DIR = "data/raw"
REFERENCE_DIR = "data/reference"
CURATED_DIR = "data/curated"
OUTPUT_DIR = "output"
QUARANTINE_DIR = "output/quarantine"
SUMMARY_FILE = "output/reconciliation_summary.txt"

# --------------------------------------------------------------------------
# Corrupt-record capture
# --------------------------------------------------------------------------
CORRUPT_RECORD_COL = "_corrupt_record"

# --------------------------------------------------------------------------
# Explicit schemas (no inferSchema). Date/timestamp fields are read as
# strings on purpose: casting/validation happens explicitly in transform.py
# so a bad date string becomes a DQ INVALID_DATE flag, not a silently-nulled
# or corrupt-record parse failure at ingest time.
# --------------------------------------------------------------------------
TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

LEDGER_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("cleared_flag", StringType(), True),
        StructField("cleared_date", StringType(), True),
        StructField("ledger_amount", DoubleType(), True),
        StructField("ledger_status", StringType(), True),
    ]
)

# --------------------------------------------------------------------------
# DQ flags
# --------------------------------------------------------------------------
DQ_FLAG_OK = "OK"
DQ_FLAG_NULL_ID = "NULL_ID"
DQ_FLAG_NEGATIVE_AMOUNT = "NEGATIVE_AMOUNT"
DQ_FLAG_INVALID_DATE = "INVALID_DATE"
DQ_FLAG_DUPLICATE = "DUPLICATE"
DQ_FLAG_CORRUPT_JSON = "CORRUPT_JSON"

ALL_DQ_FLAGS = [
    DQ_FLAG_NULL_ID,
    DQ_FLAG_NEGATIVE_AMOUNT,
    DQ_FLAG_INVALID_DATE,
    DQ_FLAG_DUPLICATE,
    DQ_FLAG_CORRUPT_JSON,
]

# --------------------------------------------------------------------------
# Reconciliation statuses
# --------------------------------------------------------------------------
RECON_MATCHED = "MATCHED"
RECON_AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
RECON_STATUS_MISMATCH = "STATUS_MISMATCH"
RECON_UNRECONCILED = "UNRECONCILED"

# --------------------------------------------------------------------------
# SCD Type-2
# --------------------------------------------------------------------------
SCD2_TRACKED_FIELDS = ["amount", "status", "recon_status"]
# NOTE: pandas represents timestamps as int64 nanoseconds since the epoch,
# which overflows for dates beyond ~2262-04-11 (pandas.Timestamp.max). Since
# the dashboard reads curated Parquet via pandas/pyarrow, a literal
# 9999-12-31 sentinel would silently wrap around into a bogus date on read.
# Use a sentinel far enough in the future to read as "current" in any
# realistic demo, but safely within pandas' supported range.
SCD2_END_DATE_SENTINEL = "2199-12-31 00:00:00"

# Date/timestamp parse formats used by transform.py
TRANSACTION_DATE_FORMAT = "yyyy-MM-dd"
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"
