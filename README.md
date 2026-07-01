# recon-lake

PySpark Data Quality and Reconciliation Engine — a batch pipeline that ingests
simulated financial transactions, enforces a strict schema, applies data-
quality rules with a quarantine path, reconciles against a reference bank
ledger, and writes SCD Type-2 curated output to a simulated lakehouse layer
(partitioned Parquet on local disk). A Streamlit dashboard visualizes the
results.

Batch only. No Kafka, no streaming, no cloud services, no Iceberg/Delta.

## Problem statement

Financial transaction feeds are noisy: some records are malformed, some
violate business rules (negative amounts, missing IDs, invalid or future
dates, duplicates), and some don't match the bank's own ledger. This project
demonstrates a production-style pattern for handling that noise in a single
local, reproducible batch pipeline: strict ingestion, quarantine with an
audit trail, reconciliation, and temporal (SCD2) history so that changes to a
transaction over time are never silently overwritten.

## Architecture

```
data/raw/*.json (NDJSON)          data/reference/ledger.json (NDJSON)
        |                                    |
        v                                    v
   src/ingest.py  ------------------>  src/ingest.py
   (strict schema, corrupt-record capture -> quarantine)
        |
        v
   src/transform.py
   (DQ rules: NULL_ID, NEGATIVE_AMOUNT, INVALID_DATE, DUPLICATE -> quarantine)
        |
        v
   src/reconcile.py
   (left join vs ledger -> recon_status: MATCHED / AMOUNT_MISMATCH /
    STATUS_MISMATCH / UNRECONCILED; reconciliation_summary.txt)
        |
        v
   src/load.py
   (SCD Type-2 merge vs existing curated Parquet -> data/curated/,
    partitioned by transaction_date)
        |
        v
   src/dashboard.py  (Streamlit; reads curated + quarantine + summary only)
```

`src/pipeline.py` orchestrates the whole chain end to end via one
`SparkSession`. `src/generate_data.py` produces the synthetic input data.

## Tech stack

Python 3.10 · PySpark 3.5.x · Docker · pytest · Streamlit · pandas · pyarrow.
Input: NDJSON. Output: Parquet.

## Data source

**Option A (synthetic generator), used here.** `src/generate_data.py`
deterministically generates ~200 transaction records and a matching ledger,
seeded via `random.Random(seed)` (not global state) so runs are fully
reproducible. This was chosen over adapting a public dataset (Option B)
because DQ, reconciliation, and SCD2 all require *controlled* defects and
*controlled* changes between runs — a clean public dataset wouldn't have
either, and would need the same injected-defect logic bolted on regardless.

Record composition (`src/generate_data.py`, `N_*` constants):

| Category | Count |
|---|---|
| Clean | 170 |
| `NULL_ID` | 5 |
| `NEGATIVE_AMOUNT` | 5 |
| `INVALID_DATE` (unparseable / future) | 5 |
| Duplicate pairs (same `transaction_id`, different `updated_at`) | 5 pairs (10 rows) |
| Structurally malformed JSON lines | 5 |
| **Total** | **200** |

Ledger breaks (built only from the clean subset): 15 transaction_ids
deliberately absent (`UNRECONCILED`), 8 amount mismatches, 4 status
mismatches, remainder `MATCHED`. The ledger is identical between scenarios —
it represents an unchanging external system snapshot.

`--scenario updated` does **not** read a prior run's output. It regenerates
the identical base 200 records from the same seed, then deterministically
mutates 10 transaction_ids (6 amount changes, 4 status changes) with a
strictly later `updated_at` — this is what the second pipeline run detects
as SCD2 changes.

## Design decisions

- **Strict schemas, corrupt-record capture, not silent drops.** `transaction_date`/`updated_at` are read as `StringType` and only cast/validated in `transform.py`, so a malformed date string becomes a DQ `INVALID_DATE` flag rather than a corrupt-record parse failure. Only structurally-malformed JSON becomes `_corrupt_record`, captured and routed to quarantine as `CORRUPT_JSON`, never dropped.
- **NDJSON, not a JSON array.** One JSON object per line is required so Spark's `columnNameOfCorruptRecord` isolates individual bad records instead of corrupt-flagging the whole file.
- **Single dq_flag per row**, assigned via one `when/otherwise` priority chain (`NULL_ID` > `NEGATIVE_AMOUNT` > `INVALID_DATE` > `OK`), so a row failing multiple rules gets one deterministic flag. Deduplication (`DUPLICATE`) is a separate cross-row pass over already-`OK` rows only.
- **Quarantine location:** `output/quarantine/` (partitioned by `dq_flag`), not nested under `data/curated/` — avoids pandas/pyarrow Hive-partition-discovery surprises when the dashboard reads `data/curated/*`.
- **SCD2 write strategy — full read-merge-rewrite, not partition-overwrite-of-affected-partitions-only.** A changed transaction's business `transaction_date` is unrelated to which run touched it, so it can share a partition with many untouched rows; correctly rewriting only affected partitions would require carefully re-including every co-partitioned untouched row (Spark's dynamic partition overwrite replaces a partition's entire file set). At this project's ~200-row demo scale, reading the whole curated table, computing the full new state in Spark, and overwriting the whole directory is simpler to implement correctly and to verify — at the cost of rewriting more data than a production Delta/Iceberg `MERGE INTO` would need to.
- **Run-timestamp vs business-date.** `effective_start_date`/`effective_end_date` come from an actual wall-clock `run_ts` (captured once in `pipeline.py`, overridable via `--run-ts` for deterministic tests), not a coarse calendar date — so two runs on the same day still produce strictly increasing timestamps and the SCD2 close/open pair is always ordered correctly. `transaction_date` (the partition column) is the transaction's own business date and never changes across a given ID's SCD2 versions.
- **SCD2 sentinel end-date is `2199-12-31`, not `9999-12-31`.** pandas represents timestamps as int64 nanoseconds since the epoch, which overflows for dates beyond ~2262-04-11 (`pandas.Timestamp.max`). Since the dashboard reads curated Parquet through pandas/pyarrow, a literal `9999-12-31` sentinel silently wraps around into a nonsense date (observed: it read back as `1816-03-29`) instead of raising an error. `2199-12-31` is unambiguously "current" for this demo while staying inside pandas' supported range.
- **Partitioning:** curated Parquet is partitioned by `transaction_date`; quarantine Parquet by `dq_flag`.

## How to run (Docker)

```bash
docker build -t recon-lake .

# 1. Generate initial data + run pipeline
docker run --rm -v "$PWD":/app recon-lake python -m src.pipeline --scenario initial --generate

# 2. Regenerate with the "updated" scenario (changed records) + run pipeline again
docker run --rm -v "$PWD":/app recon-lake python -m src.pipeline --scenario updated --generate

# 3. Run tests
docker run --rm -v "$PWD":/app recon-lake pytest tests/ -v

# 4. Launch the dashboard
docker run --rm -p 8501:8501 -v "$PWD":/app recon-lake streamlit run src/dashboard.py --server.address=0.0.0.0
```

Then open `http://localhost:8501`.

### Demo sequence

1. Generate initial data → run pipeline `initial` → curated Parquet created, all `is_current=true`.
2. Reconciliation summary prints (and is written to `output/reconciliation_summary.txt`) showing breaks + quarantine counts.
3. Generate `updated` data (10 mutated transaction_ids) → run pipeline `updated`.
4. Curated table now contains closed historical rows (`is_current=false`) and new current rows for the mutated IDs — proving SCD2 growth.
5. `pytest tests/ -v` — all green, including the SCD2 two-row assertion.
6. `streamlit run src/dashboard.py` — DQ breakdown, reconciliation breaks, and SCD2 history explorer.

### Sample reconciliation summary

```
=== Reconciliation Summary (scenario=updated, run=2026-07-01T14:32:07+00:00) ===
Ingested records:          200
DQ passed:                 175
Quarantined (total):        30
  NULL_ID                     5
  NEGATIVE_AMOUNT              5
  INVALID_DATE                 5
  DUPLICATE                    5
  CORRUPT_JSON                  5
Reconciliation:
  MATCHED                    ...
  AMOUNT_MISMATCH               8
  STATUS_MISMATCH               4
  UNRECONCILED                 15
  --------------------------
  Reconciled (Matched+Mismatch): ...
  Unreconciled:                   15
============================================================
```

### SCD2 before/after

- **Run 1 (`initial`):** every reconciled transaction_id gets exactly one curated row, `is_current=true`, `effective_end_date=2199-12-31` (a far-future sentinel, chosen to stay within pandas' nanosecond-timestamp range so the dashboard doesn't overflow reading it — see Design decisions).
- **Run 2 (`updated`):** for the 10 mutated transaction_ids, the run-1 row is closed (`is_current=false`, `effective_end_date=<run-2 timestamp>`) and a new row is inserted (`is_current=true`, `effective_start_date=<run-2 timestamp>`). Every other transaction_id's row is untouched. Querying the curated table for one of the mutated IDs after run 2 returns exactly 2 rows — proof the history grew.

## Dashboard

`streamlit run src/dashboard.py` reads curated Parquet, quarantine Parquet,
and `output/reconciliation_summary.txt` directly (no SparkSession) and shows:
summary metric cards; a DQ panel (bar chart + table by `dq_flag`); a
reconciliation panel (`MATCHED`/`AMOUNT_MISMATCH`/`STATUS_MISMATCH`/
`UNRECONCILED` breakdown + filterable breaks table); an SCD2 history
explorer (pick a `transaction_id`, see every version ordered by
`effective_start_date`); and a filterable curated data browser
(`transaction_date`, `recon_status`, `is_current`).

## Limitations

- Batch only — no streaming, no Kafka, no cloud services.
- The "lakehouse" is simulated: partitioned Parquet on local disk, not
  Delta Lake/Iceberg. There is no native upsert/MERGE, ACID transaction log,
  or time-travel query engine — SCD2 and full-table-rewrite are implemented
  by hand in `src/load.py` as a deliberate simplification (see Design
  decisions above).
- Demo scale (~200 records); the full-read-merge-rewrite SCD2 strategy would
  not scale to a large curated table without moving to partition-level
  merge or a real table format.
