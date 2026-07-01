"""Synthetic data generator for the recon-lake demo (spec Option A).

Generates ~200 NDJSON transaction records plus a matching reference ledger
CSV-equivalent (NDJSON, per project convention — see README for rationale),
with intentional bad data (nulls, negative amounts, invalid/future dates,
duplicates, structurally-malformed JSON lines) and a subset of clean
transactions deliberately absent from / mismatched against the ledger to
produce reconciliation breaks.

All randomness is drawn from a dedicated `random.Random(seed)` instance (not
global module state) so that generation is fully deterministic and
reproducible: the same `--seed` always regenerates byte-identical base data.
The `--scenario updated` run does NOT read a prior run's output file — it
regenerates the identical base set from scratch and then applies a
deterministic set of mutations (changed amount/status + newer updated_at) to
a handful of existing transaction_ids, which is what the second pipeline run
uses to demonstrate SCD Type-2 history growth.
"""

import argparse
import json
import random
from datetime import datetime, timedelta, timezone

from src import config

# --------------------------------------------------------------------------
# Composition constants (sum to 200 total NDJSON lines in transactions file)
# --------------------------------------------------------------------------
N_CLEAN = 170
N_NULL_ID = 5
N_NEGATIVE_AMOUNT = 5
N_INVALID_DATE = 5
N_DUP_PAIRS = 5  # -> 10 rows
N_CORRUPT = 5
TOTAL_RECORDS = N_CLEAN + N_NULL_ID + N_NEGATIVE_AMOUNT + N_INVALID_DATE + (N_DUP_PAIRS * 2) + N_CORRUPT

N_LEDGER_MISSING = 15  # clean transactions with no ledger entry -> UNRECONCILED
N_LEDGER_AMOUNT_MISMATCH = 8
N_LEDGER_STATUS_MISMATCH = 4

N_MUTATE = 10  # transaction_ids mutated for the "updated" scenario
N_MUTATE_AMOUNT = 6
N_MUTATE_STATUS = N_MUTATE - N_MUTATE_AMOUNT

STATUSES = ["SETTLED", "PENDING", "FAILED"]
CURRENCIES = ["USD", "EUR", "GBP"]
ACCOUNT_POOL_SIZE = 30

RAW_KEY = "__raw_line__"  # sentinel key: value is written verbatim, not json-encoded


def _txn_id(seed: int, index: int) -> str:
    return f"TXN{seed:04d}{index:05d}"


def _account_id(rng: random.Random) -> str:
    return f"ACC{rng.randint(1, ACCOUNT_POOL_SIZE):04d}"


def _iso_date(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def _iso_ts(d: datetime) -> str:
    return d.strftime("%Y-%m-%dT%H:%M:%S")


def generate_base_transactions(seed: int, n: int = TOTAL_RECORDS) -> list:
    """Deterministically generate the full base set of transaction records
    (as plain dicts), tagged internally with `_category` for use by
    `generate_ledger` / `apply_updated_scenario_mutations`. `_category` and
    any other underscore-prefixed helper key is stripped before writing.
    """
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)
    records = []
    idx = 0

    def base_record(category, **overrides):
        nonlocal idx
        days_ago = rng.randint(0, 29)
        rec = {
            "transaction_id": _txn_id(seed, idx),
            "account_id": _account_id(rng),
            "amount": round(rng.uniform(5, 5000), 2),
            "currency": rng.choice(CURRENCIES),
            "transaction_date": _iso_date(now - timedelta(days=days_ago)),
            "status": rng.choice(STATUSES),
            "updated_at": _iso_ts(now - timedelta(days=days_ago, hours=rng.randint(0, 23))),
            "_category": category,
        }
        rec.update(overrides)
        idx += 1
        return rec

    for _ in range(N_CLEAN):
        records.append(base_record("clean"))

    for _ in range(N_NULL_ID):
        records.append(base_record("null_id", transaction_id=None))

    for _ in range(N_NEGATIVE_AMOUNT):
        records.append(base_record("negative_amount", amount=-round(rng.uniform(5, 500), 2)))

    for i in range(N_INVALID_DATE):
        if i < 3:
            records.append(base_record("invalid_date", transaction_date="not-a-date"))
        else:
            future_days = rng.randint(5, 30)
            records.append(
                base_record("invalid_date", transaction_date=_iso_date(now + timedelta(days=future_days)))
            )

    for _ in range(N_DUP_PAIRS):
        shared_id = _txn_id(seed, idx)
        idx += 1
        earlier = now - timedelta(days=rng.randint(10, 20))
        later = earlier + timedelta(hours=rng.randint(1, 48))
        common = {
            "account_id": _account_id(rng),
            "currency": rng.choice(CURRENCIES),
            "transaction_date": _iso_date(earlier),
        }
        records.append(
            {
                "transaction_id": shared_id,
                "amount": round(rng.uniform(5, 5000), 2),
                "status": rng.choice(STATUSES),
                "updated_at": _iso_ts(earlier),
                "_category": "duplicate",
                **common,
            }
        )
        records.append(
            {
                "transaction_id": shared_id,
                "amount": round(rng.uniform(5, 5000), 2),
                "status": rng.choice(STATUSES),
                "updated_at": _iso_ts(later),
                "_category": "duplicate",
                **common,
            }
        )

    corrupt_lines = [
        '{"transaction_id": "TXN%04d99991", "account_id": "ACC0001", "amount":' % seed,
        '{"transaction_id": "TXN%04d99992", "amount": "N/A", "currency": "USD"}extra' % seed,
        "not even json",
        '{"transaction_id": "TXN%04d99994", "account_id": "ACC0002", "amount": 100.0,}' % seed,
        '{{"transaction_id": "TXN%04d99995"}' % seed,
    ]
    for line in corrupt_lines[:N_CORRUPT]:
        records.append({RAW_KEY: line, "_category": "corrupt"})

    assert len(records) == n, f"expected {n} records, generated {len(records)}"
    return records


def generate_ledger(base_transactions: list, seed: int, missing_count: int = N_LEDGER_MISSING) -> list:
    """Build ledger entries mirroring the 'clean' transactions, omitting
    `missing_count` of them entirely (UNRECONCILED breaks) and deliberately
    mismatching a handful of amounts/statuses (AMOUNT_MISMATCH /
    STATUS_MISMATCH breaks). The ledger is identical regardless of scenario
    (it represents an external system snapshot that doesn't change between
    pipeline runs in this demo).
    """
    rng = random.Random(seed + 1)  # distinct sub-stream from transaction generation
    clean = [r for r in base_transactions if r.get("_category") == "clean"]
    rng.shuffle(clean)

    missing = clean[:missing_count]
    amount_mismatch = clean[missing_count : missing_count + N_LEDGER_AMOUNT_MISMATCH]
    status_mismatch = clean[
        missing_count + N_LEDGER_AMOUNT_MISMATCH : missing_count + N_LEDGER_AMOUNT_MISMATCH + N_LEDGER_STATUS_MISMATCH
    ]
    matched = clean[missing_count + N_LEDGER_AMOUNT_MISMATCH + N_LEDGER_STATUS_MISMATCH :]

    missing_ids = {r["transaction_id"] for r in missing}

    ledger = []
    for r in matched:
        ledger.append(
            {
                "transaction_id": r["transaction_id"],
                "cleared_flag": "Y",
                "cleared_date": r["transaction_date"],
                "ledger_amount": r["amount"],
                "ledger_status": r["status"],
            }
        )
    for r in amount_mismatch:
        ledger.append(
            {
                "transaction_id": r["transaction_id"],
                "cleared_flag": "Y",
                "cleared_date": r["transaction_date"],
                "ledger_amount": round(r["amount"] + rng.uniform(10, 100), 2),
                "ledger_status": r["status"],
            }
        )
    for r in status_mismatch:
        other_statuses = [s for s in STATUSES if s != r["status"]]
        ledger.append(
            {
                "transaction_id": r["transaction_id"],
                "cleared_flag": "Y",
                "cleared_date": r["transaction_date"],
                "ledger_amount": r["amount"],
                "ledger_status": rng.choice(other_statuses),
            }
        )
    # `missing_ids` intentionally get no ledger row at all.
    del missing_ids
    return ledger


def apply_updated_scenario_mutations(base_transactions: list, seed: int, mutate_count: int = N_MUTATE) -> list:
    """Return a NEW list (copy) equal to `base_transactions` except that
    `mutate_count` deterministically-chosen clean transaction_ids have a
    changed amount or status and a strictly later `updated_at`.
    """
    rng = random.Random(seed + 2)  # distinct sub-stream
    records = [dict(r) for r in base_transactions]  # shallow copy of each record
    clean_indices = [i for i, r in enumerate(records) if r.get("_category") == "clean"]
    chosen = rng.sample(clean_indices, mutate_count)
    rng.shuffle(chosen)
    amount_targets = set(chosen[:N_MUTATE_AMOUNT])
    status_targets = set(chosen[N_MUTATE_AMOUNT:])

    for i in chosen:
        rec = records[i]
        original_updated_at = datetime.strptime(rec["updated_at"], "%Y-%m-%dT%H:%M:%S")
        rec["updated_at"] = _iso_ts(original_updated_at + timedelta(hours=rng.randint(1, 48)))
        if i in amount_targets:
            rec["amount"] = round(rec["amount"] + rng.uniform(50, 500), 2)
        if i in status_targets:
            other_statuses = [s for s in STATUSES if s != rec["status"]]
            rec["status"] = rng.choice(other_statuses)

    return records


def _strip_helper_keys(record: dict) -> dict:
    return {k: v for k, v in record.items() if not k.startswith("_")}


def write_ndjson(records: list, path: str) -> None:
    """Write one JSON object per line (no enclosing array). Records
    containing the `__raw_line__` sentinel key are written verbatim as
    (deliberately malformed) raw text lines instead of being JSON-encoded.
    """
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            if RAW_KEY in record:
                f.write(record[RAW_KEY] + "\n")
            else:
                f.write(json.dumps(_strip_helper_keys(record)) + "\n")


def main(scenario: str, seed: int = 42) -> None:
    import os

    os.makedirs(config.RAW_DIR, exist_ok=True)
    os.makedirs(config.REFERENCE_DIR, exist_ok=True)

    base = generate_base_transactions(seed)
    ledger = generate_ledger(base, seed)
    write_ndjson(ledger, f"{config.REFERENCE_DIR}/ledger.json")

    if scenario == "updated":
        txns = apply_updated_scenario_mutations(base, seed)
    else:
        txns = base
    write_ndjson(txns, f"{config.RAW_DIR}/transactions_{scenario}.json")

    print(
        f"generate_data: scenario={scenario} seed={seed} "
        f"transactions={len(txns)} ledger_entries={len(ledger)} "
        f"-> {config.RAW_DIR}/transactions_{scenario}.json, {config.REFERENCE_DIR}/ledger.json"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic recon-lake demo data")
    parser.add_argument("--scenario", choices=["initial", "updated"], default="initial")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    main(scenario=args.scenario, seed=args.seed)
