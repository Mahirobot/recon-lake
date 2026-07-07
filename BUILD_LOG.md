# Build Log: recon-lake

This is a chronological record of how this project was built: every design
decision (and why), every command that was run, every bug that was hit
during verification, and how it was fixed. Follow it top to bottom to
reproduce the project from an empty repo, or use it as a reference for why
the code looks the way it does.

The companion `README.md` documents the *finished* system (architecture,
how to run it, design decisions). This document documents the *process* —
including the two dead ends and the bugs that only showed up when things
were actually run.

---

## 0. Starting state

- Empty git repo at `recon-lake/recon-lake` containing only `README.md`
  (2 lines), `LICENSE`, `.gitignore`. No source code.
- Windows host with **Docker Desktop installed but no local Python and no
  local Java**. This was checked first because it decides the whole
  verification strategy — PySpark cannot run natively on this machine, so
  every run/test/dashboard command has to go through `docker run`:

  ```bash
  python --version      # not found
  java -version         # not found
  docker --version      # OK
  docker info | head -5 # confirms the CLI is installed
  ```

- **Decision: no `lakehouse-dq-engine/` wrapper folder.** The spec's folder
  tree names the project root `lakehouse-dq-engine/`, but a git repo
  already existed at `recon-lake/recon-lake`. Nesting a second project
  root inside an existing repo would be confusing, so `src/`, `tests/`,
  `data/`, `output/` were created directly under the existing repo root.

---

## 1. Folder skeleton

```bash
mkdir -p data/raw data/reference data/curated output src tests
```

Empty `__init__.py` files were added to `src/` and `tests/` so both are
importable as Python packages (`from src import config`, etc.), which is
what lets `tests/test_dq_rules.py` import pipeline modules directly and
what makes `python -m src.pipeline` work as a module invocation.

---

## 2. Design decisions made before writing code

These were worked out up front (in plan mode) because getting them wrong
would mean rewriting multiple modules later. Each is implemented in the
file named.

| Decision | Why | Where |
|---|---|---|
| `transaction_date` / `updated_at` read as `StringType`, not `DateType`/`TimestampType` | If Spark parses these at JSON-read time, a bad date string becomes a corrupt-record parse failure, conflating it with genuinely malformed JSON. Casting/validating explicitly in `transform.py` turns a bad date into a `INVALID_DATE` **DQ flag** instead — a business-rule violation, not an ingestion failure. | `config.py`, `transform.py` |
| Input files are **NDJSON** (one JSON object per line), not a JSON array | Spark's `columnNameOfCorruptRecord` isolates individual bad *lines* in NDJSON. With a single JSON array, one malformed element corrupt-flags the *entire file* instead of just that record. | `generate_data.py::write_ndjson`, `ingest.py` |
| Corrupt-record column must be added to the schema passed to `.schema()` | Spark silently ignores `columnNameOfCorruptRecord` if that column isn't part of the schema — a well-known, easy-to-miss gotcha. | `ingest.py::read_json_strict` |
| Exactly one `dq_flag` per row, via a single `when/otherwise` priority chain (`NULL_ID` > `NEGATIVE_AMOUNT` > `INVALID_DATE` > `OK`) | A row can fail multiple rules at once; a priority order makes the outcome deterministic instead of ambiguous. | `transform.py::apply_dq_rules` |
| Deduplication is a separate pass, over already-`OK` rows only | `Window.partitionBy("transaction_id").orderBy(...)` on a row with a null ID or unparseable date is unsafe/meaningless — those rows are already flagged and excluded from the dedup competition. | `transform.py::apply_dedup` |
| Quarantine lives at `output/quarantine/` (partitioned by `dq_flag`), **not** nested under `data/curated/` | Nesting it under `data/curated/` risks pandas/pyarrow's Hive-partition auto-discovery treating the quarantine subfolder as a bogus partition when the dashboard globs `data/curated/*`. | `config.py::QUARANTINE_DIR` |
| Reconciliation uses 4 statuses (`MATCHED` / `AMOUNT_MISMATCH` / `STATUS_MISMATCH` / `UNRECONCILED`), using **null-safe** equality (`eqNullSafe`) | A richer model than a plain match/no-match binary makes the demo more informative, and null-safe comparison avoids a mismatch being silently masked by a NULL on either side (plain `!=` returns NULL, not `True`, when either side is NULL). | `reconcile.py::reconcile` |
| SCD2 write strategy: **full read-merge-rewrite** of the whole curated table every run, not partition-overwrite-only-affected-partitions | A changed transaction's business `transaction_date` is unrelated to which run touched it, so it can share a partition with many untouched rows. Correctly rewriting only affected partitions would require carefully re-including every co-partitioned untouched row (Spark's dynamic partition overwrite replaces a partition's *entire* file set). At ~200-row demo scale, reading the whole table and rewriting it is simpler to get right. This is a deliberate simplification vs. a production Delta/Iceberg `MERGE INTO`. | `load.py` module docstring |
| `effective_start_date`/`effective_end_date` come from a **wall-clock run timestamp** (`run_ts`, captured once in `pipeline.py`), not a coarse calendar date | Two pipeline runs on the same calendar day still need `closed.effective_end_date > closed.effective_start_date` to hold strictly — a date-only sentinel would collide. `transaction_date` (the partition column) is a separate axis: the transaction's own business date, which never changes across a given ID's SCD2 versions. | `load.py`, `pipeline.py::parse_run_ts` |
| Same `--seed` always regenerates an identical base 200 records from scratch; `--scenario updated` does **not** read the previous run's output file | Keeps both scenarios independently reproducible and testable — `generate_base_transactions(seed=42)` called twice always returns byte-identical data, with no ordering dependency between `initial` and `updated` runs. | `generate_data.py` |
| Dockerfile base: `python:3.10-slim` + manually installed OpenJDK, not `apache/spark-py` | The image needs to run three different things (`pytest`, `streamlit`, and the pipeline) as first-class commands, not just `spark-submit` — a general-purpose Python base gives full control without fighting a cluster-oriented image's assumptions. | `Dockerfile` |

---

## 3. Build order

Written and (where applicable) unit-tested in this order, since each layer
depends only on the ones before it:

1. `src/config.py` — schemas, constants, paths (everything else imports this)
2. `src/ingest.py` + `src/generate_data.py` (independent of each other, both only depend on config)
3. `src/transform.py`
4. `src/reconcile.py`
5. `src/load.py` (the most complex module — SCD2 merge)
6. `src/pipeline.py` (glues 2–5 together via one SparkSession)
7. `tests/test_dq_rules.py`
8. `Dockerfile` + `requirements.txt`
9. `src/dashboard.py` (needs real pipeline output to exist to be meaningfully tested)
10. `README.md`

`requirements.txt` pins:

```
pyspark==3.5.1
pandas==2.2.2
pyarrow==15.0.2
streamlit>=1.30
pytest>=8.0
```

(`pyspark`/`pandas`/`pyarrow` pinned exactly for reproducibility; `streamlit`/`pytest` left loose since they aren't load-bearing for correctness the way schema-sensitive libraries are.)

---

## 4. Commands run, in order, with what happened

All commands below were run from the repo root
(`recon-lake/recon-lake`) in Git Bash. PowerShell equivalents are noted
where the syntax differs.

### 4.1 First build attempt — failed

```bash
docker build -t recon-lake .
```

Failed: `E: Unable to locate package openjdk-17-jdk-headless`. The
`python:3.10-slim` tag had moved to Debian "trixie", which dropped that
package name. **Fix:** pin the base image to a Debian release known to
carry `openjdk-17-jdk-headless`:

```dockerfile
# Dockerfile
- FROM python:3.10-slim
+ FROM python:3.10-slim-bookworm
```

### 4.2 Second build — succeeded

```bash
docker build -t recon-lake .
```

Confirm Java resolves inside the image (the Dockerfile computes
`JAVA_HOME` at build time into `/etc/java_home_path` and an entrypoint
script exports it, rather than hardcoding a path that could shift between
Debian point releases):

```bash
docker run --rm recon-lake sh -c 'echo JAVA_HOME=$JAVA_HOME && java -version'
```

### 4.3 First pipeline run — failed

```bash
docker run --rm -v "$(pwd -W 2>/dev/null || pwd)":/app recon-lake \
  python -m src.pipeline --scenario initial --generate
```

(PowerShell equivalent: `docker run --rm -v ${PWD}:/app recon-lake python -m src.pipeline --scenario initial --generate`)

Failed with:

```
AnalysisException: Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
referenced columns only include the internal corrupt record column...
```

This is a known Spark restriction: you can't query/count a DataFrame read
from JSON/CSV when the *only* column touched is the corrupt-record column,
unless the DataFrame has been materialized first. **Fix** in `ingest.py`:
cache and force a `.count()` on the raw read before splitting it into
clean/corrupt:

```python
raw_df = read_json_strict(spark, path, config.TRANSACTION_SCHEMA).cache()
raw_df.count()
clean_df, corrupt_df = split_corrupt_records(raw_df)
```

### 4.4 Pipeline runs — succeeded

```bash
docker run --rm -v "$(pwd -W 2>/dev/null || pwd)":/app recon-lake \
  python -m src.pipeline --scenario initial --generate
```

Output: 200 ingested (195 parsed + 5 corrupt), 175 DQ-passed, 25 quarantined
(5 each of `NULL_ID`/`NEGATIVE_AMOUNT`/`INVALID_DATE`/`DUPLICATE`/`CORRUPT_JSON`),
155 reconciled / 20 unreconciled, 175 curated rows inserted, all `is_current=true`.

```bash
docker run --rm -v "$(pwd -W 2>/dev/null || pwd)":/app recon-lake \
  python -m src.pipeline --scenario updated --generate
```

Output: same DQ/ingest numbers (ledger and bad-record generation are
scenario-independent by design — see §2), but SCD2 log line reads
`inserted=10 closed=10 unchanged=165 new_ids=0` — exactly the 10
deterministically-mutated transaction_ids from `generate_data.py`.

### 4.5 Manual verification via pandas — surfaced a second bug

```bash
docker run --rm -v "$(pwd -W 2>/dev/null || pwd)":/app recon-lake python -c "
import pandas as pd
df = pd.read_parquet('data/curated', engine='pyarrow')
tid = df.groupby('transaction_id').size().loc[lambda s: s>1].index[0]
print(df[df.transaction_id==tid][['amount','status','recon_status','effective_start_date','effective_end_date','is_current']].sort_values('effective_start_date').to_string())
"
```

The `is_current=true` row's `effective_end_date` printed as
`1816-03-29 05:56:08.066277376` instead of the intended `9999-12-31`
sentinel. **Root cause:** pandas represents timestamps as int64
nanoseconds since the epoch, which overflows for dates beyond
`pandas.Timestamp.max` (~2262-04-11). `9999-12-31` silently wraps around
into garbage the moment pyarrow hands the column to pandas — which is
exactly what the dashboard does. **Fix:** move the sentinel to
`2199-12-31 00:00:00` (`config.py::SCD2_END_DATE_SENTINEL`) — comfortably
inside pandas' supported range, and still unambiguously "far future / not
closed" for this demo. Cleared the previously-written (bad-sentinel) data
and reran both scenarios from scratch:

```bash
rm -rf data/curated/* output/quarantine/*
docker run --rm -v "$(pwd -W 2>/dev/null || pwd)":/app recon-lake python -m src.pipeline --scenario initial --generate
docker run --rm -v "$(pwd -W 2>/dev/null || pwd)":/app recon-lake python -m src.pipeline --scenario updated --generate
```

Re-verified with the same pandas snippet — sentinel now reads back as
`2199-12-31 00:00:00`, correctly.

### 4.6 pytest — failed, then passed

```bash
docker run --rm -v "$(pwd -W 2>/dev/null || pwd)":/app recon-lake pytest tests/ -v
```

6 of 7 passed; `test_scd2_second_run_produces_exactly_two_rows_for_changed_id`
failed with a Spark `SparkFileNotFoundException` on a curated parquet
part-file. **Root cause:** `load.py` reads the existing curated table
*lazily* and then overwrites that same path. Because Spark doesn't
materialize the read until an action forces it, and no action had forced
it before the write began, the write's internal re-evaluation of the read
DAG raced against the overwrite deleting the old part-files — the reader
tried to open a file the writer had just deleted. This didn't reliably
surface with the ~175-row real pipeline data (different task scheduling
at that size happened not to trigger the race) but was reliable with the
tiny single-row test fixture. **Fix:** cache and eagerly materialize the
existing curated DataFrame the moment it's read, so every downstream use
(including whatever the write action re-triggers) is served from memory,
never from a second scan of the path about to be overwritten:

```python
# load.py::read_existing_curated
df = spark.read.parquet(path).cache()
df.count()   # force materialization before this path is later overwritten
return df
```

Reran:

```bash
docker run --rm -v "$(pwd -W 2>/dev/null || pwd)":/app recon-lake pytest tests/ -v
```

All 7 passed. Then re-ran both pipeline scenarios again from a clean
`data/curated`/`output/quarantine` to confirm the fix didn't change
real-run behavior (same counts as §4.5).

### 4.7 Streamlit dashboard

```bash
docker run -d --rm --name recon-lake-dashboard -p 8501:8501 \
  -v "$(pwd -W 2>/dev/null || pwd)":/app recon-lake \
  streamlit run src/dashboard.py --server.address=0.0.0.0 --server.headless=true
```

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8501   # 200
docker logs recon-lake-dashboard | grep -iE "error|exception|traceback"  # nothing
```

An HTTP 200 only proves Streamlit's shell HTML loaded — it doesn't prove
the actual panels rendered without a client-side/Python exception, since
Streamlit renders over a websocket. To actually verify the four dashboard
sections, the `run` skill was invoked, which pointed at driving a headless
browser with `chromium-cli`:

```bash
chromium-cli --session recon-dash <<'EOF'
nav http://localhost:8501
...
EOF
```

`chromium-cli` wasn't available on this machine (`command not found`), so
per the skill's documented fallback, Playwright was installed **locally in
a scratch temp folder** (not in the project) and used directly:

```bash
cd <scratchpad-temp-dir>
npm install playwright --no-save
npx playwright install chromium
node screenshot_dashboard.js   # a small script: chromium.launch -> goto -> screenshot each tab
```

This confirmed, via actual screenshots: summary metric cards, the DQ bar
chart + quarantine table, the reconciliation breakdown + filterable break
table, the SCD2 history explorer (selected a mutated `transaction_id` and
confirmed exactly 2 versions with the expected changed-fields note), and
the curated data browser with filters — all rendered with zero browser
console errors.

```bash
docker stop recon-lake-dashboard
```

---

## 5. Summary of bugs found only by running things (not by reading code)

None of these three were visible from a code review — each only showed up
by actually executing the pipeline/tests/dashboard in Docker, which is why
the verification step mattered:

1. **Corrupt-record-only query restriction** (Spark, `ingest.py`) — fixed with `.cache()` + `.count()` before splitting.
2. **`9999-12-31` sentinel overflowing pandas' nanosecond timestamp range** (`config.py`) — fixed by using `2199-12-31`.
3. **Read-then-overwrite-same-path race** (`load.py`) — fixed by caching the existing curated DataFrame before the write that overwrites its source path.

---

## 6. What was installed on this PC

Nothing was installed system-wide, and nothing was installed inside the
`recon-lake` project itself. Two things *were* installed, both scoped
outside the project:

- **Inside the Docker image** (`recon-lake:latest`, stored in Docker
  Desktop's own image store, not on the Windows filesystem directly):
  `openjdk-17-jdk-headless` + `procps` (apt), and `pyspark`, `pandas`,
  `pyarrow`, `streamlit`, `pytest` (pip). These only exist inside
  containers run from this image — removing the image (`docker rmi
  recon-lake`) removes them entirely.
- **On the Windows host**, for dashboard-screenshot verification only: a
  `node_modules/playwright` package installed via `npm install playwright
  --no-save` inside a Claude scratch/temp directory (not the project
  folder), and `npx playwright install chromium`, which downloaded
  Chromium + a headless shell + ffmpeg + winldd (690 MB total) to
  `C:\Users\Mahira\AppData\Local\ms-playwright\`. This exists because an
  agent has no display of its own to look at a running dashboard with —
  it isn't your installed Chrome, it's a separate pinned browser build
  that Playwright's automation library downloads and drives itself,
  version-matched to its own automation protocol. It has nothing to do
  with testing the app as a human: once the dashboard container is
  running, you just open your own regular browser to
  `http://localhost:8501`. Because of that, both the `ms-playwright`
  cache and the scratchpad's `node_modules`/`package.json` were removed
  after verification was done (`rm -rf` on both paths) — nothing from
  this step remains on the machine.

No PATH changes, no registry changes, no globally-installed npm/pip
packages, no services registered.

---

## 7. How to run the program

**PowerShell** (verified working — the `pwd -W ... || pwd` construct used
in §4's log is Git-Bash-only syntax; PowerShell doesn't have `||` as a
statement separator, so use `${PWD}` instead):

```powershell
# from the repo root: recon-lake/recon-lake

# 1. Build the image (only needed once, or after changing requirements.txt/Dockerfile)
docker build -t recon-lake .

# 2. Generate initial data and run the pipeline
docker run --rm -v "${PWD}:/app" recon-lake python -m src.pipeline --scenario initial --generate

# 3. Regenerate the "updated" scenario (mutates 10 records) and run again — this is what
#    grows the SCD2 history
docker run --rm -v "${PWD}:/app" recon-lake python -m src.pipeline --scenario updated --generate

# 4. Run the test suite
docker run --rm -v "${PWD}:/app" recon-lake pytest tests/ -v

# 5. Launch the dashboard, then open http://localhost:8501 in a browser (Ctrl+C to stop)
docker run --rm -p 8501:8501 -v "${PWD}:/app" recon-lake streamlit run src/dashboard.py --server.address=0.0.0.0
```

**Git Bash / WSL / macOS / Linux:**

```bash
docker build -t recon-lake .
docker run --rm -v "$(pwd -W 2>/dev/null || pwd)":/app recon-lake python -m src.pipeline --scenario initial --generate
docker run --rm -v "$(pwd -W 2>/dev/null || pwd)":/app recon-lake python -m src.pipeline --scenario updated --generate
docker run --rm -v "$(pwd -W 2>/dev/null || pwd)":/app recon-lake pytest tests/ -v
docker run --rm -p 8501:8501 -v "$(pwd -W 2>/dev/null || pwd)":/app recon-lake streamlit run src/dashboard.py --server.address=0.0.0.0
```

(`pwd -W` only exists in Git Bash on Windows, to print a Windows-style
path Docker Desktop understands; the `|| pwd` fallback makes the same
line work unchanged on real Linux/macOS, where `pwd -W` doesn't exist.)

**If you want a completely clean first run** (matching the README's
"before/after" demo exactly — `initial` showing `inserted=175 closed=0`,
then `updated` showing `inserted=10 closed=10`), clear any leftover state
before step 2:

```powershell
Remove-Item -Recurse -Force data\curated\*, output\quarantine\* -ErrorAction SilentlyContinue
```

Every `docker run` mounts the current directory into `/app`, so
`data/raw`, `data/reference`, `data/curated`, and `output/` all persist on
your actual filesystem between runs — nothing is trapped inside the
container.

## 8. Is there a front end?

Yes, but it's a specific kind: `src/dashboard.py` is a **Streamlit
dashboard** — a read-only, single-page web reporting UI, served at
`http://localhost:8501` once you run the command in step 5 above. You
view it in a normal browser. There's:

- no login/auth (none was in scope),
- no separate backend API — the dashboard reads the pipeline's output
  files directly (curated Parquet, quarantine Parquet,
  `output/reconciliation_summary.txt`) via pandas, with no SparkSession
  of its own,
- no way to trigger a pipeline run from the UI — you run the pipeline via
  `docker run ... python -m src.pipeline` separately, then click
  "Refresh data" in the dashboard (or just reload the page) to see the
  new output.

It has four sections: summary metric cards, a data-quality panel (bar
chart + filterable quarantine table), a reconciliation panel (breakdown +
filterable breaks table), an SCD2 history explorer (pick a
`transaction_id`, see every version of it), and a filterable curated data
browser.
