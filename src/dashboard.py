"""Streamlit dashboard for the recon-lake demo.

Reads pipeline OUTPUTS ONLY (curated Parquet, quarantine Parquet, the
reconciliation summary text file) via pandas/pyarrow -- it never starts a
SparkSession or re-runs the pipeline, so it stays lightweight.

    streamlit run src/dashboard.py
"""

import os

import pandas as pd
import streamlit as st

from src import config

st.set_page_config(page_title="recon-lake dashboard", layout="wide")


@st.cache_data
def load_curated(_cache_key: float) -> pd.DataFrame:
    if not (os.path.isdir(config.CURATED_DIR) and os.listdir(config.CURATED_DIR)):
        return pd.DataFrame()
    return pd.read_parquet(config.CURATED_DIR, engine="pyarrow")


@st.cache_data
def load_quarantine(_cache_key: float) -> pd.DataFrame:
    if not (os.path.isdir(config.QUARANTINE_DIR) and os.listdir(config.QUARANTINE_DIR)):
        return pd.DataFrame()
    return pd.read_parquet(config.QUARANTINE_DIR, engine="pyarrow")


def load_summary_text() -> str:
    if not os.path.isfile(config.SUMMARY_FILE):
        return "No reconciliation_summary.txt found yet -- run the pipeline first."
    with open(config.SUMMARY_FILE, encoding="utf-8") as f:
        return f.read()


def _mtime(path: str) -> float:
    if os.path.isdir(path):
        return max((os.path.getmtime(os.path.join(root, f)) for root, _, files in os.walk(path) for f in files), default=0.0)
    return 0.0


def main() -> None:
    st.title("recon-lake -- DQ & Reconciliation Dashboard")

    if st.button("Refresh data"):
        st.cache_data.clear()

    curated_df = load_curated(_mtime(config.CURATED_DIR))
    quarantine_df = load_quarantine(_mtime(config.QUARANTINE_DIR))
    summary_text = load_summary_text()

    # ---- Summary header -------------------------------------------------
    st.subheader("Run Summary")
    lines = summary_text.splitlines()

    def _extract(prefix: str) -> str:
        for line in lines:
            if line.strip().startswith(prefix):
                return line.split(":")[-1].strip()
        return "-"

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Ingested", _extract("Ingested records"))
    col2.metric("DQ Passed", _extract("DQ passed"))
    col3.metric("Quarantined", _extract("Quarantined (total)"))
    col4.metric("Reconciled", _extract("Reconciled (Matched+Mismatch)"))
    col5.metric("Unreconciled", _extract("Unreconciled"))

    with st.expander("Raw reconciliation_summary.txt"):
        st.text(summary_text)

    tab_dq, tab_recon, tab_scd2, tab_curated = st.tabs(
        ["Data Quality", "Reconciliation", "SCD2 History Explorer", "Curated Data Browser"]
    )

    # ---- Data quality panel ---------------------------------------------
    with tab_dq:
        if quarantine_df.empty:
            st.info("No quarantine data found -- run the pipeline first.")
        else:
            st.bar_chart(quarantine_df["dq_flag"].value_counts())
            flags = sorted(quarantine_df["dq_flag"].dropna().unique().tolist())
            selected_flags = st.multiselect("Filter by dq_flag", flags, default=flags)
            st.dataframe(quarantine_df[quarantine_df["dq_flag"].isin(selected_flags)])

    # ---- Reconciliation panel --------------------------------------------
    with tab_recon:
        if curated_df.empty:
            st.info("No curated data found -- run the pipeline first.")
        else:
            st.bar_chart(curated_df["recon_status"].value_counts())
            statuses = sorted(curated_df["recon_status"].dropna().unique().tolist())
            selected_statuses = st.multiselect(
                "Filter recon_status",
                statuses,
                default=[s for s in statuses if s != config.RECON_MATCHED] or statuses,
            )
            st.dataframe(curated_df[curated_df["recon_status"].isin(selected_statuses)])

    # ---- SCD2 history explorer --------------------------------------------
    with tab_scd2:
        if curated_df.empty:
            st.info("No curated data found -- run the pipeline first.")
        else:
            txn_ids = sorted(curated_df["transaction_id"].dropna().unique().tolist())
            selected_id = st.selectbox("transaction_id", txn_ids)
            history = curated_df[curated_df["transaction_id"] == selected_id].sort_values("effective_start_date")
            st.caption(f"{len(history)} version(s) for {selected_id}")
            st.dataframe(
                history[
                    [
                        "effective_start_date",
                        "effective_end_date",
                        "is_current",
                        "amount",
                        "status",
                        "recon_status",
                    ]
                ]
            )
            if len(history) > 1:
                changed_cols = [
                    c
                    for c in ["amount", "status", "recon_status"]
                    if history[c].nunique(dropna=False) > 1
                ]
                if changed_cols:
                    st.write(f"Fields that changed across versions: {', '.join(changed_cols)}")

    # ---- Curated data browser ---------------------------------------------
    with tab_curated:
        if curated_df.empty:
            st.info("No curated data found -- run the pipeline first.")
        else:
            col_a, col_b, col_c = st.columns(3)
            dates = sorted(curated_df["transaction_date"].dropna().astype(str).unique().tolist())
            selected_dates = col_a.multiselect("transaction_date", dates, default=[])
            statuses = sorted(curated_df["recon_status"].dropna().unique().tolist())
            selected_recon = col_b.multiselect("recon_status", statuses, default=[])
            current_only = col_c.checkbox("is_current only", value=True)

            filtered = curated_df
            if selected_dates:
                filtered = filtered[filtered["transaction_date"].astype(str).isin(selected_dates)]
            if selected_recon:
                filtered = filtered[filtered["recon_status"].isin(selected_recon)]
            if current_only:
                filtered = filtered[filtered["is_current"]]
            st.dataframe(filtered)


if __name__ == "__main__":
    main()
