import os
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

st.set_page_config(page_title="Hospital Recon Dashboard", layout="wide")

# Databricks Apps injects DATABRICKS_HOST + OAuth client credentials automatically.
# Strip any scheme/slashes in case the env var is malformed.
_raw_host = os.environ.get("DATABRICKS_HOST", "")
HOST = _raw_host.replace("https://", "").replace("http://", "").strip("/")

HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "")

cfg = Config(host=f"https://{HOST}")

RESULTS_SCHEMA = "hospital_recon_results"


@st.cache_data(ttl=60)
def fetch(query):
    conn = sql.connect(
        server_hostname=HOST,
        http_path=HTTP_PATH,
        credentials_provider=lambda: cfg.authenticate,
    )
    cur = conn.cursor()
    cur.execute(query)
    df = cur.fetchall_arrow().to_pandas()
    cur.close()
    conn.close()
    return df


st.title("Hospital Data Pipeline - Reconciliation Report")
st.write("Comparing Bronze (source) vs Gold (target) for the encounters table - checks row counts, sums, hashes and per-record differences.")

try:
    all_runs = fetch(
        f"SELECT DISTINCT run_id, run_ts FROM {RESULTS_SCHEMA}.reconciliation_summary ORDER BY run_ts DESC"
    )
except Exception as e:
    st.error(f"Couldn't reach the SQL warehouse - {e}")
    st.stop()

if all_runs.empty:
    st.info("No runs yet. Trigger the reconciliation_engine and results_aggregator notebooks first.")
    st.stop()

run_options = all_runs["run_id"].tolist()
selected_run_id = st.selectbox(
    "Select a run to inspect",
    run_options,
    index=0,
    help="Most recent run is selected by default. Pick an older run_id to see its results."
)

summary_for_run = fetch(
    f"SELECT * FROM {RESULTS_SCHEMA}.reconciliation_summary WHERE run_id = '{selected_run_id}' ORDER BY run_ts DESC LIMIT 1"
)
run = summary_for_run.iloc[0]

if selected_run_id == run_options[0]:
    st.caption("Showing the latest run.")
else:
    st.caption(f"Showing a past run from {run['run_ts']}.")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Run ID", str(run["run_id"]))
c2.metric("Checks Run", int(run["total_checks"]))
c3.metric("Passed", int(run["passed"]))
c4.metric("Failed", int(run["failed"]))
c5.metric("Status", "PASS" if run["overall_status"] == "PASS" else "FAIL")

st.divider()
st.subheader("Check breakdown")

checks = fetch(f"""
    SELECT check_type, table_name, metric_name, source_value, target_value, status, mismatch_count
    FROM {RESULTS_SCHEMA}.reconciliation_checks
    WHERE run_id = '{run["run_id"]}'
    ORDER BY check_type
""")

def row_style(r):
    bg = "#d4edda" if r["status"] == "PASS" else "#f8d7da"
    return [f"background-color: {bg}"] * len(r)

st.dataframe(checks.style.apply(row_style, axis=1), use_container_width=True)
st.download_button("Download checks as CSV", checks.to_csv(index=False),
                    file_name=f"checks_{run['run_id']}.csv", mime="text/csv")

st.divider()
st.subheader("Row-level mismatches")

mismatches = fetch(f"""
    SELECT * FROM {RESULTS_SCHEMA}.record_diff_details
    WHERE run_id = '{run["run_id"]}'
    LIMIT 1000
""")

if mismatches.empty:
    st.success("No mismatched rows for this run.")
else:
    st.warning(f"{len(mismatches)} rows differ between source and target.")
    st.dataframe(mismatches, use_container_width=True)
    st.download_button("Download mismatches as CSV", mismatches.to_csv(index=False),
                        file_name=f"mismatches_{run['run_id']}.csv", mime="text/csv")

st.divider()
st.subheader("All runs over time")
st.write("Every reconciliation run ever executed - select any run_id above to inspect its full details.")

history = fetch(f"""
    SELECT run_id, run_ts, total_checks, passed, failed, total_mismatches, overall_status
    FROM {RESULTS_SCHEMA}.reconciliation_summary
    ORDER BY run_ts DESC
""")

st.dataframe(history, use_container_width=True)
st.line_chart(history.set_index("run_ts")[["passed", "failed"]])