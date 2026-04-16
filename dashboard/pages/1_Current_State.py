"""Current State -- live view of the Iceberg merge table."""

import streamlit as st
from lib.config import load_config
from lib.athena import query_to_dataframe

st.set_page_config(page_title="Current State", layout="wide")
st.header("Current State")
st.caption("The current_state table shows the latest version of each row, maintained via Firehose merge mode. Deleted rows are tombstoned and filtered out.")

cfg = st.session_state.get("config")
if not cfg:
    st.error("Not connected. Go to the main page first.")
    st.stop()

auto_refresh = st.toggle("Auto-refresh", value=False)
col1, col2 = st.columns([1, 4])
with col1:
    if st.button("Refresh", use_container_width=True):
        pass  # triggers rerun
with col2:
    if auto_refresh:
        interval = st.slider("Interval (seconds)", 5, 30, 10)

sql = '''
SELECT id, name, email, created_at, _cdc_commit_ts_ns
FROM "dsql_cdc_iceberg"."current_state"
WHERE _is_deleted = false
ORDER BY _cdc_commit_ts_ns DESC
'''

with st.spinner("Querying Athena..."):
    df = query_to_dataframe(sql, cfg.athena_workgroup, cfg.glue_database, cfg.region)

if "error" in df.columns:
    st.warning(f"Query error: {df.iloc[0]['error']}")
elif df.empty:
    st.info("No data yet. Generate some events from the sidebar, then wait for the Firehose buffer to flush (~60s).")
else:
    st.metric("Rows", len(df))
    st.dataframe(df, use_container_width=True, hide_index=True)

if auto_refresh:
    import time
    time.sleep(interval)
    st.rerun()
