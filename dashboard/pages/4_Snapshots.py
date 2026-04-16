"""Snapshots -- Iceberg snapshot listing and time-travel queries."""

import streamlit as st
import pandas as pd
from lib.athena import query_to_dataframe

st.set_page_config(page_title="Snapshots", layout="wide")
st.header("Snapshots")
st.caption("Iceberg maintains a history of snapshots. You can time-travel to any previous state.")

cfg = st.session_state.get("config")
if not cfg:
    st.error("Not connected. Go to the main page first.")
    st.stop()

table = st.selectbox("Table", ["cdc_events", "current_state"])

# Static query map keyed by table name — avoids f-string SQL construction (B608)
SNAPSHOT_QUERIES = {
    "cdc_events": 'SELECT * FROM "dsql_cdc_iceberg"."cdc_events$snapshots" ORDER BY committed_at DESC',
    "current_state": 'SELECT * FROM "dsql_cdc_iceberg"."current_state$snapshots" ORDER BY committed_at DESC',
}
HISTORY_QUERIES = {
    "cdc_events": 'SELECT * FROM "dsql_cdc_iceberg"."cdc_events$history" ORDER BY made_current_at DESC',
    "current_state": 'SELECT * FROM "dsql_cdc_iceberg"."current_state$history" ORDER BY made_current_at DESC',
}
TIME_TRAVEL_TEMPLATES = {
    "cdc_events": 'SELECT * FROM "dsql_cdc_iceberg"."cdc_events" FOR VERSION AS OF ',
    "current_state": 'SELECT * FROM "dsql_cdc_iceberg"."current_state" FOR VERSION AS OF ',
}

if table not in SNAPSHOT_QUERIES:
    st.error("Invalid table name.")
    st.stop()

if st.button("Refresh"):
    pass

# -- Snapshot listing --
st.subheader("Snapshot History")
snap_sql = SNAPSHOT_QUERIES[table]

with st.spinner("Loading snapshots..."):
    snap_df = query_to_dataframe(snap_sql, cfg.athena_workgroup, cfg.glue_database, cfg.region)

if "error" in snap_df.columns:
    st.warning("Error: " + str(snap_df.iloc[0]["error"]))
    st.stop()

if snap_df.empty:
    st.info("No snapshots yet. Data needs to flow through Firehose first.")
    st.stop()

st.dataframe(snap_df, use_container_width=True, hide_index=True)
st.metric("Snapshots", len(snap_df))

# -- Time travel --
st.divider()
st.subheader("Time Travel")

snapshot_ids = snap_df["snapshot_id"].tolist() if "snapshot_id" in snap_df.columns else []
if not snapshot_ids:
    st.info("No snapshot IDs available.")
    st.stop()

selected = st.selectbox("Select snapshot", snapshot_ids)

if st.button("Query Snapshot", type="primary"):
    tt_sql = TIME_TRAVEL_TEMPLATES[table] + str(int(selected))
    with st.spinner("Querying " + table + " at snapshot " + str(selected) + "..."):
        tt_df = query_to_dataframe(tt_sql, cfg.athena_workgroup, cfg.glue_database, cfg.region)
    if "error" in tt_df.columns:
        st.warning("Error: " + str(tt_df.iloc[0]["error"]))
    elif tt_df.empty:
        st.info("No rows at this snapshot.")
    else:
        st.metric("Rows at snapshot", len(tt_df))
        st.dataframe(tt_df, use_container_width=True, hide_index=True)

# -- Table history --
with st.expander("Table History"):
    hist_sql = HISTORY_QUERIES[table]
    with st.spinner("Loading history..."):
        hist_df = query_to_dataframe(hist_sql, cfg.athena_workgroup, cfg.glue_database, cfg.region)
    if not hist_df.empty and "error" not in hist_df.columns:
        st.dataframe(hist_df, use_container_width=True, hide_index=True)
    else:
        st.info("No history available.")
