"""DSQL CDC to Iceberg -- Streamlit Dashboard.

Sidebar: connection info, data generation controls, auto-refresh.
Main page: pipeline overview with key metrics.
"""

import streamlit as st
from lib.config import load_config
from lib.dsql import DsqlConnection
from lib.athena import query_to_dataframe

st.set_page_config(
    page_title="DSQL CDC to Iceberg",
    page_icon=":ice:",
    layout="wide",
)


# -- Initialize config + connections in session_state --
if "config" not in st.session_state:
    try:
        st.session_state.config = load_config()
        st.session_state.connected = True
    except Exception as e:
        st.session_state.config = None
        st.session_state.connected = False
        st.session_state.conn_error = str(e)

if "dsql" not in st.session_state and st.session_state.connected:
    cfg = st.session_state.config
    st.session_state.dsql = DsqlConnection(cfg.cluster_host, cfg.region, cfg.dsql_endpoint)

cfg = st.session_state.config


# -- Sidebar --
with st.sidebar:
    st.title("DSQL CDC to Iceberg")

    if st.session_state.connected:
        st.success("Connected")
    else:
        st.error(f"Not connected: {st.session_state.get('conn_error', 'unknown')}")
        st.stop()

    st.divider()
    st.caption(f"**Region:** {cfg.region}")
    st.caption(f"**Workgroup:** {cfg.athena_workgroup}")
    st.caption(f"**Database:** {cfg.glue_database}")
    st.caption(f"**Cluster:** {cfg.cluster_host}")


# -- Main page --
st.header("Pipeline Overview")

st.markdown("""
**Data flow:** DSQL &rarr; Kinesis &rarr; 2x Firehose &rarr; Iceberg (AWS Glue) &rarr; Athena

Amazon Data Firehose maintains two Iceberg tables from the same Kinesis stream:
- **current_state** -- latest version of each row (merge mode with upserts and tombstone deletes)
- **cdc_events** -- append-only audit trail of every change
""")

# Metrics row
col1, col2, col3 = st.columns(3)

with col1:
    try:
        n = st.session_state.dsql.count_events()
        st.metric("DSQL Events Table", n)
    except Exception as e:
        st.metric("DSQL Events Table", "?")

with col2:
    try:
        df = query_to_dataframe(
            'SELECT count(*) AS cnt FROM "dsql_cdc_iceberg"."current_state" WHERE _is_deleted = false',
            cfg.athena_workgroup, cfg.glue_database, cfg.region,
        )
        val = df.iloc[0]["cnt"] if not df.empty and "cnt" in df.columns else "0"
        st.metric("Iceberg Current State", val)
    except Exception:
        st.metric("Iceberg Current State", "?")

with col3:
    try:
        df = query_to_dataframe(
            'SELECT count(*) AS cnt FROM "dsql_cdc_iceberg"."cdc_events"',
            cfg.athena_workgroup, cfg.glue_database, cfg.region,
        )
        val = df.iloc[0]["cnt"] if not df.empty and "cnt" in df.columns else "0"
        st.metric("Iceberg CDC Events", val)
    except Exception:
        st.metric("Iceberg CDC Events", "?")

st.divider()

# -- Generate Data --
st.subheader("Generate Data")
st.caption("CDC events appear in Iceberg after the Firehose buffer interval (~60s).")

gen_col, mut_col, clear_col = st.columns(3)

with gen_col:
    gen_count = st.number_input("Events to generate", min_value=1, max_value=100, value=10)
    if st.button("Generate Events", type="primary", use_container_width=True):
        with st.spinner("Inserting..."):
            rows = st.session_state.dsql.generate_events(gen_count)
        st.success(f"Inserted {len(rows)} events")

with mut_col:
    mut_count = st.number_input("Events to mutate", min_value=1, max_value=50, value=5)
    if st.button("Mutate Events", use_container_width=True):
        with st.spinner("Mutating..."):
            ops = st.session_state.dsql.mutate_events(mut_count)
        updates = sum(1 for o in ops if o["op"] == "UPDATE")
        deletes = sum(1 for o in ops if o["op"] == "DELETE")
        st.success(f"{updates} updates, {deletes} deletes")

with clear_col:
    st.write("")
    st.write("")
    if st.button("Clear All Events", use_container_width=True):
        with st.spinner("Clearing..."):
            n = st.session_state.dsql.clear_events()
        st.success(f"Deleted {n} events")

st.divider()
col_a, col_b = st.columns(2)
col_a.caption(f"**S3 Bucket:** {cfg.iceberg_bucket_name}")
col_b.caption(f"**Stack:** {cfg.stack_name}")
