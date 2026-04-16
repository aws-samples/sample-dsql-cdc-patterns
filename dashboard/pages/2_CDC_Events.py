"""CDC Events -- append-only audit trail from the cdc_events Iceberg table."""

import streamlit as st
from lib.config import load_config
from lib.athena import query_to_dataframe

st.set_page_config(page_title="CDC Events", layout="wide")
st.header("CDC Events")
st.caption("Every INSERT, UPDATE, and DELETE from DSQL is recorded as an append-only row in the cdc_events table.")

cfg = st.session_state.get("config")
if not cfg:
    st.error("Not connected. Go to the main page first.")
    st.stop()

col1, col2, col3 = st.columns([1, 1, 2])
with col1:
    op_filter = st.selectbox("Operation", ["All", "INSERT (c)", "UPDATE (u)", "DELETE (d)"])
with col2:
    limit = st.number_input("Limit", min_value=10, max_value=1000, value=200, step=50)
with col3:
    auto_refresh = st.toggle("Auto-refresh", value=False, key="cdc_auto")

# Static WHERE clauses keyed by operation — avoids f-string SQL construction (B608)
WHERE_CLAUSES = {
    "All": "",
    "INSERT (c)": "WHERE _cdc_op = 'c'",
    "UPDATE (u)": "WHERE _cdc_op = 'u'",
    "DELETE (d)": "WHERE _cdc_op = 'd'",
}

if op_filter not in WHERE_CLAUSES:
    st.error("Invalid operation filter.")
    st.stop()

BASE_SQL = (
    'SELECT id, name, email, created_at,'
    ' _cdc_op, _cdc_tx_id, _cdc_ts_ms, _cdc_source_table, _cdc_commit_ts_ns'
    ' FROM "dsql_cdc_iceberg"."cdc_events" '
)
where = WHERE_CLAUSES[op_filter]
sql = BASE_SQL + where + " ORDER BY _cdc_commit_ts_ns DESC, _cdc_ts_ms DESC LIMIT " + str(int(limit))

if st.button("Refresh", use_container_width=False):
    pass

with st.spinner("Querying Athena..."):
    df = query_to_dataframe(sql, cfg.athena_workgroup, cfg.glue_database, cfg.region)

if "error" in df.columns:
    st.warning("Query error: " + str(df.iloc[0]["error"]))
elif df.empty:
    st.info("No CDC events yet. Generate and mutate events, then wait for Firehose to flush (~60s).")
else:
    st.metric("Events", len(df))

    OP_LABELS = {"c": "INSERT", "u": "UPDATE", "d": "DELETE"}
    df["operation"] = df["_cdc_op"].map(OP_LABELS).fillna(df["_cdc_op"])

    display_cols = ["operation", "id", "name", "email", "created_at", "_cdc_commit_ts_ns", "_cdc_ts_ms", "_cdc_tx_id"]
    display_df = df[[c for c in display_cols if c in df.columns]]

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "operation": st.column_config.TextColumn("Op", width="small"),
        },
    )

if auto_refresh:
    import time
    time.sleep(10)
    st.rerun()
