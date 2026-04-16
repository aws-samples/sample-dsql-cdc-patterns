"""Analytics -- operation breakdown and daily volume charts."""

import streamlit as st
import pandas as pd
from lib.athena import query_to_dataframe

st.set_page_config(page_title="Analytics", layout="wide")
st.header("Analytics")

cfg = st.session_state.get("config")
if not cfg:
    st.error("Not connected. Go to the main page first.")
    st.stop()

if st.button("Refresh"):
    pass

# -- Operation distribution --
op_sql = '''
SELECT _cdc_op,
       CASE _cdc_op
           WHEN 'c' THEN 'INSERT'
           WHEN 'u' THEN 'UPDATE'
           WHEN 'd' THEN 'DELETE'
           ELSE 'UNKNOWN'
       END AS operation,
       COUNT(*) AS event_count
FROM "dsql_cdc_iceberg"."cdc_events"
GROUP BY _cdc_op
ORDER BY event_count DESC
'''

# -- Daily volume --
daily_sql = '''
SELECT DATE(FROM_UNIXTIME(CAST(_cdc_ts_ms AS bigint) / 1000)) AS event_date,
       COUNT(*) AS total_events,
       SUM(CASE WHEN _cdc_op = 'c' THEN 1 ELSE 0 END) AS creates,
       SUM(CASE WHEN _cdc_op = 'u' THEN 1 ELSE 0 END) AS updates,
       SUM(CASE WHEN _cdc_op = 'd' THEN 1 ELSE 0 END) AS deletes
FROM "dsql_cdc_iceberg"."cdc_events"
GROUP BY DATE(FROM_UNIXTIME(CAST(_cdc_ts_ms AS bigint) / 1000))
ORDER BY event_date
'''

with st.spinner("Querying Athena..."):
    op_df = query_to_dataframe(op_sql, cfg.athena_workgroup, cfg.glue_database, cfg.region)
    daily_df = query_to_dataframe(daily_sql, cfg.athena_workgroup, cfg.glue_database, cfg.region)

# Metrics
col1, col2, col3 = st.columns(3)
if not op_df.empty and "error" not in op_df.columns:
    op_df["event_count"] = pd.to_numeric(op_df["event_count"], errors="coerce")
    total = int(op_df["event_count"].sum())
    with col1:
        st.metric("Total CDC Events", total)
    for _, row in op_df.iterrows():
        if row["operation"] == "INSERT":
            col2.metric("Inserts", int(row["event_count"]))
        elif row["operation"] == "UPDATE":
            col2.metric("Updates", int(row["event_count"]))
        elif row["operation"] == "DELETE":
            col3.metric("Deletes", int(row["event_count"]))
else:
    st.info("No CDC events yet.")
    st.stop()

st.divider()

# Charts side by side
left, right = st.columns(2)

with left:
    st.subheader("Operation Breakdown")
    import plotly.express as px
    fig = px.pie(
        op_df, values="event_count", names="operation",
        color="operation",
        color_discrete_map={"INSERT": "#2ecc71", "UPDATE": "#3498db", "DELETE": "#e74c3c"},
    )
    fig.update_layout(margin=dict(t=20, b=20, l=20, r=20))
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Daily Volume")
    if not daily_df.empty and "error" not in daily_df.columns:
        for c in ["creates", "updates", "deletes", "total_events"]:
            if c in daily_df.columns:
                daily_df[c] = pd.to_numeric(daily_df[c], errors="coerce")
        fig2 = px.bar(
            daily_df, x="event_date", y=["creates", "updates", "deletes"],
            labels={"value": "Events", "event_date": "Date"},
            color_discrete_map={"creates": "#2ecc71", "updates": "#3498db", "deletes": "#e74c3c"},
            barmode="stack",
        )
        fig2.update_layout(margin=dict(t=20, b=20, l=20, r=20), legend_title_text="")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No daily data yet.")
