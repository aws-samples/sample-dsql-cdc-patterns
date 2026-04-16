"""Athena query helper -- run queries and return results as DataFrames."""

import time

import boto3
import pandas as pd


def run_query(
    sql: str,
    workgroup: str,
    database: str = "dsql_cdc_iceberg",
    region: str = "us-east-1",
    timeout: int = 60,
) -> list[dict]:
    """Run an Athena query, poll for completion, return list of dicts."""
    client = boto3.client("athena", region_name=region)

    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )
    qid = resp["QueryExecutionId"]

    deadline = time.time() + timeout
    while time.time() < deadline:
        status = client.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED",):
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(1)
    else:
        raise TimeoutError(f"Athena query timed out after {timeout}s")

    rows = []
    paginator = client.get_paginator("get_query_results")
    first_page = True
    for page in paginator.paginate(QueryExecutionId=qid):
        result_rows = page["ResultSet"]["Rows"]
        if first_page and result_rows:
            headers = [col["VarCharValue"] for col in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
            first_page = False
        for row in result_rows:
            values = [col.get("VarCharValue", "") for col in row["Data"]]
            rows.append(dict(zip(headers, values)))
    return rows


def query_to_dataframe(
    sql: str,
    workgroup: str,
    database: str = "dsql_cdc_iceberg",
    region: str = "us-east-1",
) -> pd.DataFrame:
    """Run an Athena query and return a pandas DataFrame."""
    try:
        rows = run_query(sql, workgroup, database, region)
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})
