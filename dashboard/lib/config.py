"""Load pipeline configuration.

Auto-discovers settings from the CloudFormation stack outputs so the
dashboard works locally with REGION set.

You can also override any value via environment variables.
"""

import os
import re
import subprocess
import json
from dataclasses import dataclass


def _validate_cli_arg(value: str, label: str) -> str:
    """Validate that a CLI argument contains only safe characters."""
    if not re.match(r'^[a-zA-Z0-9._:/-]+$', value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


@dataclass
class Config:
    cluster_host: str
    cluster_id: str
    region: str
    iceberg_bucket_name: str
    athena_workgroup: str
    glue_database: str
    dsql_endpoint: str
    stack_name: str


def _discover_from_cfn(stack_name: str, region: str) -> dict:
    """Read CloudFormation stack outputs to auto-discover pipeline config."""
    try:
        result = subprocess.run(
            ["aws", "cloudformation", "describe-stacks",
             "--stack-name", _validate_cli_arg(stack_name, "stack_name"),
             "--region", _validate_cli_arg(region, "region"),
             "--query", "Stacks[0].Outputs"],
            capture_output=True, text=True, check=True,  # nosec B603
        )
        outputs = json.loads(result.stdout)
        return {o["OutputKey"]: o["OutputValue"] for o in outputs}
    except Exception:
        return {}


def load_config() -> Config:
    """Load config, auto-discovering from CloudFormation stack outputs.

    Required env vars: REGION (defaults to us-east-1)
    Optional env vars: STACK_NAME, CLUSTER_HOST,
                       CLUSTER_ID, ICEBERG_BUCKET_NAME, ATHENA_WORKGROUP,
                       GLUE_DATABASE, DSQL_ENDPOINT
    """
    region = os.environ.get("REGION", os.environ.get("AWS_REGION", "us-east-1"))
    stack_name = os.environ.get("STACK_NAME", "DsqlCdcIcebergStack")

    cluster_host = os.environ.get("CLUSTER_HOST", "")
    cluster_id = os.environ.get("CLUSTER_ID", "")
    iceberg_bucket = os.environ.get("ICEBERG_BUCKET_NAME", "")
    workgroup = os.environ.get("ATHENA_WORKGROUP", "")
    database = os.environ.get("GLUE_DATABASE", "")

    # Auto-discover from CFN if key vars are missing
    if not cluster_host or not iceberg_bucket or not workgroup:
        cfn = _discover_from_cfn(stack_name, region)
        if cfn:
            cluster_host = cluster_host or cfn.get("DsqlClusterHost", "")
            cluster_id = cluster_id or cfn.get("DsqlClusterId", "")
            iceberg_bucket = iceberg_bucket or cfn.get("IcebergBucketName", "")
            workgroup = workgroup or cfn.get("AthenaWorkgroupName", "")
            database = database or cfn.get("GlueDatabaseName", "dsql_cdc_iceberg")

    if not cluster_host:
        raise RuntimeError(
            "Could not discover pipeline configuration.\n\n"
            "Make sure you have:\n"
            "  1. Deployed the stack with deploy.sh in CloudShell\n"
            "  2. Set REGION in your environment\n"
            "  3. Valid AWS credentials that can read the CloudFormation stack\n\n"
            "  export REGION=us-east-1\n"
        )

    dsql_endpoint = os.environ.get(
        "DSQL_ENDPOINT", f"https://dsql.{region}.api.aws"
    )

    return Config(
        cluster_host=cluster_host,
        cluster_id=cluster_id,
        region=region,
        iceberg_bucket_name=iceberg_bucket,
        athena_workgroup=workgroup or "dsql-cdc-iceberg",
        glue_database=database or "dsql_cdc_iceberg",
        dsql_endpoint=dsql_endpoint,
        stack_name=stack_name,
    )
