# DSQL CDC to Iceberg

## Introduction

This sample streams change data capture (CDC) events from Amazon Aurora DSQL to Apache Iceberg tables. Amazon Data Firehose delivers the events. You can query the resulting tables with Amazon Athena and explore the data through a Streamlit dashboard.

The pipeline captures every INSERT, UPDATE, and DELETE from your DSQL database. It delivers these changes to two Iceberg tables: an append-only audit trail and a current-state view with the latest version of each row. Deletes are stored as tombstones so that out-of-order delivery does not resurrect deleted rows. Each row carries a transaction commit timestamp (`_cdc_commit_ts_ns`) for best-effort ordering.

### What you'll learn

- How to set up CDC streaming from Amazon Aurora DSQL to Amazon Kinesis
- How Amazon Data Firehose delivers CDC events to Apache Iceberg tables in two modes (append and merge)
- How to query Iceberg tables with Amazon Athena, including time-travel queries
- How to build a Streamlit dashboard that connects to the deployed pipeline

## Architecture

```
DSQL --> Kinesis --> Firehose (append) --> Iceberg: cdc_events    --> Athena
                \-> Firehose (merge)  --> Iceberg: current_state  --> Athena
```

Amazon Data Firehose maintains two Iceberg tables from the same Kinesis data stream:
- **current_state** -- reflects the latest version of each row through Firehose merge mode (upserts and tombstone deletes)
- **cdc_events** -- append-only audit trail where every change is recorded as a new row, ordered by `_cdc_commit_ts_ns`

See [DSQL CDC to Iceberg architecture](docs/architecture.md) for details.

## What Gets Deployed

| Resource | Service | Purpose |
|----------|---------|---------|
| Kinesis data stream | Amazon Kinesis | Receives CDC events from DSQL |
| Append Firehose | Amazon Data Firehose | Writes every CDC event to `cdc_events` Iceberg table |
| Merge Firehose | Amazon Data Firehose | Applies upserts and tombstone deletes to `current_state` Iceberg table |
| 2x AWS Lambda | AWS Lambda | Transform CDC JSON to Iceberg-compatible format |
| AWS Glue Data Catalog | AWS Glue | `dsql_cdc_iceberg` database with two Iceberg tables |
| S3 bucket | Amazon S3 | Iceberg data warehouse (Parquet files and metadata) |
| Athena workgroup | Amazon Athena | Query engine for Iceberg tables |
| IAM roles | AWS IAM | Least-privilege roles for each service |

Stack name: `DsqlCdcIcebergStack`

## Project Structure

```
deploy.sh               # CloudShell: deploy infrastructure
destroy.sh              # CloudShell: remove all deployed resources
cfn/                    # CloudFormation template
lambda/                 # Lambda transform source (readable copies)
sql/                    # DSQL table DDL + example Athena queries
dashboard/              # Streamlit dashboard (run locally)
  Overview.py           #   Main page + sidebar controls
  pages/                #   Current State, CDC Events, Analytics, Snapshots
  lib/                  #   Config, DSQL connection, Athena helper
  requirements.txt      #   Python dependencies
docs/                   # Architecture docs
```

## Security

- Amazon S3 bucket: `BlockPublicAccess` enabled, SSL-only bucket policy, access logging enabled
- Amazon Kinesis: KMS encryption at rest
- AWS IAM: least-privilege roles scoped to specific resource ARNs
- Amazon Aurora DSQL: SSL connections with IAM auth tokens (15-minute expiry)
- No public endpoints or AWS Lambda function URLs

## Prerequisites

**CloudShell (deploy):**
- Python 3.9+
- pip
- AWS CLI v2.34.61+ (includes the Aurora DSQL CDC stream commands)
- jq 1.6+

**Local (dashboard):**
- Python 3.9+
- pip
- AWS credentials with read access to the deployed CloudFormation stack and Amazon Athena

**Required IAM permissions:** The deploying principal needs permissions for AWS CloudFormation, Amazon Aurora DSQL, Amazon Kinesis, and Amazon Data Firehose. It also needs permissions for AWS Lambda, AWS Glue, Amazon S3, Amazon Athena, AWS IAM, and Amazon CloudWatch Logs. The dashboard user needs permissions to read CloudFormation stack outputs, run Athena queries, and connect to DSQL.

**Estimated cost:** Running this sample for a few hours with approximately 500 CDC events costs less than $1.00 USD. To avoid ongoing charges, follow the [Cleanup](#cleanup) instructions when you are done.

## Quick Start

### Step 1: Deploy (CloudShell)

The deploy script creates a DSQL cluster and deploys the AWS CloudFormation stack. It provisions the Kinesis data stream, Firehose, Lambda, AWS Glue, and Athena resources. It then creates the events table and sets up the CDC stream.

```bash
git clone https://github.com/aws-samples/sample-dsql-cdc-patterns.git && cd sample-dsql-cdc-patterns
bash deploy.sh
```

Deployment takes about 5 minutes. At the end, the script prints the environment variables you need for the dashboard.

Options:
```bash
bash deploy.sh --cluster-id abc123    # reuse existing cluster
bash deploy.sh --region us-west-2     # non-default region
```

### Step 2: Verify deployment

After `deploy.sh` completes, verify that the stack deployed successfully:

```bash
aws cloudformation describe-stacks \
  --stack-name DsqlCdcIcebergStack \
  --region us-east-1 \
  --query 'Stacks[0].StackStatus' \
  --output text
```

The output should be `CREATE_COMPLETE` or `UPDATE_COMPLETE`. You can also verify the CDC stream is active:

```bash
aws dsql get-stream \
  --cluster-identifier <cluster-id> \
  --stream-identifier <stream-id> \
  --endpoint-url "https://dsql.us-east-1.api.aws" \
  --region us-east-1 \
  --query 'status' --output text
```

The output should be `ACTIVE`.

### Step 3: Dashboard (local)

The Streamlit dashboard connects to your deployed pipeline. It lets you generate CDC events, view the current state and audit trail in the Iceberg tables, and explore analytics and time-travel snapshots.

```bash
git clone https://github.com/aws-samples/sample-dsql-cdc-patterns.git && cd sample-dsql-cdc-patterns/dashboard
pip install -r requirements.txt

export REGION="us-east-1"
streamlit run Overview.py
```

The dashboard auto-discovers pipeline settings from the CloudFormation stack. You need `REGION` and valid AWS credentials.

**Dashboard pages:**
- **Overview** -- Pipeline metrics and data generation controls
- **Current State** -- Live view of the Iceberg merge table
- **CDC Events** -- Filterable audit trail with operation highlighting
- **Analytics** -- Operation breakdown pie chart and daily volume bar chart
- **Snapshots** -- Iceberg snapshot listing and time-travel queries

### Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `CLUSTER_HOST not set` | Dashboard cannot discover stack outputs | Verify `REGION` is exported, and your AWS credentials have `cloudformation:DescribeStacks` permission |
| Changeset `FAILED` with `ResourceExistenceCheck` | A previous stack left orphaned resources | Delete the failed stack (`aws cloudformation delete-stack`) and redeploy |
| Athena query returns no data | Firehose buffer has not flushed yet | Wait 60 seconds after generating events, then refresh |
| `ExpiredToken` errors | AWS session credentials expired | Refresh your credentials (re-run `aws sso login` or get new session token) |

## Cleanup

> **Important:** The resources deployed by this sample incur ongoing AWS charges. To avoid unexpected costs, clean up all resources when you are done.

```bash
export REGION="us-east-1" CLUSTER_ID="<id>" STREAM_ID="<id>"
bash destroy.sh
```

The deploy script prints `CLUSTER_ID` and `STREAM_ID` at the end.

The `destroy.sh` script deletes the following resources:
1. **CDC stream** -- deleted through the DSQL API
2. **CloudFormation stack** -- deletes the Kinesis data stream, Amazon Data Firehose delivery streams, and AWS Lambda functions. Also deletes the AWS Glue Data Catalog database and tables, Amazon Athena workgroup, IAM roles, and Amazon CloudWatch Logs log group.
3. **DSQL cluster** -- deletion protection is disabled and the cluster is deleted

**Retained resource:** The Amazon S3 bucket has a `Retain` deletion policy and is not deleted by the stack. This bucket continues to incur storage charges until you delete it manually.

> **Warning:** Deleting the S3 bucket permanently destroys all Iceberg table data, CDC event history, and Athena query results. This action cannot be undone.

```bash
aws s3 rm s3://amzn-s3-demo-bucket --recursive
aws s3 rb s3://amzn-s3-demo-bucket
```

To verify cleanup completed successfully:
```bash
aws cloudformation describe-stacks --stack-name DsqlCdcIcebergStack --region us-east-1
```

This command should return a `StackNotFoundException` error, confirming the stack has been deleted.

## Conclusion

This sample demonstrates an end-to-end CDC pipeline from Amazon Aurora DSQL to Apache Iceberg using Amazon Data Firehose. The two-table strategy provides both historical traceability and real-time analytics. You can extend this pattern by adding Iceberg tables, integrating with other query engines, or connecting downstream consumers to the Kinesis data stream.
