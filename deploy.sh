#!/usr/bin/env bash
# ============================================================================
# DSQL CDC to Iceberg — Deploy
# ============================================================================
# Run from the repo root in CloudShell after cloning:
#
#   git clone https://github.com/aws-samples/sample-dsql-cdc-patterns.git && cd sample-dsql-cdc-patterns
#   bash deploy.sh
#
# Creates a DSQL cluster, deploys the CloudFormation stack, creates the
# events table, and sets up the CDC stream.
#
# Usage:
#   bash deploy.sh                            # deploy
#   bash deploy.sh --cluster-id abc123        # reuse existing cluster
#   bash deploy.sh --region us-west-2         # non-default region
#
# Prerequisites: python3, pip3, aws (v2.34.61+), jq
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# -------------------------------------------------------------------------
# Parse arguments
# -------------------------------------------------------------------------
REGION="${REGION:-us-east-1}"
CLUSTER_ID=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --region)         REGION="$2"; shift 2 ;;
    --cluster-id)     CLUSTER_ID="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 [--region REGION] [--cluster-id ID]"
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

DSQL_ENDPOINT="https://dsql.${REGION}.api.aws"
DSQL_HOST_SUFFIX="dsql"

STACK_NAME="${STACK_NAME:-DsqlCdcIcebergStack}"

# Verify repo files exist
if [[ ! -f "${SCRIPT_DIR}/cfn/cdc-iceberg-pipeline.yaml" ]]; then
  echo "ERROR: cfn/cdc-iceberg-pipeline.yaml not found. Run from the repo root."
  exit 1
fi

# =========================================================================
# Check prerequisites
# =========================================================================
echo "--- Checking prerequisites ---"
missing=()
command -v python3 >/dev/null 2>&1 || missing+=("python3")
command -v pip3    >/dev/null 2>&1 || missing+=("pip3")
command -v aws     >/dev/null 2>&1 || missing+=("aws")
command -v jq      >/dev/null 2>&1 || missing+=("jq")

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "ERROR: Missing required tools: ${missing[*]}"
  exit 1
fi

echo "  All prerequisites found."
echo ""

# =========================================================================
# Install Python dependencies (for table creation step)
# =========================================================================
echo "--- Installing Python dependencies ---"
pip3 install --user --quiet psycopg2-binary boto3 2>/dev/null || \
  pip3 install --quiet psycopg2-binary boto3
echo "  Done."
echo ""

# =========================================================================
# Verify DSQL CDC stream commands are available
# =========================================================================
echo "--- Checking AWS CLI for DSQL stream support ---"

if ! aws dsql help 2>&1 | grep -q create-stream; then
  echo "ERROR: 'aws dsql create-stream' is not available in your AWS CLI."
  echo ""
  echo "  Aurora DSQL CDC stream commands require AWS CLI v2.34.61 or later."
  echo "  Your version:"
  echo "    $(aws --version 2>&1)"
  echo ""
  echo "  Upgrade the AWS CLI and re-run: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
  exit 1
fi
echo "  DSQL stream commands available."
echo ""

# =========================================================================
# Resolve AWS account
# =========================================================================
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo "=== DSQL CDC to Iceberg Pipeline Deployment ==="
echo "  Region:     ${REGION}"
echo "  Account:    ${ACCOUNT_ID}"
echo ""

# =========================================================================
# Step 1: DSQL cluster
# =========================================================================
if [[ -z "$CLUSTER_ID" ]]; then
  echo "--- Step 1: Creating DSQL cluster ---"
  CLUSTER_OUTPUT=$(aws dsql create-cluster \
    --region "${REGION}" \
    --endpoint-url "${DSQL_ENDPOINT}")

  CLUSTER_ID=$(echo "${CLUSTER_OUTPUT}" | jq -r '.identifier')
  echo "  Cluster ID: ${CLUSTER_ID}"

  echo "  Waiting for cluster to be ACTIVE..."
  MAX_WAIT=60
  WAITED=0
  while true; do
    STATUS=$(aws dsql get-cluster \
      --identifier "${CLUSTER_ID}" \
      --region "${REGION}" \
      --endpoint-url "${DSQL_ENDPOINT}" \
      --query 'status' --output text)
    echo "    Status: ${STATUS}"
    [[ "${STATUS}" == "ACTIVE" ]] && break
    WAITED=$((WAITED + 1))
    if [[ $WAITED -ge $MAX_WAIT ]]; then
      echo "ERROR: Timed out waiting for cluster to become ACTIVE"
      exit 1
    fi
    sleep 10
  done
else
  echo "--- Step 1: Using existing DSQL cluster ---"
  echo "  Cluster ID: ${CLUSTER_ID}"
fi

CLUSTER_HOST="${CLUSTER_ID}.${DSQL_HOST_SUFFIX}.${REGION}.on.aws"
echo "  Cluster Host: ${CLUSTER_HOST}"
echo ""

# =========================================================================
# Step 2: Deploy CloudFormation stack
# =========================================================================
echo "--- Step 2: Deploying CloudFormation stack ---"

# Look up default VPC and subnets for Lambda VPC config
echo "  Discovering default VPC..."
VPC_ID=$(aws ec2 describe-vpcs \
  --filters Name=isDefault,Values=true \
  --region "${REGION}" \
  --query 'Vpcs[0].VpcId' --output text)

if [[ -z "${VPC_ID}" || "${VPC_ID}" == "None" ]]; then
  echo "ERROR: No default VPC found in ${REGION}."
  echo "  Create a default VPC with: aws ec2 create-default-vpc --region ${REGION}"
  exit 1
fi

SUBNET_IDS=$(aws ec2 describe-subnets \
  --filters Name=vpc-id,Values="${VPC_ID}" \
  --region "${REGION}" \
  --query 'Subnets[*].SubnetId' --output text | tr '\t' ',')

VPC_CIDR=$(aws ec2 describe-vpcs \
  --vpc-ids "${VPC_ID}" \
  --region "${REGION}" \
  --query 'Vpcs[0].CidrBlock' --output text)

echo "  VPC: ${VPC_ID}"
echo "  CIDR: ${VPC_CIDR}"
echo "  Subnets: ${SUBNET_IDS}"
echo ""

STACK_STATUS=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --region "${REGION}" \
  --query 'Stacks[0].StackStatus' \
  --output text 2>/dev/null || echo "DOES_NOT_EXIST")

if [[ "${STACK_STATUS}" == "ROLLBACK_COMPLETE" || "${STACK_STATUS}" == "REVIEW_IN_PROGRESS" ]]; then
  echo "  Cleaning up failed stack (${STACK_STATUS})..."
  aws cloudformation delete-stack \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}"
  aws cloudformation wait stack-delete-complete \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}"
fi

aws cloudformation deploy \
  --template-file "${SCRIPT_DIR}/cfn/cdc-iceberg-pipeline.yaml" \
  --stack-name "${STACK_NAME}" \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset \
  --parameter-overrides \
    "DsqlClusterId=${CLUSTER_ID}" \
    "VpcId=${VPC_ID}" \
    "SubnetIds=${SUBNET_IDS}" \
    "VpcCidr=${VPC_CIDR}" \
  --tags Project=dsql-cdc-iceberg \
  --region "${REGION}"

echo "  Stack deployed."
echo ""

# =========================================================================
# Step 3: Read stack outputs
# =========================================================================
echo "--- Step 3: Reading stack outputs ---"
STACK_OUTPUTS=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --region "${REGION}" \
  --query 'Stacks[0].Outputs')

get_output() {
  echo "${STACK_OUTPUTS}" | jq -r ".[] | select(.OutputKey==\"$1\") | .OutputValue"
}

KINESIS_STREAM_ARN=$(get_output "KinesisStreamArn")
CDC_ROLE_ARN=$(get_output "CdcRoleArn")
ICEBERG_BUCKET_NAME=$(get_output "IcebergBucketName")
ATHENA_WORKGROUP=$(get_output "AthenaWorkgroupName")
GLUE_DATABASE=$(get_output "GlueDatabaseName")

echo "  Kinesis ARN:      ${KINESIS_STREAM_ARN}"
echo "  CDC Role ARN:     ${CDC_ROLE_ARN}"
echo "  Iceberg Bucket:   ${ICEBERG_BUCKET_NAME}"
echo "  Athena Workgroup: ${ATHENA_WORKGROUP}"
echo "  AWS Glue database: ${GLUE_DATABASE}"
echo ""

# =========================================================================
# Step 4: Create events table
# =========================================================================
echo "--- Step 4: Creating events table ---"
DB_TOKEN=$(aws dsql generate-db-connect-admin-auth-token \
  --hostname "${CLUSTER_HOST}" \
  --region "${REGION}" \
  --endpoint-url "${DSQL_ENDPOINT}")

CLUSTER_HOST="${CLUSTER_HOST}" DB_TOKEN="${DB_TOKEN}" python3 << 'PYEOF'
import os, psycopg2
conn = psycopg2.connect(
    host=os.environ["CLUSTER_HOST"], port=5432, dbname="postgres",
    user="admin", password=os.environ["DB_TOKEN"], sslmode="require",
)
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.events (
            id         UUID DEFAULT gen_random_uuid() NOT NULL,
            name       VARCHAR(100) NOT NULL,
            email      VARCHAR(200) NOT NULL,
            created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
            PRIMARY KEY (id)
        )
    """)
conn.close()
print("  Events table ready")
PYEOF
echo ""

# =========================================================================
# Step 5: Wait for IAM role propagation
# =========================================================================
echo "--- Step 5: Waiting for IAM role propagation ---"
echo "  Waiting 15 seconds..."
sleep 15
echo ""

# =========================================================================
# Step 6: Create CDC stream
# =========================================================================
echo "--- Step 6: Creating CDC stream ---"
STREAM_OUTPUT=$(aws dsql create-stream \
  --cluster-identifier "${CLUSTER_ID}" \
  --target-definition "{\"kinesis\":{\"streamArn\":\"${KINESIS_STREAM_ARN}\",\"roleArn\":\"${CDC_ROLE_ARN}\"}}" \
  --ordering UNORDERED \
  --format JSON \
  --endpoint-url "${DSQL_ENDPOINT}" \
  --region "${REGION}")

STREAM_ID=$(echo "${STREAM_OUTPUT}" | jq -r '.streamIdentifier')
echo "  Stream ID: ${STREAM_ID}"

echo "  Waiting for CDC stream to be ACTIVE..."
MAX_WAIT=60
WAITED=0
while true; do
  STATUS=$(aws dsql get-stream \
    --cluster-identifier "${CLUSTER_ID}" \
    --stream-identifier "${STREAM_ID}" \
    --endpoint-url "${DSQL_ENDPOINT}" \
    --region "${REGION}" \
    --query 'status' --output text)
  echo "    Status: ${STATUS}"
  [[ "${STATUS}" == "ACTIVE" ]] && break
  WAITED=$((WAITED + 1))
  if [[ $WAITED -ge $MAX_WAIT ]]; then
    echo "ERROR: Timed out waiting for CDC stream to become ACTIVE"
    exit 1
  fi
  sleep 10
done
echo ""

# =========================================================================
# Done
# =========================================================================
echo "=========================================="
echo "  Deployment complete!"
echo "=========================================="
echo ""
echo "  Cluster:     ${CLUSTER_HOST}"
echo "  Bucket:      ${ICEBERG_BUCKET_NAME}"
echo "  Workgroup:   ${ATHENA_WORKGROUP}"
echo "  CDC Stream:  ${STREAM_ID}"
echo ""
echo "  -- Run the dashboard locally --"
echo ""
echo "  Copy these environment variables to your local machine:"
echo ""
echo "    export REGION=\"${REGION}\""
echo "    export STACK_NAME=\"${STACK_NAME}\""
echo ""
echo "  Then:"
echo ""
echo "    git clone https://github.com/aws-samples/sample-dsql-cdc-patterns.git && cd sample-dsql-cdc-patterns/dashboard"
echo "    pip install -r requirements.txt"
echo "    streamlit run Overview.py"
echo ""
echo "  The dashboard auto-discovers all other settings from the"
echo "  CloudFormation stack. You need REGION and valid AWS credentials."
echo ""
echo "  -- Destroy everything --"
echo ""
echo "    export REGION=\"${REGION}\" CLUSTER_ID=\"${CLUSTER_ID}\" STREAM_ID=\"${STREAM_ID}\""
echo "    bash destroy.sh"
echo ""
