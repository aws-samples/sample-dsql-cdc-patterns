#!/usr/bin/env bash
# ============================================================================
# DSQL CDC to Amazon S3 Vectors — Deploy
# ============================================================================
# Run in CloudShell after cloning:
#
#   git clone https://github.com/aws-samples/sample-dsql-cdc-patterns.git
#   cd sample-dsql-cdc-patterns/patterns/dsql-to-s3-vectors
#   bash deploy.sh
#
# Creates a DSQL cluster, deploys the CloudFormation stack (S3 vector bucket
# and index, Kinesis, Lambda CDC consumer with Bedrock embeddings, SQS DLQ),
# creates the products table, sets up the CDC stream, and launches an
# interactive demo.
#
# Usage:
#   bash deploy.sh                            # deploy + demo
#   bash deploy.sh --cluster-id abc123        # reuse existing cluster
#   bash deploy.sh --region us-west-2         # non-default region
#   bash deploy.sh --no-demo                  # deploy only, skip interactive demo
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
RUN_DEMO=true

while [[ $# -gt 0 ]]; do
  case $1 in
    --region)         REGION="$2"; shift 2 ;;
    --cluster-id)     CLUSTER_ID="$2"; shift 2 ;;
    --no-demo)        RUN_DEMO=false; shift ;;
    -h|--help)
      echo "Usage: $0 [--region REGION] [--cluster-id ID] [--no-demo]"
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

DSQL_ENDPOINT="https://dsql.${REGION}.api.aws"
DSQL_HOST_SUFFIX="dsql"

STACK_NAME="${STACK_NAME:-DsqlCdcS3VectorsStack}"
ENV_FILE="${SCRIPT_DIR}/env.sh"

# Verify pattern files exist
if [[ ! -f "${SCRIPT_DIR}/cfn/cdc-pipeline.yaml" ]]; then
  echo "ERROR: cfn/cdc-pipeline.yaml not found next to deploy.sh."
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
# Install Python dependencies (for table creation and the demo)
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

echo "=== DSQL CDC to S3 Vectors Pipeline Deployment ==="
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
  --template-file "${SCRIPT_DIR}/cfn/cdc-pipeline.yaml" \
  --stack-name "${STACK_NAME}" \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset \
  --parameter-overrides \
    "DsqlClusterId=${CLUSTER_ID}" \
  --tags Project=dsql-cdc-samples \
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
VECTOR_BUCKET_NAME=$(get_output "VectorBucketName")
INDEX_NAME=$(get_output "VectorIndexName")

echo "  Kinesis ARN:    ${KINESIS_STREAM_ARN}"
echo "  CDC Role ARN:   ${CDC_ROLE_ARN}"
echo "  Vector Bucket:  ${VECTOR_BUCKET_NAME}"
echo "  Vector Index:   ${INDEX_NAME}"
echo ""

# =========================================================================
# Step 4: Create products table
# =========================================================================
echo "--- Step 4: Creating products table ---"
DB_TOKEN=$(aws dsql generate-db-connect-admin-auth-token \
  --hostname "${CLUSTER_HOST}" \
  --region "${REGION}" \
  --endpoint-url "${DSQL_ENDPOINT}")

CLUSTER_HOST="${CLUSTER_HOST}" DB_TOKEN="${DB_TOKEN}" SCHEMA_FILE="${SCRIPT_DIR}/sql/schema.sql" python3 << 'PYEOF'
import os, psycopg2
conn = psycopg2.connect(
    host=os.environ["CLUSTER_HOST"], port=5432, dbname="postgres",
    user="admin", password=os.environ["DB_TOKEN"], sslmode="require",
)
conn.autocommit = True
with open(os.environ["SCHEMA_FILE"]) as f:
    lines = [l for l in f.read().splitlines() if not l.strip().startswith("--")]
with conn.cursor() as cur:
    # DSQL allows one DDL statement per transaction.
    for stmt in "\n".join(lines).split(";"):
        if stmt.strip():
            cur.execute(stmt)
conn.close()
print("  Products table ready")
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
# Step 7: Write env.sh
# =========================================================================
echo "--- Step 7: Writing env.sh ---"
cat > "${ENV_FILE}" << EOF
#!/usr/bin/env bash
# Generated by deploy.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)
# Usage: source env.sh

export REGION="${REGION}"
export DSQL_ENDPOINT="${DSQL_ENDPOINT}"
export STACK_NAME="${STACK_NAME}"
export ACCOUNT_ID="${ACCOUNT_ID}"
export CLUSTER_ID="${CLUSTER_ID}"
export CLUSTER_HOST="${CLUSTER_HOST}"
export KINESIS_STREAM_ARN="${KINESIS_STREAM_ARN}"
export CDC_ROLE_ARN="${CDC_ROLE_ARN}"
export VECTOR_BUCKET_NAME="${VECTOR_BUCKET_NAME}"
export INDEX_NAME="${INDEX_NAME}"
export STREAM_ID="${STREAM_ID}"
EOF

echo "  Written to ${ENV_FILE}"
echo ""

# =========================================================================
# Done
# =========================================================================
echo "=========================================="
echo "  Deployment complete!"
echo "=========================================="
echo ""
echo "  Cluster:        ${CLUSTER_HOST}"
echo "  Vector Bucket:  ${VECTOR_BUCKET_NAME}"
echo "  Vector Index:   ${INDEX_NAME}"
echo "  CDC Stream:     ${STREAM_ID}"
echo ""

# =========================================================================
# Step 8: Launch demo
# =========================================================================
if $RUN_DEMO; then
  echo "--- Step 8: Starting interactive demo ---"
  echo ""
  source "${ENV_FILE}"
  exec python3 "${SCRIPT_DIR}/demo.py"
else
  echo "  To run the demo later:"
  echo "    source ${ENV_FILE}"
  echo "    python3 ${SCRIPT_DIR}/demo.py"
  echo ""
  echo "  To destroy everything:"
  echo "    bash ${SCRIPT_DIR}/destroy.sh"
  echo ""
fi
