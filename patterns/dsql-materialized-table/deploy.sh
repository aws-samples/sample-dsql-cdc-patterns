#!/usr/bin/env bash
# ============================================================================
# DSQL CDC Materialized Table — Deploy
# ============================================================================
# Run in CloudShell after cloning:
#
#   git clone https://github.com/aws-samples/sample-dsql-cdc-patterns.git
#   cd sample-dsql-cdc-patterns/patterns/dsql-materialized-table
#   bash deploy.sh
#
# Creates a DSQL cluster, builds and uploads the Lambda artifacts, deploys the
# CloudFormation stack (Kinesis, Lambda consumer, SQS DLQ), creates the
# tables, sets up the CDC stream, and launches an interactive demo.
#
# Usage:
#   bash deploy.sh                            # deploy + demo
#   bash deploy.sh --cluster-id abc123        # reuse existing cluster
#   bash deploy.sh --region us-west-2         # non-default region
#   bash deploy.sh --no-demo                  # deploy only, skip interactive demo
#
# Prerequisites: python3, pip3, aws (v2.34.61+), jq, zip
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

STACK_NAME="${STACK_NAME:-DsqlCdcMaterializedTableStack}"
ENV_FILE="${SCRIPT_DIR}/env.sh"

# psycopg2-binary version bundled into the Lambda layer (Python 3.12, x86_64)
PSYCOPG2_VERSION="2.9.10"

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
command -v zip     >/dev/null 2>&1 || missing+=("zip")

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
ARTIFACT_BUCKET_NAME="dsql-cdc-mat-artifacts-${ACCOUNT_ID}-${REGION}"

echo "=== DSQL CDC Materialized Table Pipeline Deployment ==="
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
# Step 2: Build and upload Lambda artifacts
# =========================================================================
echo "--- Step 2: Building Lambda artifacts ---"

if ! aws s3api head-bucket --bucket "${ARTIFACT_BUCKET_NAME}" --region "${REGION}" 2>/dev/null; then
  echo "  Creating artifact bucket: ${ARTIFACT_BUCKET_NAME}"
  if [[ "${REGION}" == "us-east-1" ]]; then
    aws s3api create-bucket \
      --bucket "${ARTIFACT_BUCKET_NAME}" \
      --region "${REGION}" >/dev/null
  else
    aws s3api create-bucket \
      --bucket "${ARTIFACT_BUCKET_NAME}" \
      --region "${REGION}" \
      --create-bucket-configuration LocationConstraint="${REGION}" >/dev/null
  fi
  aws s3api put-public-access-block \
    --bucket "${ARTIFACT_BUCKET_NAME}" \
    --public-access-block-configuration \
      BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true \
    --region "${REGION}"
  aws s3api put-bucket-policy \
    --bucket "${ARTIFACT_BUCKET_NAME}" \
    --region "${REGION}" \
    --policy "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"DenyInsecureTransport\",\"Effect\":\"Deny\",\"Principal\":\"*\",\"Action\":\"s3:*\",\"Resource\":[\"arn:aws:s3:::${ARTIFACT_BUCKET_NAME}\",\"arn:aws:s3:::${ARTIFACT_BUCKET_NAME}/*\"],\"Condition\":{\"Bool\":{\"aws:SecureTransport\":\"false\"}}}]}"
else
  echo "  Artifact bucket exists: ${ARTIFACT_BUCKET_NAME}"
fi

BUILD_DIR=$(mktemp -d)
trap 'rm -rf "${BUILD_DIR}"' EXIT

# psycopg2 layer. Built for the Lambda platform (not the local Python
# version) so the compiled extension loads on python3.12 / x86_64.
LAYER_S3_KEY="layers/psycopg2-binary-${PSYCOPG2_VERSION}-py312-x86_64.zip"
if aws s3api head-object --bucket "${ARTIFACT_BUCKET_NAME}" --key "${LAYER_S3_KEY}" --region "${REGION}" >/dev/null 2>&1; then
  echo "  psycopg2 layer already uploaded."
else
  echo "  Building psycopg2 layer..."
  pip3 install --quiet \
    --target "${BUILD_DIR}/layer/python" \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --python-version 3.12 \
    --only-binary=:all: \
    "psycopg2-binary==${PSYCOPG2_VERSION}"
  (cd "${BUILD_DIR}/layer" && zip -qr "${BUILD_DIR}/layer.zip" python/)
  aws s3 cp "${BUILD_DIR}/layer.zip" "s3://${ARTIFACT_BUCKET_NAME}/${LAYER_S3_KEY}" \
    --region "${REGION}" --quiet
  echo "  psycopg2 layer uploaded."
fi

# Consumer code, keyed by content hash so code changes update the function.
CODE_HASH=$(python3 -c "import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest()[:16])" \
  "${SCRIPT_DIR}/lambda/index.py")
CODE_S3_KEY="code/consumer-${CODE_HASH}.zip"
zip -qj "${BUILD_DIR}/consumer.zip" "${SCRIPT_DIR}/lambda/index.py"
aws s3 cp "${BUILD_DIR}/consumer.zip" "s3://${ARTIFACT_BUCKET_NAME}/${CODE_S3_KEY}" \
  --region "${REGION}" --quiet
echo "  Consumer code uploaded (${CODE_S3_KEY})."
rm -rf "${BUILD_DIR}"
echo ""

# =========================================================================
# Step 3: Deploy CloudFormation stack
# =========================================================================
echo "--- Step 3: Deploying CloudFormation stack ---"

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
    "ArtifactBucketName=${ARTIFACT_BUCKET_NAME}" \
    "LayerS3Key=${LAYER_S3_KEY}" \
    "ConsumerCodeS3Key=${CODE_S3_KEY}" \
  --tags Project=dsql-cdc-samples \
  --region "${REGION}"

echo "  Stack deployed."
echo ""

# =========================================================================
# Step 4: Read stack outputs
# =========================================================================
echo "--- Step 4: Reading stack outputs ---"
STACK_OUTPUTS=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --region "${REGION}" \
  --query 'Stacks[0].Outputs')

get_output() {
  echo "${STACK_OUTPUTS}" | jq -r ".[] | select(.OutputKey==\"$1\") | .OutputValue"
}

KINESIS_STREAM_ARN=$(get_output "KinesisStreamArn")
CDC_ROLE_ARN=$(get_output "CdcRoleArn")
CONSUMER_FUNCTION_NAME=$(get_output "ConsumerFunctionName")
CONSUMER_DLQ_URL=$(get_output "ConsumerDLQUrl")

echo "  Kinesis ARN:     ${KINESIS_STREAM_ARN}"
echo "  CDC Role ARN:    ${CDC_ROLE_ARN}"
echo "  Lambda Function: ${CONSUMER_FUNCTION_NAME}"
echo "  DLQ URL:         ${CONSUMER_DLQ_URL}"
echo ""

# =========================================================================
# Step 5: Create database tables
# =========================================================================
echo "--- Step 5: Creating database tables ---"
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
    lines = [l for l in f if not l.lstrip().startswith("--")]
# DSQL allows one DDL statement per transaction.
with conn.cursor() as cur:
    for stmt in "".join(lines).split(";"):
        if stmt.strip():
            cur.execute(stmt)
conn.close()
print("  Tables ready (orders, daily_order_totals, order_contributions)")
PYEOF
echo ""

# =========================================================================
# Step 6: Wait for IAM role propagation
# =========================================================================
echo "--- Step 6: Waiting for IAM role propagation ---"
echo "  Waiting 15 seconds..."
sleep 15
echo ""

# =========================================================================
# Step 7: Create CDC stream
# =========================================================================
echo "--- Step 7: Creating CDC stream ---"
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
# Step 8: Write env.sh
# =========================================================================
echo "--- Step 8: Writing env.sh ---"
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
export CONSUMER_FUNCTION_NAME="${CONSUMER_FUNCTION_NAME}"
export CONSUMER_DLQ_URL="${CONSUMER_DLQ_URL}"
export ARTIFACT_BUCKET_NAME="${ARTIFACT_BUCKET_NAME}"
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
echo "  Cluster:     ${CLUSTER_HOST}"
echo "  Lambda:      ${CONSUMER_FUNCTION_NAME}"
echo "  CDC Stream:  ${STREAM_ID}"
echo ""

# =========================================================================
# Step 9: Launch demo
# =========================================================================
if $RUN_DEMO; then
  echo "--- Step 9: Starting interactive demo ---"
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
