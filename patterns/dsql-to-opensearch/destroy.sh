#!/usr/bin/env bash
# ============================================================================
# DSQL CDC to OpenSearch Serverless — Destroy
# ============================================================================
# Removes the pipeline created by deploy.sh.
#
# Reads CLUSTER_ID, STREAM_ID, and REGION from env.sh (written by deploy.sh)
# unless they are already exported.
#
# Usage:
#   bash destroy.sh
#
# Removes (in order):
#   1. CDC stream
#   2. CloudFormation stack (VPC, OpenSearch Serverless, Kinesis, Lambda x2,
#      API Gateway, IAM roles, SQS DLQ, CloudWatch)
#   3. DSQL cluster
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source env.sh if vars aren't already set
if [[ -z "${CLUSTER_ID:-}" ]] && [[ -f "${SCRIPT_DIR}/env.sh" ]]; then
  echo "Sourcing env.sh..."
  source "${SCRIPT_DIR}/env.sh"
fi

REGION="${REGION:-us-east-1}"
STACK_NAME="${STACK_NAME:-DsqlCdcOpenSearchStack}"
DSQL_ENDPOINT="https://dsql.${REGION}.api.aws"

if [[ -z "${CLUSTER_ID:-}" ]]; then
  echo "ERROR: CLUSTER_ID is not set. Source env.sh or export it."
  exit 1
fi

echo "=== DSQL CDC to OpenSearch Pipeline Cleanup ==="
echo "  Cluster ID:  ${CLUSTER_ID}"
echo "  Stream ID:   ${STREAM_ID:-not set}"
echo "  Stack:       ${STACK_NAME}"
echo "  Region:      ${REGION}"
echo ""

# Step 1: Delete CDC stream
if [[ -n "${STREAM_ID:-}" ]]; then
  echo "--- Step 1: Deleting CDC stream ---"
  aws dsql delete-stream \
    --cluster-identifier "${CLUSTER_ID}" \
    --stream-identifier "${STREAM_ID}" \
    --endpoint-url "${DSQL_ENDPOINT}" \
    --region "${REGION}" 2>/dev/null || echo "  Stream may already be deleted"

  echo "  Waiting for stream deletion..."
  for _ in $(seq 1 30); do
    STATUS=$(aws dsql get-stream \
      --cluster-identifier "${CLUSTER_ID}" \
      --stream-identifier "${STREAM_ID}" \
      --endpoint-url "${DSQL_ENDPOINT}" \
      --region "${REGION}" \
      --query 'status' --output text 2>/dev/null || echo "DELETED")
    echo "    Status: ${STATUS}"
    [[ "${STATUS}" == "DELETED" || "${STATUS}" == *"not found"* || "${STATUS}" == *"NotFound"* ]] && break
    sleep 10
  done
else
  echo "--- Step 1: Skipping CDC stream (STREAM_ID not set) ---"
fi
echo ""

# Step 2: Delete CloudFormation stack
echo "--- Step 2: Deleting CloudFormation stack ---"
if aws cloudformation describe-stacks --stack-name "${STACK_NAME}" --region "${REGION}" >/dev/null 2>&1; then
  aws cloudformation delete-stack \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}"

  echo "  Waiting for stack deletion (this may take several minutes)..."
  aws cloudformation wait stack-delete-complete \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}"
  echo "  Stack deleted."
else
  echo "  Stack does not exist, skipping."
fi
echo ""

# Step 3: Delete DSQL cluster
echo "--- Step 3: Deleting DSQL cluster ---"
aws dsql update-cluster \
  --identifier "${CLUSTER_ID}" \
  --no-deletion-protection-enabled \
  --endpoint-url "${DSQL_ENDPOINT}" \
  --region "${REGION}" 2>/dev/null || true

aws dsql delete-cluster \
  --identifier "${CLUSTER_ID}" \
  --endpoint-url "${DSQL_ENDPOINT}" \
  --region "${REGION}" 2>/dev/null || echo "  Cluster may already be deleted"

echo "  Cluster deletion initiated."
echo ""
echo "=== Cleanup complete ==="
