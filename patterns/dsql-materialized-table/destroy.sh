#!/usr/bin/env bash
# ============================================================================
# DSQL CDC Materialized Table — Destroy
# ============================================================================
# Removes the pipeline created by deploy.sh.
#
# Reads CLUSTER_ID, STREAM_ID, REGION, and ARTIFACT_BUCKET_NAME from env.sh
# (written by deploy.sh) unless they are already exported.
#
# Usage:
#   bash destroy.sh
#
# Removes (in order):
#   1. CDC stream
#   2. CloudFormation stack (Kinesis, IAM roles, Lambda, SQS DLQ)
#   3. Lambda artifact bucket created by deploy.sh (build artifacts only)
#   4. DSQL cluster
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source env.sh if vars aren't already set
if [[ -z "${CLUSTER_ID:-}" ]] && [[ -f "${SCRIPT_DIR}/env.sh" ]]; then
  echo "Sourcing env.sh..."
  source "${SCRIPT_DIR}/env.sh"
fi

REGION="${REGION:-us-east-1}"
STACK_NAME="${STACK_NAME:-DsqlCdcMaterializedTableStack}"
DSQL_ENDPOINT="https://dsql.${REGION}.api.aws"

if [[ -z "${CLUSTER_ID:-}" ]]; then
  echo "ERROR: CLUSTER_ID is not set. Source env.sh or export it."
  exit 1
fi

echo "=== DSQL CDC Materialized Table Pipeline Cleanup ==="
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

  echo "  Waiting for stack deletion (this may take a few minutes)..."
  aws cloudformation wait stack-delete-complete \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}"
  echo "  Stack deleted."
else
  echo "  Stack does not exist, skipping."
fi
echo ""

# Step 3: Delete the Lambda artifact bucket (layer + code zips only)
if [[ -n "${ARTIFACT_BUCKET_NAME:-}" ]]; then
  echo "--- Step 3: Deleting Lambda artifact bucket ---"
  if aws s3 rb "s3://${ARTIFACT_BUCKET_NAME}" --force --region "${REGION}" >/dev/null 2>&1; then
    echo "  Bucket ${ARTIFACT_BUCKET_NAME} deleted."
  else
    echo "  Bucket may already be deleted"
  fi
else
  echo "--- Step 3: Skipping artifact bucket (ARTIFACT_BUCKET_NAME not set) ---"
fi
echo ""

# Step 4: Delete DSQL cluster
echo "--- Step 4: Deleting DSQL cluster ---"
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
