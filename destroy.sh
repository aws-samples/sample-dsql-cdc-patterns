#!/usr/bin/env bash
# ============================================================================
# DSQL CDC to Iceberg — Destroy
# ============================================================================
# Removes the pipeline created by deploy.sh.
#
# Requires REGION and CLUSTER_ID at minimum. If STREAM_ID or STACK_NAME
# are not set, they'll be discovered or use defaults.
#
# Usage:
#   export REGION="us-east-1" CLUSTER_ID="abc123" STREAM_ID="xyz789"
#   bash destroy.sh
#
# Removes (in order):
#   1. CDC stream
#   2. CloudFormation stack
#   3. DSQL cluster
#
# NOTE: The S3 Iceberg bucket has a RETAIN policy and will NOT be deleted.
# ============================================================================

set -euo pipefail

REGION="${REGION:-us-east-1}"
STACK_NAME="${STACK_NAME:-DsqlCdcIcebergStack}"
DSQL_ENDPOINT="https://dsql.${REGION}.api.aws"

if [[ -z "${CLUSTER_ID:-}" ]]; then
  echo "ERROR: CLUSTER_ID is not set."
  echo "  export CLUSTER_ID=<your-cluster-id>"
  exit 1
fi

echo "=== DSQL CDC Iceberg Pipeline Cleanup ==="
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
echo ""
echo "NOTE: The S3 Iceberg bucket was retained (DeletionPolicy: Retain)."
echo "To delete it manually:"
echo "  aws s3 rm s3://amzn-s3-demo-bucket --recursive"
echo "  aws s3 rb s3://amzn-s3-demo-bucket"
