#!/usr/bin/env bash
# Force a run of the Terraform-managed job (useful after deploying a new template).
set -euo pipefail
PROJECT_ID="${PROJECT_ID:?}"; REGION="${REGION:-us-central1}"
gcloud scheduler jobs run tb-otd-daily-reconciliation --project "$PROJECT_ID" --location "$REGION"
sleep 20
gcloud dataflow jobs list --project "$PROJECT_ID" --region "$REGION" --filter="name~daily-reconciliation" --limit 3
