#!/usr/bin/env bash
# gcloud equivalent of terraform/modules/scheduler in tb-platform-infra.
set -euo pipefail
PROJECT_ID="${PROJECT_ID:?}"; REGION="${REGION:-us-central1}"
BUCKET="${PROJECT_ID}-tb-otd-dataflow"
BODY=$(cat <<JSON
{"launchParameter":{"jobName":"daily-reconciliation-scheduled",
 "containerSpecGcsPath":"gs://${BUCKET}/templates/daily-reconciliation.json",
 "parameters":{"legacyExtractPath":"gs://${PROJECT_ID}-tb-otd-legacy-extracts/extracts/*.xml","bigQueryDataset":"otd",
               "reportGcsPath":"gs://${PROJECT_ID}-tb-otd-reports/reconciliation/daily-reconciliation.csv"},
 "environment":{"serviceAccountEmail":"dataflow-runner@${PROJECT_ID}.iam.gserviceaccount.com",
               "tempLocation":"gs://${BUCKET}/temp","stagingLocation":"gs://${BUCKET}/staging","maxWorkers":2,"machineType":"n1-standard-2"}}}
JSON
)
gcloud scheduler jobs create http tb-otd-daily-reconciliation-adhoc --project "$PROJECT_ID" --location "$REGION" \
  --schedule="10 6 * * *" --time-zone="Etc/UTC" --http-method=POST \
  --uri="https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/${REGION}/flexTemplates:launch" \
  --headers="Content-Type=application/json" --message-body="$BODY" \
  --oauth-service-account-email="tb-scheduler@${PROJECT_ID}.iam.gserviceaccount.com" \
  --max-retry-attempts=3 --min-backoff=60s --max-backoff=600s
