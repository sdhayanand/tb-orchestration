"""Shared settings for the OTD DAGs. Values come from Composer environment variables
(set by terraform/modules/composer) or Airflow Variables, with safe defaults."""
from __future__ import annotations

import os
from datetime import timedelta

from airflow.models import Variable

PROJECT_ID = os.environ.get("OTD_PROJECT_ID") or Variable.get("otd_project_id", default_var="crosscutdata-509514")
REGION = os.environ.get("OTD_REGION") or Variable.get("otd_region", default_var="us-central1")
DATASET = Variable.get("otd_dataset", default_var="otd")

DATAFLOW_BUCKET = f"{PROJECT_ID}-tb-otd-dataflow"
LEGACY_EXTRACT_BUCKET = f"{PROJECT_ID}-tb-otd-legacy-extracts"
REPORTS_BUCKET = f"{PROJECT_ID}-tb-otd-reports"
DATAFLOW_SA = f"dataflow-runner@{PROJECT_ID}.iam.gserviceaccount.com"

# legacy OMS admin API (in-cluster service; reachable from Composer through the VPC / an internal LB)
LEGACY_OMS_URL = Variable.get("otd_legacy_oms_url", default_var="http://legacy-oms-soap.legacy.svc.cluster.local:8085")

DEFAULT_ARGS = {
    "owner": "integration-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "execution_timeout": timedelta(hours=1),
}
