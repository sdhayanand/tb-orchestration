"""
daily_order_reconciliation
--------------------------
06:00 UTC daily:
  1. export yesterday's orders from the legacy OMS (XML extract) into GCS
  2. launch the `daily-reconciliation` Dataflow Flex Template (legacy XML ⨝ BigQuery order_events)
  3. check the result and fail the DAG (→ alert) if mismatches exceed the threshold

This is the Composer equivalent of terraform/modules/scheduler in tb-platform-infra; the two are
alternatives (Cloud Scheduler is the cheap default, Composer when you need the DAG semantics:
dependencies, retries per step, backfills with `airflow dags backfill`).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

from otd_common import (DATAFLOW_BUCKET, DATAFLOW_SA, DATASET, DEFAULT_ARGS, LEGACY_EXTRACT_BUCKET,
                        LEGACY_OMS_URL, PROJECT_ID, REGION, REPORTS_BUCKET)

log = logging.getLogger(__name__)

with DAG(
    dag_id="daily_order_reconciliation",
    description="Legacy OMS extract -> GCS -> Dataflow batch reconciliation -> BigQuery check",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["otd", "dataflow", "reconciliation"],
    doc_md=__doc__,
) as dag:

    @task
    def export_legacy_extract(ds: str) -> str:
        """Pull the OMS XML extract for the business date and land it in GCS.
        Returns the GCS path. Uses the OMS admin endpoint (`GET /admin/export?date=`)."""
        import requests
        from google.cloud import storage

        run_date = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        resp = requests.get(f"{LEGACY_OMS_URL}/admin/export", params={"date": run_date}, timeout=120)
        resp.raise_for_status()
        blob_name = f"extracts/oms-orders-{run_date}.xml"
        storage.Client(project=PROJECT_ID).bucket(LEGACY_EXTRACT_BUCKET).blob(blob_name).upload_from_string(
            resp.content, content_type="application/xml")
        path = f"gs://{LEGACY_EXTRACT_BUCKET}/{blob_name}"
        log.info("legacy extract for %s -> %s (%d bytes)", run_date, path, len(resp.content))
        return path

    extract = export_legacy_extract()

    reconcile = DataflowStartFlexTemplateOperator(
        task_id="run_dataflow_reconciliation",
        project_id=PROJECT_ID,
        location=REGION,
        body={
            "launchParameter": {
                "jobName": "daily-reconciliation-{{ ds_nodash }}",
                "containerSpecGcsPath": f"gs://{DATAFLOW_BUCKET}/templates/daily-reconciliation.json",
                "parameters": {
                    "runDate": "{{ macros.ds_add(ds, -1) }}",
                    "legacyExtractPath": "{{ ti.xcom_pull(task_ids='export_legacy_extract') }}",
                    "bigQueryDataset": DATASET,
                    "reportGcsPath": f"gs://{REPORTS_BUCKET}/reconciliation/{{{{ macros.ds_add(ds, -1) }}}}.csv",
                },
                "environment": {
                    "serviceAccountEmail": DATAFLOW_SA,
                    "tempLocation": f"gs://{DATAFLOW_BUCKET}/temp",
                    "stagingLocation": f"gs://{DATAFLOW_BUCKET}/staging",
                    "maxWorkers": 2,
                    "machineType": "n1-standard-2",
                },
            }
        },
        wait_until_finished=True,
    )

    # Fail the run (and therefore alert) when more than 1% of yesterday's orders mismatched.
    check = BigQueryCheckOperator(
        task_id="check_mismatch_ratio",
        sql=f"""
            SELECT SAFE_DIVIDE(COUNTIF(classification != 'MATCH'), COUNT(*)) <= 0.01
            FROM `{PROJECT_ID}.{DATASET}.order_reconciliation`
            WHERE run_date = '{{{{ macros.ds_add(ds, -1) }}}}'
        """,
        use_legacy_sql=False,
        location="US",
    )

    extract >> reconcile >> check
