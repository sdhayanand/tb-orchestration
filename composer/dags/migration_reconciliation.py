"""
migration_reconciliation
------------------------
Every 4 hours during the TIBCO -> Pub/Sub migration: run the reconciler Cloud Run Job
(tb-tibco-to-pubsub-migration/reconciler) over the last window and record the result in
`otd.migration_reconciliation`; fail when either side is missing messages.
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

from otd_common import DATASET, DEFAULT_ARGS, PROJECT_ID, REGION

with DAG(
    dag_id="migration_reconciliation",
    description="Run the EMS/MQ vs Pub/Sub reconciler and gate on zero gaps",
    schedule="15 */4 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["otd", "migration"],
    doc_md=__doc__,
) as dag:

    run_reconciler = CloudRunExecuteJobOperator(
        task_id="run_reconciler_job",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="tb-reconciler",
        overrides={
            "container_overrides": [{
                "args": [
                    "--from={{ data_interval_start.isoformat() }}",
                    "--to={{ data_interval_end.isoformat() }}",
                    f"--bigQueryDataset={DATASET}",
                ]
            }]
        },
    )

    gate = BigQueryCheckOperator(
        task_id="gate_on_gaps",
        sql=f"""
            SELECT only_legacy = 0 AND only_pubsub = 0
            FROM `{PROJECT_ID}.{DATASET}.migration_reconciliation`
            ORDER BY run_time DESC LIMIT 1
        """,
        use_legacy_sql=False,
        location="US",
    )

    run_reconciler >> gate
