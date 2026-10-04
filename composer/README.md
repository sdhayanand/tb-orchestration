# Composer / Airflow DAGs

| DAG | Schedule | What it does |
|---|---|---|
| `daily_order_reconciliation` | 06:00 UTC | Legacy OMS XML extract → GCS → **Dataflow Flex Template** (`DataflowStartFlexTemplateOperator`) → BigQuery gate (`BigQueryCheckOperator`) |
| `migration_reconciliation` | every 4 h | **Cloud Run Job** reconciler (`CloudRunExecuteJobOperator`) → gate on zero gaps between EMS/MQ and Pub/Sub |
| `dlq_watch` | hourly | `PubSubPullOperator` on `events-dlq-monitor` (no ack) → summary → fail if non-empty |

Cloud Scheduler (`terraform/modules/scheduler` in tb-platform-infra and `../scheduler/`) is the
cheap default for the single daily launch; Composer earns its cost when steps have dependencies,
per-step retries and backfills (`airflow dags backfill -s 2026-09-01 -e 2026-09-30 daily_order_reconciliation`).

## Test locally
```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
pytest tests -q
```

## Deploy
`enable_composer = true` in tb-platform-infra, then the `composer-sync` workflow (or by hand):
```bash
gcloud composer environments storage dags import --environment tb-otd-composer --location us-central1 --source composer/dags/
```
