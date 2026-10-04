# tb-orchestration — scheduling and test suites for the OTD platform

Part of the Tailored Brands Order-to-Delivery integration platform
([architecture](https://github.com/sdhayanand/tb-platform-infra/blob/main/docs/ARCHITECTURE.md)).
This repo holds what a Sr Integration Engineer owns besides the services: **when things run** and
**how they are proven**.

| Folder | Posting requirement | Content |
|---|---|---|
| `composer/` | Cloud Composer | 3 Airflow DAGs (Dataflow Flex Template launch, Cloud Run Job, Pub/Sub DLQ watch) + integrity tests (run in CI with Airflow 2.9.3) |
| `scheduler/` | Cloud Scheduler | `gcloud` equivalents of the Terraform-managed job that launches the daily reconciliation template |
| `postman/` | Postman | Collection (health, REST orders incl. validation errors, SOAP + XSLT, inventory, HMAC-signed carrier webhook, legacy simulators) + local/gcp environments; run headless with Newman in CI |
| `jmeter/` | JMeter | CSV-driven load test for `POST /v1/orders` with 201/JSON/duration assertions; CI runs a smoke profile and publishes the HTML report |
| `soapui/` | SoapUI | Contract tests against the legacy OMS WSDL (schema compliance, faults, SLA) and the SOAP adapter of the new platform |

## Run locally
```bash
# whole platform first (tb-platform-infra/local): docker compose up -d
newman run postman/tb-otd.postman_collection.json -e postman/local.postman_environment.json --delay-request 1500
jmeter -n -t jmeter/order-intake-load.jmx -l results.jtl -e -o report -Jusers=20 -Jloops=20
cd composer && python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements-dev.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt && pytest tests -q
```

## CI
`ci.yml`: DAG integrity (real Airflow import) → file validation → Newman + JMeter + SoapUI against the
whole platform stack pulled from GHCR. `composer-sync.yml` uploads DAGs to Composer when the
environment exists (`vars.COMPOSER_ENV`).

## What to say in the interview
1. "Scheduler vs Composer is a cost/complexity decision: one daily launch is a Scheduler job hitting the Dataflow REST API with an OAuth service account; when a run has steps, retries per step and backfills, that is a DAG."
2. "The DAG gates on data, not on job success: `BigQueryCheckOperator` fails the run if more than 1% of yesterday's orders mismatch, which is what turns a pipeline into a control."
3. "Postman is the contract test, JMeter is the capacity test, SoapUI is the legacy contract test — three tools because they answer three different questions."
4. "The HMAC in the Postman pre-request script is the same secret and algorithm the carrier would use; the suite proves both the happy path and that a bad signature is rejected."
5. "The JMeter plan asserts latency per request, not just the average, because the SLO is p95."
