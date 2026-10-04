"""
dlq_watch
---------
Hourly: pull up to 50 messages from `events-dlq-monitor` without acking, summarise by
`dlqReason` / `originalTopic`, and fail (alert) when anything is there. Complements the
Cloud Monitoring alert with a human-readable summary in the task log.
"""
from __future__ import annotations

import base64
import collections
import logging

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.pubsub import PubSubPullOperator

from otd_common import DEFAULT_ARGS, PROJECT_ID

log = logging.getLogger(__name__)

with DAG(
    dag_id="dlq_watch",
    description="Summarise the dead-letter topic every hour",
    schedule="5 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["otd", "operations"],
    doc_md=__doc__,
) as dag:

    pull = PubSubPullOperator(
        task_id="pull_dlq",
        project_id=PROJECT_ID,
        subscription="events-dlq-monitor",
        max_messages=50,
        ack_messages=False,
    )

    @task
    def summarise(messages: list | None) -> int:
        messages = messages or []
        by_reason = collections.Counter()
        for m in messages:
            attrs = (m.get("message") or {}).get("attributes") or {}
            by_reason[(attrs.get("originalTopic", "?"), attrs.get("dlqStage", "?"), attrs.get("dlqReason", "?")[:80])] += 1
        for (topic, stage, reason), n in by_reason.most_common():
            log.warning("DLQ %-14s %-15s x%-3d %s", topic, stage, n, reason)
        if messages:
            sample = messages[0].get("message", {}).get("data", "")
            log.warning("sample payload: %s", base64.b64decode(sample)[:500] if sample else "")
            raise RuntimeError(f"{len(messages)} message(s) in events-dlq — see summary above")
        log.info("DLQ empty")
        return 0

    summarise(pull.output)
