"""DAG integrity tests: every DAG file imports, has an owner, retries and no cycles,
and the schedules/task graphs are what the docs say. Runs in CI with a real Airflow install."""
import os
import pathlib

import pytest
from airflow.models import DagBag

DAGS = pathlib.Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="session")
def dagbag():
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    os.environ.setdefault("OTD_PROJECT_ID", "test-project")
    os.environ.setdefault("OTD_REGION", "us-central1")
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"import errors: {dagbag.import_errors}"


@pytest.mark.parametrize("dag_id,n_tasks,schedule", [
    ("daily_order_reconciliation", 3, "0 6 * * *"),
    ("migration_reconciliation", 2, "15 */4 * * *"),
    ("dlq_watch", 2, "5 * * * *"),
])
def test_dag_shape(dagbag, dag_id, n_tasks, schedule):
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"{dag_id} missing"
    assert len(dag.tasks) == n_tasks
    assert str(dag.schedule_interval) == schedule
    assert dag.default_args["owner"] == "integration-engineering"
    assert dag.default_args["retries"] >= 1
    assert not dag.catchup


def test_reconciliation_dependencies(dagbag):
    dag = dagbag.get_dag("daily_order_reconciliation")
    t = {x.task_id: x for x in dag.tasks}
    assert t["run_dataflow_reconciliation"].upstream_task_ids == {"export_legacy_extract"}
    assert t["check_mismatch_ratio"].upstream_task_ids == {"run_dataflow_reconciliation"}
    body = t["run_dataflow_reconciliation"].body["launchParameter"]
    assert body["containerSpecGcsPath"].endswith("/templates/daily-reconciliation.json")
    assert "runDate" in body["parameters"]


def test_all_tasks_have_timeouts(dagbag):
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            assert task.execution_timeout is not None, f"{dag.dag_id}.{task.task_id} has no execution_timeout"
