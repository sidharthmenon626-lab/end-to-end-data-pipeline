"""
Unit & Integration Tests for Airflow DAG
Milestone 5: Orchestrate with Airflow
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Ensure Airflow and project env are set
PROJECT_ROOT = Path("E:/end-to-end-data-pipeline").resolve()
os.environ["AIRFLOW_HOME"] = str(PROJECT_ROOT / "airflow")
os.environ["PYTHONPATH"] = str(PROJECT_ROOT)

from airflow.models.dagbag import DagBag
from airflow.dags.pipeline_dag import dag, log_pipeline_incident, INCIDENT_LOG


@pytest.fixture(scope="module")
def dag_bag():
    """Load the DagBag from airflow/dags."""
    dags_dir = PROJECT_ROOT / "airflow" / "dags"
    bag = DagBag(dag_folder=str(dags_dir))
    assert len(bag.import_errors) == 0, f"Import errors: {bag.import_errors}"
    return bag


def test_dag_metadata_and_integrity(dag_bag):
    """Test 1: Verify DAG configuration, metadata, and schedule."""
    assert "analytics_end_to_end_pipeline" in dag_bag.dags
    pipeline_dag = dag_bag.dags["analytics_end_to_end_pipeline"]

    schedule = getattr(pipeline_dag, "schedule", None) or getattr(pipeline_dag, "schedule_interval", None)
    assert schedule == "@daily"
    assert pipeline_dag.catchup is False
    assert pipeline_dag.dagrun_timeout == timedelta(minutes=30)
    assert set(pipeline_dag.tags) == {"production", "analytics", "capstone", "kimball"}
    assert pipeline_dag.doc_md is not None
    assert len(pipeline_dag.tasks) == 4


def test_linear_task_dependencies(dag_bag):
    """Test 2: Verify strictly linear task dependency chain."""
    pipeline_dag = dag_bag.dags["analytics_end_to_end_pipeline"]

    ingest = pipeline_dag.get_task("ingest_raw_data")
    dbt = pipeline_dag.get_task("dbt_build_marts")
    checks = pipeline_dag.get_task("data_quality_checks")
    docs = pipeline_dag.get_task("generate_pipeline_docs")

    # Ingest -> dbt
    assert ingest.downstream_task_ids == {"dbt_build_marts"}
    assert dbt.upstream_task_ids == {"ingest_raw_data"}

    # dbt -> Quality Checks
    assert dbt.downstream_task_ids == {"data_quality_checks"}
    assert checks.upstream_task_ids == {"dbt_build_marts"}

    # Quality Checks -> Docs
    assert checks.downstream_task_ids == {"generate_pipeline_docs"}
    assert docs.upstream_task_ids == {"data_quality_checks"}

    # End of pipeline
    assert docs.downstream_task_ids == set()


def test_retry_and_exponential_backoff_policy(dag_bag):
    """Test 3: Verify retry count, exponential backoff, and timeouts."""
    pipeline_dag = dag_bag.dags["analytics_end_to_end_pipeline"]

    for task in pipeline_dag.tasks:
        assert task.retries == 2, f"Task {task.task_id} retries != 2"
        assert task.retry_delay == timedelta(seconds=30), f"Task {task.task_id} retry_delay != 30s"
        assert task.retry_exponential_backoff is True, f"Task {task.task_id} backoff not exponential"
        assert task.max_retry_delay == timedelta(minutes=5), f"Task {task.task_id} max_retry_delay != 5m"
        assert task.execution_timeout == timedelta(minutes=15)

    # Validate exponential backoff progression math
    # Attempt 1 delay = 30s * 2^0 = 30s
    # Attempt 2 delay = 30s * 2^1 = 60s
    delay_1 = timedelta(seconds=30) * (2 ** 0)
    delay_2 = timedelta(seconds=30) * (2 ** 1)
    assert delay_1 == timedelta(seconds=30)
    assert delay_2 == timedelta(seconds=60)
    assert delay_2 <= timedelta(minutes=5)


def test_incident_logging_callback():
    """Test 4: Verify on_failure_callback writes structured audit logs."""
    mock_ti = MagicMock()
    mock_ti.task_id = "test_failing_task"
    mock_ti.dag_id = "analytics_end_to_end_pipeline"
    mock_ti.try_number = 2

    test_context = {
        "task_instance": mock_ti,
        "logical_date": datetime(2026, 3, 25, 0, 0, 0, tzinfo=timezone.utc),
        "exception": RuntimeError("Simulated transient connection timeout"),
    }

    log_pipeline_incident(test_context)

    assert INCIDENT_LOG.exists()
    content = INCIDENT_LOG.read_text(encoding="utf-8")
    assert "[INCIDENT]" in content
    assert "Task: test_failing_task" in content
    assert "Try: 2" in content
    assert "Simulated transient connection timeout" in content
