"""
End-to-End Analytics Warehouse Orchestration Pipeline DAG
Milestone 5: Orchestrate with Airflow

This DAG orchestrates the production pipeline:
1. Ingestion: Extracts incremental parquet source files with CDC sequence validation and watermark tracking.
2. Transformations: Executes Kimball dimensional and fact modeling via dbt build (staging + marts + tests).
3. Quality Assurance: Runs the comprehensive data verification and integrity suite.
4. Documentation: Compiles dbt catalog and schema documentation.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Base directory paths
PROJECT_ROOT = Path("E:/end-to-end-data-pipeline").resolve()
DBT_DIR = (PROJECT_ROOT / "dbt").resolve()
VENV_SCRIPTS = (PROJECT_ROOT / ".venv" / "Scripts").resolve()
PYTHON_EXE = str(VENV_SCRIPTS / "python.exe") if (VENV_SCRIPTS / "python.exe").exists() else sys.executable
DBT_EXE = str(VENV_SCRIPTS / "dbt.exe") if (VENV_SCRIPTS / "dbt.exe").exists() else "dbt"
LOGS_DIR = (PROJECT_ROOT / "airflow" / "logs").resolve()
INCIDENT_LOG = LOGS_DIR / "incidents.log"


def log_pipeline_incident(context: dict) -> None:
    """
    Incident callback triggered whenever a DAG task fails.
    Extracts execution context and appends an auditable alert to the incidents log.
    """
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown_task"
    dag_id = ti.dag_id if ti else "analytics_end_to_end_pipeline"
    try_number = ti.try_number if ti else 1
    logical_date = context.get("logical_date") or context.get("execution_date") or datetime.now(timezone.utc)
    exception = context.get("exception") or "Unhandled Task Exception"

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat()
    log_entry = (
        f"[{timestamp}] [INCIDENT] DAG: {dag_id} | Task: {task_id} | "
        f"Try: {try_number} | LogicalDate: {logical_date} | "
        f"Error: {exception}\n"
    )

    with open(INCIDENT_LOG, "a", encoding="utf-8") as f:
        f.write(log_entry)

    print(f"[ON_FAILURE_CALLBACK] Recorded incident:\n{log_entry.strip()}")


def _execute_subprocess(command: list[str], cwd: Path | str, task_name: str) -> None:
    """
    Helper to execute CLI commands in a child process, capturing and streaming output.
    Raises RuntimeError on non-zero exit codes.
    """
    print(f"[{task_name}] Executing command: {' '.join(command)}")
    print(f"[{task_name}] Working directory: {cwd}")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT)
    env["AIRFLOW_HOME"] = str(PROJECT_ROOT / "airflow")

    result = subprocess.run(
        command,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        print(f"[{task_name} STDOUT]\n{result.stdout}")
    if result.stderr:
        print(f"[{task_name} STDERR]\n{result.stderr}")

    if result.returncode != 0:
        raise RuntimeError(
            f"Task {task_name} failed with exit code {result.returncode}. "
            f"Stderr: {result.stderr.strip() or 'None'}"
        )

    print(f"[{task_name}] Completed successfully with exit code 0.")


def task_ingest_raw_data(**context) -> None:
    """Task 1: Execute incremental ingestion and CDC mutation processing."""
    cmd = [PYTHON_EXE, "-m", "src.ingest.pipeline"]
    _execute_subprocess(cmd, cwd=PROJECT_ROOT, task_name="ingest_raw_data")


def task_dbt_build_marts(**context) -> None:
    """Task 2: Execute dbt build (models, tests, seeds, snapshots)."""
    cmd = [DBT_EXE, "build", "--profiles-dir", "."]
    _execute_subprocess(cmd, cwd=DBT_DIR, task_name="dbt_build_marts")


def task_data_quality_checks(**context) -> None:
    """Task 3: Execute comprehensive system verification suite."""
    cmd = [PYTHON_EXE, "-m", "src.utils.verify_all_steps"]
    _execute_subprocess(cmd, cwd=PROJECT_ROOT, task_name="data_quality_checks")


def task_generate_pipeline_docs(**context) -> None:
    """Task 4: Generate dbt catalog and schema documentation."""
    cmd = [DBT_EXE, "docs", "generate", "--profiles-dir", "."]
    _execute_subprocess(cmd, cwd=DBT_DIR, task_name="generate_pipeline_docs")


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": log_pipeline_incident,
}

with DAG(
    dag_id="analytics_end_to_end_pipeline",
    default_args=default_args,
    description="Orchestrates ingestion, dbt Kimball marts, quality checks, and doc generation.",
    schedule="@daily",
    start_date=datetime(2026, 3, 20),
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    tags=["production", "analytics", "capstone", "kimball"],
    doc_md=__doc__,
) as dag:

    ingest_raw_data = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=task_ingest_raw_data,
        sla=timedelta(minutes=10),
    )

    dbt_build_marts = PythonOperator(
        task_id="dbt_build_marts",
        python_callable=task_dbt_build_marts,
        sla=timedelta(minutes=15),
    )

    data_quality_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=task_data_quality_checks,
        sla=timedelta(minutes=5),
    )

    generate_pipeline_docs = PythonOperator(
        task_id="generate_pipeline_docs",
        python_callable=task_generate_pipeline_docs,
        sla=timedelta(minutes=5),
    )

    # Linear dependencies: Ingest -> Transform -> Validate -> Document
    ingest_raw_data >> dbt_build_marts >> data_quality_checks >> generate_pipeline_docs
