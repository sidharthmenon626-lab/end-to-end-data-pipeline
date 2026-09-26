"""
Unit & Integration Tests for Warehouse Health & Monitoring Suite
Milestone 6: Production Readiness & Observability
"""

from __future__ import annotations

import os
from pathlib import Path

# Ensure Airflow and project env are set
PROJECT_ROOT = Path("E:/end-to-end-data-pipeline").resolve()
os.environ["PYTHONPATH"] = str(PROJECT_ROOT)

from src.monitoring import WarehouseMonitor


def test_warehouse_monitoring_healthy():
    """Verify that live warehouse state passes all 13 operational health checks."""
    monitor = WarehouseMonitor(simulate_failure=False)
    summary = monitor.run_all()

    assert summary["overall_status"] == "PASS"
    assert summary["failed_checks"] == 0
    assert summary["total_checks"] == 13
    assert summary["passed_checks"] == 13

    # Validate pillar coverage
    pillars = {c["pillar"] for c in summary["checks"]}
    assert pillars == {"Freshness", "Volume", "Correctness", "Runtime"}


def test_warehouse_monitoring_detects_anomalies():
    """Verify that monitor detects degradation and transitions to FAIL."""
    monitor = WarehouseMonitor(simulate_failure=True)
    summary = monitor.run_all()

    assert summary["overall_status"] == "FAIL"
    assert summary["failed_checks"] >= 3

    # Verify specific failed pillars
    failed_pillars = {c["pillar"] for c in summary["checks"] if c["status"] == "FAIL"}
    assert "Freshness" in failed_pillars
    assert "Volume" in failed_pillars
    assert "Correctness" in failed_pillars
