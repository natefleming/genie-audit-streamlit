"""
Pytest Configuration and Fixtures

Provides shared fixtures for unit and integration tests.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock, patch
from typing import Generator
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ============================================================================
# Mock Fixtures
# ============================================================================

@pytest.fixture
def mock_workspace_client() -> Mock:
    """Create a mock WorkspaceClient."""
    client = Mock()
    
    # Mock statement execution
    client.statement_execution = Mock()
    client.statement_execution.execute_statement = Mock()
    
    # Mock genie API with pagination response format
    client.genie = Mock()
    empty_response = Mock()
    empty_response.spaces = []
    empty_response.next_page_token = None
    client.genie.list_spaces = Mock(return_value=empty_response)
    client.genie.get_space = Mock(return_value=None)
    
    return client


@pytest.fixture
def mock_statement_response() -> Mock:
    """Create a mock statement execution response."""
    from databricks.sdk.service.sql import StatementState
    
    response = Mock()
    response.status = Mock()
    response.status.state = StatementState.SUCCEEDED
    response.status.error = None
    response.result = Mock()
    response.result.data_array = [
        ["value1", "value2", 100],
        ["value3", "value4", 200],
    ]
    response.manifest = Mock()
    response.manifest.schema = Mock()
    response.manifest.schema.columns = [
        Mock(name="col1"),
        Mock(name="col2"),
        Mock(name="count"),
    ]
    
    return response


@pytest.fixture
def sample_genie_space() -> Mock:
    """Create a mock Genie space."""
    space = Mock()
    space.space_id = "test-space-123"
    space.title = "Test Genie Room"
    space.description = "A test Genie room for testing"
    space.create_time = "2024-01-01T00:00:00Z"
    space.warehouse_id = "warehouse-abc"
    
    return space


# ============================================================================
# Sample Data Fixtures
# ============================================================================

@pytest.fixture
def sample_metrics() -> dict:
    """Sample metrics dictionary."""
    return {
        "total_queries": 1500,
        "genie_spaces": 5,
        "unique_users": 25,
        "avg_duration_sec": 3.5,
        "p90_sec": 8.2,
        "slow_10s": 45,
        "slow_30s": 12,
        "success_rate_pct": 97.5,
        "successful_queries": 1463,
        "failed_queries": 37,
    }


@pytest.fixture
def sample_query() -> dict:
    """Sample query dictionary for testing."""
    return {
        "statement_id": "stmt-123",
        "genie_space_id": "space-456",
        "executed_by": "user@example.com",
        "start_time": "2024-01-15T10:30:00Z",
        "total_duration_ms": 15000,
        "total_sec": 15.0,
        "compilation_ms": 500,
        "compile_sec": 0.5,
        "execution_ms": 12000,
        "execute_sec": 12.0,
        "compute_wait_ms": 1500,
        "wait_compute_sec": 1.5,
        "queue_wait_ms": 1000,
        "queue_sec": 1.0,
        "result_fetch_ms": 0,
        "fetch_sec": 0.0,
        "bytes_scanned": 104857600,  # 100 MB
        "read_mb": 100.0,
        "read_rows": 1000000,
        "produced_rows": 500,
        "execution_status": "FINISHED",
        "speed_category": "SLOW",
        "bottleneck": "SLOW_EXECUTION",
        "query_text": "SELECT * FROM large_table WHERE category = 'test'",
    }


@pytest.fixture
def sample_duration_df() -> pd.DataFrame:
    """Sample duration distribution DataFrame."""
    return pd.DataFrame({
        "duration_bucket": ["< 1s", "1-5s", "5-10s", "10-30s", "30-60s", "> 60s"],
        "bucket_order": [1, 2, 3, 4, 5, 6],
        "query_count": [500, 350, 150, 80, 30, 10],
    })


@pytest.fixture
def sample_bottleneck_df() -> pd.DataFrame:
    """Sample bottleneck distribution DataFrame."""
    return pd.DataFrame({
        "bottleneck_type": ["Normal", "Slow Execution", "Queue Wait", "Large Scan", "Compilation"],
        "query_count": [800, 120, 50, 30, 20],
        "total_time_min": [50.5, 120.3, 35.2, 45.8, 15.1],
        "avg_duration_sec": [2.5, 45.2, 25.0, 60.5, 30.0],
    })


@pytest.fixture
def sample_daily_trend_df() -> pd.DataFrame:
    """Sample daily trend DataFrame."""
    import datetime
    
    dates = pd.date_range(start="2024-01-01", periods=7, freq="D")
    return pd.DataFrame({
        "query_date": dates,
        "total_queries": [100, 120, 95, 110, 130, 80, 90],
        "slow_queries": [5, 8, 3, 6, 10, 4, 5],
        "avg_sec": [2.5, 3.0, 2.2, 2.8, 3.5, 2.0, 2.3],
        "p90_sec": [8.0, 9.5, 7.0, 8.5, 11.0, 6.5, 7.5],
        "success_rate": [98.0, 97.5, 99.0, 98.5, 96.5, 99.5, 98.0],
    })


@pytest.fixture
def sample_hourly_df() -> pd.DataFrame:
    """Sample hourly volume DataFrame."""
    return pd.DataFrame({
        "hour_of_day": list(range(24)),
        "query_count": [5, 2, 1, 1, 2, 5, 15, 45, 80, 90, 85, 75, 
                        70, 75, 80, 85, 70, 50, 30, 20, 15, 12, 8, 5],
        "avg_sec": [2.0] * 24,
        "p90_sec": [5.0] * 24,
        "slow_count": [0, 0, 0, 0, 0, 0, 1, 3, 5, 6, 5, 4,
                       4, 5, 5, 6, 4, 3, 2, 1, 1, 0, 0, 0],
    })


@pytest.fixture
def sample_queries_df() -> pd.DataFrame:
    """Sample queries list DataFrame."""
    return pd.DataFrame([
        {
            "statement_id": "stmt-001",
            "genie_space_id": "space-123",
            "executed_by": "alice@example.com",
            "start_time": "2024-01-15T10:30:00Z",
            "total_sec": 45.2,
            "compile_sec": 0.5,
            "execute_sec": 43.0,
            "wait_compute_sec": 1.0,
            "queue_sec": 0.7,
            "fetch_sec": 0.0,
            "read_mb": 500.0,
            "read_rows": 5000000,
            "produced_rows": 100,
            "execution_status": "FINISHED",
            "total_duration_ms": 45200,
            "compilation_ms": 500,
            "execution_ms": 43000,
            "compute_wait_ms": 1000,
            "queue_wait_ms": 700,
            "result_fetch_ms": 0,
            "bytes_scanned": 524288000,
            "speed_category": "CRITICAL",
            "bottleneck": "SLOW_EXECUTION",
            "query_text": "SELECT * FROM orders WHERE date > '2024-01-01'",
        },
        {
            "statement_id": "stmt-002",
            "genie_space_id": "space-123",
            "executed_by": "bob@example.com",
            "start_time": "2024-01-15T11:00:00Z",
            "total_sec": 2.5,
            "compile_sec": 0.2,
            "execute_sec": 2.0,
            "wait_compute_sec": 0.2,
            "queue_sec": 0.1,
            "fetch_sec": 0.0,
            "read_mb": 10.0,
            "read_rows": 10000,
            "produced_rows": 50,
            "execution_status": "FINISHED",
            "total_duration_ms": 2500,
            "compilation_ms": 200,
            "execution_ms": 2000,
            "compute_wait_ms": 200,
            "queue_wait_ms": 100,
            "result_fetch_ms": 0,
            "bytes_scanned": 10485760,
            "speed_category": "FAST",
            "bottleneck": "NORMAL",
            "query_text": "SELECT COUNT(*) FROM users",
        },
    ])


# ============================================================================
# Integration Test Fixtures
# ============================================================================

@pytest.fixture
def databricks_client():
    """
    Create a real DatabricksClient for integration tests.
    
    Requires DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_WAREHOUSE_ID
    environment variables to be set.
    
    Skip tests if credentials are not available.
    """
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    
    if not warehouse_id:
        pytest.skip("DATABRICKS_WAREHOUSE_ID not set - skipping integration test")
    
    try:
        from services.databricks_client import DatabricksClient
        client = DatabricksClient(warehouse_id=warehouse_id)
        return client
    except Exception as e:
        pytest.skip(f"Failed to create DatabricksClient: {e}")


@pytest.fixture
def integration_space_id():
    """Get a Genie space ID for integration tests."""
    space_id = os.environ.get("TEST_GENIE_SPACE_ID")
    if not space_id:
        pytest.skip("TEST_GENIE_SPACE_ID not set - skipping integration test")
    return space_id
