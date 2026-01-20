"""
Unit Tests for Databricks Client

Tests the DatabricksClient class with mocked API calls.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, PropertyMock
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestDatabricksClientInit:
    """Tests for DatabricksClient initialization."""
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_creates_workspace_client(self, mock_ws):
        from services.databricks_client import DatabricksClient
        
        client = DatabricksClient(warehouse_id="test-warehouse")
        mock_ws.assert_called_once()
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_stores_warehouse_id(self, mock_ws):
        from services.databricks_client import DatabricksClient
        
        client = DatabricksClient(warehouse_id="test-warehouse")
        assert client._warehouse_id == "test-warehouse"
    
    @patch.dict(os.environ, {"DATABRICKS_WAREHOUSE_ID": "env-warehouse"})
    @patch("services.databricks_client.WorkspaceClient")
    def test_uses_env_warehouse_id(self, mock_ws):
        from services.databricks_client import DatabricksClient
        
        client = DatabricksClient()
        assert client._warehouse_id == "env-warehouse"


class TestDatabricksClientCache:
    """Tests for DatabricksClient caching."""
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_cache_stores_value(self, mock_ws):
        from services.databricks_client import DatabricksClient
        
        client = DatabricksClient(warehouse_id="test")
        client._set_cached("test_key", {"data": "value"})
        
        result = client._get_cached("test_key")
        assert result == {"data": "value"}
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_cache_returns_none_for_missing_key(self, mock_ws):
        from services.databricks_client import DatabricksClient
        
        client = DatabricksClient(warehouse_id="test")
        result = client._get_cached("nonexistent")
        assert result is None
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_clear_cache(self, mock_ws):
        from services.databricks_client import DatabricksClient
        
        client = DatabricksClient(warehouse_id="test")
        client._set_cached("key1", "value1")
        client._set_cached("key2", "value2")
        
        client.clear_cache()
        
        assert client._get_cached("key1") is None
        assert client._get_cached("key2") is None


class TestDatabricksClientExecuteSQL:
    """Tests for DatabricksClient.execute_sql method."""
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_raises_without_warehouse_id(self, mock_ws):
        from services.databricks_client import DatabricksClient
        
        client = DatabricksClient()
        client._warehouse_id = None
        
        with pytest.raises(ValueError, match="No warehouse ID"):
            client.execute_sql("SELECT 1")
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_executes_statement(self, mock_ws):
        from services.databricks_client import DatabricksClient
        from databricks.sdk.service.sql import StatementState
        
        # Setup mock response
        mock_response = Mock()
        mock_response.status = Mock()
        mock_response.status.state = StatementState.SUCCEEDED
        mock_response.result = Mock()
        mock_response.result.data_array = [["val1", 100]]
        mock_response.manifest = Mock()
        mock_response.manifest.schema = Mock()
        mock_response.manifest.schema.columns = [Mock(name="col1"), Mock(name="col2")]
        
        mock_ws.return_value.statement_execution.execute_statement.return_value = mock_response
        
        client = DatabricksClient(warehouse_id="test")
        result = client.execute_sql("SELECT * FROM test", use_cache=False)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_returns_cached_result(self, mock_ws):
        from services.databricks_client import DatabricksClient
        from databricks.sdk.service.sql import StatementState
        
        # Setup mock
        mock_response = Mock()
        mock_response.status = Mock()
        mock_response.status.state = StatementState.SUCCEEDED
        mock_response.result = Mock()
        mock_response.result.data_array = [["val1"]]
        mock_response.manifest = Mock()
        mock_response.manifest.schema = Mock()
        mock_response.manifest.schema.columns = [Mock(name="col1")]
        
        mock_ws.return_value.statement_execution.execute_statement.return_value = mock_response
        
        client = DatabricksClient(warehouse_id="test")
        
        # First call
        result1 = client.execute_sql("SELECT 1")
        # Second call (should use cache)
        result2 = client.execute_sql("SELECT 1")
        
        # Should only call API once
        assert mock_ws.return_value.statement_execution.execute_statement.call_count == 1
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_handles_failed_statement(self, mock_ws):
        from services.databricks_client import DatabricksClient
        from databricks.sdk.service.sql import StatementState
        
        mock_response = Mock()
        mock_response.status = Mock()
        mock_response.status.state = StatementState.FAILED
        mock_response.status.error = Mock()
        mock_response.status.error.message = "SQL error"
        
        mock_ws.return_value.statement_execution.execute_statement.return_value = mock_response
        
        client = DatabricksClient(warehouse_id="test")
        
        with pytest.raises(RuntimeError, match="SQL execution failed"):
            client.execute_sql("SELECT * FROM nonexistent", use_cache=False)
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_handles_empty_result(self, mock_ws):
        from services.databricks_client import DatabricksClient
        from databricks.sdk.service.sql import StatementState
        
        mock_response = Mock()
        mock_response.status = Mock()
        mock_response.status.state = StatementState.SUCCEEDED
        mock_response.result = None
        mock_response.manifest = None
        
        mock_ws.return_value.statement_execution.execute_statement.return_value = mock_response
        
        client = DatabricksClient(warehouse_id="test")
        result = client.execute_sql("SELECT * FROM empty_table", use_cache=False)
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestDatabricksClientListGenieSpaces:
    """Tests for DatabricksClient.list_genie_spaces method."""
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_returns_empty_list_on_api_failure(self, mock_ws):
        from services.databricks_client import DatabricksClient
        
        # Make both Genie API and SQL fallback fail
        mock_ws.return_value.genie.list_spaces.side_effect = Exception("API Error")
        mock_ws.return_value.statement_execution.execute_statement.side_effect = Exception("SQL Error")
        
        client = DatabricksClient(warehouse_id="test")
        result = client.list_genie_spaces()
        
        assert result == []
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_returns_spaces_from_api(self, mock_ws):
        from services.databricks_client import DatabricksClient, GenieSpace
        
        # Setup mock spaces
        mock_space = Mock()
        mock_space.space_id = "space-123"
        mock_space.title = "Test Space"
        mock_space.description = "A test space"
        mock_space.create_time = "2024-01-01"
        mock_space.warehouse_id = "warehouse-abc"
        
        # Create mock response with spaces list and no next page
        mock_response = Mock()
        mock_response.spaces = [mock_space]
        mock_response.next_page_token = None
        mock_ws.return_value.genie.list_spaces.return_value = mock_response
        
        client = DatabricksClient(warehouse_id="test")
        result = client.list_genie_spaces()
        
        assert len(result) == 1
        assert result[0].id == "space-123"
        assert result[0].name == "Test Space"
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_caches_spaces(self, mock_ws):
        from services.databricks_client import DatabricksClient
        
        mock_space = Mock()
        mock_space.space_id = "space-123"
        mock_space.title = "Test Space"
        mock_space.description = ""
        mock_space.create_time = None
        mock_space.warehouse_id = None
        
        # Create mock response with spaces list and no next page
        mock_response = Mock()
        mock_response.spaces = [mock_space]
        mock_response.next_page_token = None
        mock_ws.return_value.genie.list_spaces.return_value = mock_response
        
        client = DatabricksClient(warehouse_id="test")
        
        # First call
        result1 = client.list_genie_spaces()
        # Second call
        result2 = client.list_genie_spaces()
        
        # Should only call API once
        assert mock_ws.return_value.genie.list_spaces.call_count == 1
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_handles_pagination(self, mock_ws):
        from services.databricks_client import DatabricksClient
        
        # Setup mock spaces for two pages
        mock_space1 = Mock()
        mock_space1.space_id = "space-1"
        mock_space1.title = "Space One"
        mock_space1.description = ""
        mock_space1.create_time = None
        mock_space1.warehouse_id = None
        
        mock_space2 = Mock()
        mock_space2.space_id = "space-2"
        mock_space2.title = "Space Two"
        mock_space2.description = ""
        mock_space2.create_time = None
        mock_space2.warehouse_id = None
        
        # Create mock responses for two pages
        mock_response1 = Mock()
        mock_response1.spaces = [mock_space1]
        mock_response1.next_page_token = "page2token"
        
        mock_response2 = Mock()
        mock_response2.spaces = [mock_space2]
        mock_response2.next_page_token = None
        
        # Return different responses for each call
        mock_ws.return_value.genie.list_spaces.side_effect = [mock_response1, mock_response2]
        
        client = DatabricksClient(warehouse_id="test")
        result = client.list_genie_spaces()
        
        # Should have spaces from both pages
        assert len(result) == 2
        assert result[0].id == "space-1"
        assert result[1].id == "space-2"
        
        # Should have called API twice (once per page)
        assert mock_ws.return_value.genie.list_spaces.call_count == 2
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_progress_callback_is_called(self, mock_ws):
        from services.databricks_client import DatabricksClient
        
        # Setup mock spaces for two pages
        mock_space1 = Mock()
        mock_space1.space_id = "space-1"
        mock_space1.title = "Space One"
        mock_space1.description = ""
        mock_space1.create_time = None
        mock_space1.warehouse_id = None
        
        mock_space2 = Mock()
        mock_space2.space_id = "space-2"
        mock_space2.title = "Space Two"
        mock_space2.description = ""
        mock_space2.create_time = None
        mock_space2.warehouse_id = None
        
        # Create mock responses for two pages
        mock_response1 = Mock()
        mock_response1.spaces = [mock_space1]
        mock_response1.next_page_token = "page2token"
        
        mock_response2 = Mock()
        mock_response2.spaces = [mock_space2]
        mock_response2.next_page_token = None
        
        mock_ws.return_value.genie.list_spaces.side_effect = [mock_response1, mock_response2]
        
        # Track callback calls
        callback_calls = []
        def progress_callback(count, has_more, total=None):
            callback_calls.append((count, has_more, total))
        
        client = DatabricksClient(warehouse_id="test")
        result = client.list_genie_spaces(progress_callback=progress_callback)
        
        # Should have called callback for each page
        assert len(callback_calls) == 2
        assert callback_calls[0] == (1, True, None)   # First page: 1 space, has more
        assert callback_calls[1] == (2, False, None)  # Second page: 2 spaces total, no more


class TestDatabricksClientGetGenieSpace:
    """Tests for DatabricksClient.get_genie_space method."""
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_returns_none_on_failure(self, mock_ws):
        from services.databricks_client import DatabricksClient
        
        mock_ws.return_value.genie.get_space.side_effect = Exception("Not found")
        
        client = DatabricksClient(warehouse_id="test")
        result = client.get_genie_space("nonexistent")
        
        assert result is None
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_returns_space(self, mock_ws):
        from services.databricks_client import DatabricksClient
        
        mock_space = Mock()
        mock_space.space_id = "space-123"
        mock_space.title = "Test Space"
        mock_space.description = "Description"
        mock_space.create_time = "2024-01-01"
        mock_space.warehouse_id = "warehouse-abc"
        
        mock_ws.return_value.genie.get_space.return_value = mock_space
        
        client = DatabricksClient(warehouse_id="test")
        result = client.get_genie_space("space-123")
        
        assert result is not None
        assert result.id == "space-123"
        assert result.name == "Test Space"
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_caches_space(self, mock_ws):
        from services.databricks_client import DatabricksClient
        
        mock_space = Mock()
        mock_space.space_id = "space-123"
        mock_space.title = "Test"
        mock_space.description = ""
        mock_space.create_time = None
        mock_space.warehouse_id = None
        
        mock_ws.return_value.genie.get_space.return_value = mock_space
        
        client = DatabricksClient(warehouse_id="test")
        
        result1 = client.get_genie_space("space-123")
        result2 = client.get_genie_space("space-123")
        
        assert mock_ws.return_value.genie.get_space.call_count == 1


class TestDatabricksClientGetSpacesWithMetrics:
    """Tests for DatabricksClient.get_spaces_with_metrics method."""
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_returns_empty_df_when_no_spaces(self, mock_ws):
        from services.databricks_client import DatabricksClient
        
        # Create mock response with empty spaces list
        mock_response = Mock()
        mock_response.spaces = []
        mock_response.next_page_token = None
        mock_ws.return_value.genie.list_spaces.return_value = mock_response
        
        client = DatabricksClient(warehouse_id="test")
        result = client.get_spaces_with_metrics(days=30)
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_returns_spaces_with_default_metrics(self, mock_ws):
        from services.databricks_client import DatabricksClient
        from databricks.sdk.service.sql import StatementState
        
        # Setup mock space
        mock_space = Mock()
        mock_space.space_id = "space-123"
        mock_space.title = "Test Space"
        mock_space.description = "Description"
        mock_space.create_time = "2024-01-01"
        mock_space.warehouse_id = "warehouse-abc"
        
        # Create mock response with pagination format
        mock_genie_response = Mock()
        mock_genie_response.spaces = [mock_space]
        mock_genie_response.next_page_token = None
        mock_ws.return_value.genie.list_spaces.return_value = mock_genie_response
        
        # Setup mock SQL response (empty metrics)
        mock_response = Mock()
        mock_response.status = Mock()
        mock_response.status.state = StatementState.SUCCEEDED
        mock_response.result = Mock()
        mock_response.result.data_array = []
        mock_response.manifest = Mock()
        mock_response.manifest.schema = Mock()
        mock_response.manifest.schema.columns = []
        
        mock_ws.return_value.statement_execution.execute_statement.return_value = mock_response
        
        client = DatabricksClient(warehouse_id="test")
        result = client.get_spaces_with_metrics(days=30)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["id"] == "space-123"
        assert result.iloc[0]["query_count"] == 0  # Default value


class TestGetClient:
    """Tests for get_client singleton function."""
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_returns_client(self, mock_ws):
        from services.databricks_client import get_client, DatabricksClient
        
        # Reset singleton
        import services.databricks_client as module
        module._client = None
        
        client = get_client(warehouse_id="test")
        assert isinstance(client, DatabricksClient)
    
    @patch("services.databricks_client.WorkspaceClient")
    def test_returns_same_instance(self, mock_ws):
        from services.databricks_client import get_client
        
        # Reset singleton
        import services.databricks_client as module
        module._client = None
        
        client1 = get_client(warehouse_id="test")
        client2 = get_client()
        
        assert client1 is client2
