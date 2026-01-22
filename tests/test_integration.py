"""
Integration Tests for Genie Audit

These tests require actual Databricks credentials and will run against
a real Databricks workspace. They are skipped if credentials are not available.

Required environment variables:
- DATABRICKS_HOST: Databricks workspace URL
- DATABRICKS_TOKEN: Personal access token or OAuth token
- DATABRICKS_WAREHOUSE_ID: SQL warehouse ID
- TEST_GENIE_SPACE_ID: (Optional) A Genie space ID for testing

Run with:
    pytest tests/test_integration.py -v

Skip these tests:
    pytest tests/ --ignore=tests/test_integration.py
"""

import pytest
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


class TestDatabricksClientIntegration:
    """Integration tests for DatabricksClient."""
    
    def test_execute_simple_query(self, databricks_client):
        """Test executing a simple SQL query."""
        result = databricks_client.execute_sql("SELECT 1 AS test_value", use_cache=False)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "test_value" in result.columns
    
    def test_execute_system_query(self, databricks_client):
        """Test querying system tables."""
        sql = """
        SELECT COUNT(*) AS query_count
        FROM system.query.history
        WHERE start_time >= date_sub(current_timestamp(), 1)
        LIMIT 1
        """
        result = databricks_client.execute_sql(sql, use_cache=False)
        
        assert isinstance(result, pd.DataFrame)
        assert "query_count" in result.columns
    
    def test_list_genie_spaces(self, databricks_client):
        """Test listing Genie spaces."""
        spaces = databricks_client.list_genie_spaces()
        
        # May return empty list if no Genie spaces exist
        assert isinstance(spaces, list)
        
        if spaces:
            space = spaces[0]
            assert space.id is not None
            assert space.name is not None
    
    def test_get_spaces_with_metrics(self, databricks_client):
        """Test getting spaces with metrics."""
        result = databricks_client.get_spaces_with_metrics(days=7)
        
        assert isinstance(result, pd.DataFrame)
        
        if not result.empty:
            assert "id" in result.columns
            assert "name" in result.columns
            assert "query_count" in result.columns


class TestSQLQueriesIntegration:
    """Integration tests for SQL queries."""
    
    def test_summary_stats_query(self, databricks_client):
        """Test the summary stats query."""
        from queries.sql import SUMMARY_STATS_QUERY
        
        sql = SUMMARY_STATS_QUERY.format(days=7)
        result = databricks_client.execute_sql(sql, use_cache=False)
        
        assert isinstance(result, pd.DataFrame)
        
        if not result.empty:
            row = result.iloc[0]
            assert "total_queries" in result.columns
            assert "genie_spaces" in result.columns
            assert "success_rate_pct" in result.columns
    
    def test_bottleneck_distribution_query(self, databricks_client):
        """Test the bottleneck distribution query."""
        from queries.sql import BOTTLENECK_DISTRIBUTION_QUERY
        
        sql = BOTTLENECK_DISTRIBUTION_QUERY.format(days=7, space_filter="")
        result = databricks_client.execute_sql(sql, use_cache=False)
        
        assert isinstance(result, pd.DataFrame)
        
        if not result.empty:
            assert "bottleneck_type" in result.columns
            assert "query_count" in result.columns
    
    def test_duration_histogram_query(self, databricks_client):
        """Test the duration histogram query."""
        from queries.sql import DURATION_HISTOGRAM_QUERY
        
        sql = DURATION_HISTOGRAM_QUERY.format(days=7, space_filter="")
        result = databricks_client.execute_sql(sql, use_cache=False)
        
        assert isinstance(result, pd.DataFrame)
        
        if not result.empty:
            assert "duration_bucket" in result.columns
            assert "query_count" in result.columns
    
    def test_daily_trend_query(self, databricks_client):
        """Test the daily trend query."""
        from queries.sql import DAILY_TREND_QUERY
        
        sql = DAILY_TREND_QUERY.format(days=7, space_filter="")
        result = databricks_client.execute_sql(sql, use_cache=False)
        
        assert isinstance(result, pd.DataFrame)
        
        if not result.empty:
            assert "query_date" in result.columns
            assert "total_queries" in result.columns


class TestSpaceSpecificIntegration:
    """Integration tests for space-specific queries."""
    
    def test_space_metrics_query(self, databricks_client, integration_space_id):
        """Test the space metrics query for a specific space."""
        from queries.sql import SPACE_METRICS_QUERY
        
        sql = SPACE_METRICS_QUERY.format(space_id=integration_space_id, days=30)
        result = databricks_client.execute_sql(sql, use_cache=False)
        
        assert isinstance(result, pd.DataFrame)
        
        if not result.empty:
            assert "total_queries" in result.columns
            assert "avg_duration_sec" in result.columns
    
    def test_queries_list_query(self, databricks_client, integration_space_id):
        """Test the queries list query for a specific space."""
        from queries.sql import QUERIES_LIST_QUERY, build_space_filter
        
        space_filter = build_space_filter(integration_space_id)
        sql = QUERIES_LIST_QUERY.format(
            days=30,
            space_filter=space_filter,
            status_filter="",
            limit=10
        )
        result = databricks_client.execute_sql(sql, use_cache=False)
        
        assert isinstance(result, pd.DataFrame)
        
        if not result.empty:
            assert "statement_id" in result.columns
            assert "query_text" in result.columns
            assert "bottleneck" in result.columns
            assert "speed_category" in result.columns
    
    def test_get_genie_space(self, databricks_client, integration_space_id):
        """Test getting a specific Genie space."""
        space = databricks_client.get_genie_space(integration_space_id)
        
        # May return None if space doesn't exist or API fails
        if space:
            assert space.id == integration_space_id
            assert space.name is not None


class TestCacheIntegration:
    """Integration tests for caching behavior."""
    
    def test_cache_works(self, databricks_client):
        """Test that caching reduces API calls."""
        import time
        
        sql = "SELECT 1 AS value"
        
        # First call (not cached)
        start1 = time.time()
        result1 = databricks_client.execute_sql(sql)
        time1 = time.time() - start1
        
        # Second call (should be cached)
        start2 = time.time()
        result2 = databricks_client.execute_sql(sql)
        time2 = time.time() - start2
        
        # Cached call should be faster
        assert time2 < time1
        
        # Results should be identical
        assert result1.equals(result2)
    
    def test_cache_clear(self, databricks_client):
        """Test that cache clearing works."""
        sql = "SELECT 1 AS value"
        
        # Execute and cache
        databricks_client.execute_sql(sql)
        
        # Clear cache
        databricks_client.clear_cache()
        
        # Should execute again (not use cache)
        result = databricks_client.execute_sql(sql, use_cache=False)
        assert isinstance(result, pd.DataFrame)


class TestErrorHandlingIntegration:
    """Integration tests for error handling."""
    
    def test_invalid_sql_raises_error(self, databricks_client):
        """Test that invalid SQL raises an appropriate error."""
        with pytest.raises(RuntimeError, match="SQL execution failed"):
            databricks_client.execute_sql(
                "SELECT * FROM nonexistent_table_xyz_123",
                use_cache=False
            )
    
    def test_nonexistent_space_returns_none(self, databricks_client):
        """Test that getting a nonexistent space returns None."""
        result = databricks_client.get_genie_space("nonexistent-space-id-xyz-123")
        assert result is None


class TestConversationMetricsIntegration:
    """Integration tests for conversation-centric metrics."""
    
    def test_message_ai_overhead_query(self, databricks_client, integration_space_id):
        """Test the message AI overhead query syntax is valid."""
        from queries.sql import get_message_ai_overhead_query
        
        sql = get_message_ai_overhead_query(integration_space_id, hours=24)
        result = databricks_client.execute_sql(sql, use_cache=False)
        
        assert isinstance(result, pd.DataFrame)
        
        # Query should return these columns even if empty
        if not result.empty:
            assert "conversation_id" in result.columns
            assert "message_id" in result.columns
            assert "ai_overhead_sec" in result.columns
            assert "message_source" in result.columns
    
    def test_conversation_sources_query(self, databricks_client, integration_space_id):
        """Test the conversation sources query syntax is valid."""
        from queries.sql import get_conversation_sources_query
        
        sql = get_conversation_sources_query(integration_space_id, hours=24)
        result = databricks_client.execute_sql(sql, use_cache=False)
        
        assert isinstance(result, pd.DataFrame)
        
        if not result.empty:
            assert "conversation_id" in result.columns
            assert "message_source" in result.columns
    
    def test_queries_by_statement_ids(self, databricks_client):
        """Test batch query for statement IDs."""
        from queries.sql import QUERIES_BY_STATEMENT_IDS, build_statement_ids_filter
        
        # Use a fake statement ID - query should succeed but return empty
        statement_ids_str = build_statement_ids_filter(["nonexistent-stmt-id"])
        sql = QUERIES_BY_STATEMENT_IDS.format(statement_ids=statement_ids_str)
        result = databricks_client.execute_sql(sql, use_cache=False)
        
        assert isinstance(result, pd.DataFrame)
    
    def test_get_conversations_with_query_metrics(self, databricks_client, integration_space_id):
        """Test the full conversation data loading."""
        from services.databricks_client import ConversationWithMessages
        
        result = databricks_client.get_conversations_with_query_metrics(
            space_id=integration_space_id,
            max_conversations=5
        )
        
        assert isinstance(result, list)
        
        # If any conversations returned, verify structure
        for conv in result:
            assert isinstance(conv, ConversationWithMessages)
            assert conv.conversation_id is not None
            assert isinstance(conv.messages, list)
            assert isinstance(conv.total_queries, int)
            assert isinstance(conv.has_performance_issues, bool)
            assert isinstance(conv.total_ai_overhead_sec, float)


# Custom pytest marker for integration tests
def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
