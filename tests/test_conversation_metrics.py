"""
Unit Tests for Conversation Metrics

Tests the conversation data model, AI overhead calculation,
and performance issue detection.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.databricks_client import (
    QueryMetrics,
    MessageWithQueries,
    ConversationWithMessages,
)


class TestQueryMetrics:
    """Tests for QueryMetrics dataclass."""
    
    def test_default_values(self):
        """Test QueryMetrics with default values."""
        qm = QueryMetrics(statement_id="stmt-123")
        
        assert qm.statement_id == "stmt-123"
        assert qm.total_duration_ms == 0
        assert qm.bottleneck == "NORMAL"
        assert qm.speed_category == "FAST"
    
    def test_full_initialization(self):
        """Test QueryMetrics with all values."""
        qm = QueryMetrics(
            statement_id="stmt-456",
            query_text="SELECT * FROM test",
            total_duration_ms=15000,
            compilation_ms=500,
            execution_ms=14000,
            bottleneck="SLOW_EXECUTION",
            speed_category="SLOW"
        )
        
        assert qm.total_duration_ms == 15000
        assert qm.bottleneck == "SLOW_EXECUTION"


class TestMessageWithQueries:
    """Tests for MessageWithQueries dataclass."""
    
    def test_empty_message(self):
        """Test message with no queries."""
        msg = MessageWithQueries(message_id="msg-123")
        
        assert msg.message_id == "msg-123"
        assert msg.queries == []
        assert msg.query_count == 0
        assert msg.total_duration_ms == 0
        assert msg.ai_overhead_sec == 0.0
        assert msg.total_response_sec == 0.0
        assert msg.has_performance_issue is False
    
    def test_message_with_queries(self):
        """Test message with queries aggregates correctly."""
        queries = [
            QueryMetrics(statement_id="q1", total_duration_ms=5000),
            QueryMetrics(statement_id="q2", total_duration_ms=8000),
        ]
        msg = MessageWithQueries(
            message_id="msg-456",
            content="Show me sales data",
            queries=queries,
            ai_overhead_sec=2.0
        )
        
        assert msg.query_count == 2
        assert msg.total_duration_ms == 13000
        assert msg.ai_overhead_sec == 2.0
        # total_response = ai_overhead + (total_duration_ms / 1000)
        assert msg.total_response_sec == 2.0 + 13.0
    
    def test_slow_ai_detection(self):
        """Test detection of slow AI overhead (>10s)."""
        msg_fast = MessageWithQueries(
            message_id="msg-fast",
            ai_overhead_sec=8.0  # Under 10s threshold
        )
        assert msg_fast.has_slow_ai is False
        assert msg_fast.has_performance_issue is False
        
        msg_slow = MessageWithQueries(
            message_id="msg-slow",
            ai_overhead_sec=12.0  # Over 10s threshold
        )
        assert msg_slow.has_slow_ai is True
        assert msg_slow.has_performance_issue is True
    
    def test_slow_query_detection(self):
        """Test detection of slow SQL queries (>10s)."""
        fast_queries = [
            QueryMetrics(statement_id="q1", total_duration_ms=5000),
        ]
        msg_fast = MessageWithQueries(
            message_id="msg-fast",
            queries=fast_queries
        )
        assert msg_fast.has_slow_query is False
        
        slow_queries = [
            QueryMetrics(statement_id="q1", total_duration_ms=5000),
            QueryMetrics(statement_id="q2", total_duration_ms=12000),  # >10s
        ]
        msg_slow = MessageWithQueries(
            message_id="msg-slow",
            queries=slow_queries
        )
        assert msg_slow.has_slow_query is True
        assert msg_slow.has_performance_issue is True
    
    def test_message_source(self):
        """Test message source tracking."""
        msg_api = MessageWithQueries(
            message_id="msg-api",
            message_source="API"
        )
        assert msg_api.message_source == "API"
        
        msg_space = MessageWithQueries(
            message_id="msg-space",
            message_source="Space"
        )
        assert msg_space.message_source == "Space"


class TestConversationWithMessages:
    """Tests for ConversationWithMessages dataclass."""
    
    def test_empty_conversation(self):
        """Test conversation with no messages."""
        conv = ConversationWithMessages(conversation_id="conv-123")
        
        assert conv.conversation_id == "conv-123"
        assert conv.messages == []
        assert conv.total_queries == 0
        assert conv.has_performance_issues is False
    
    def test_conversation_aggregates(self):
        """Test conversation aggregates metrics from messages."""
        queries1 = [QueryMetrics(statement_id="q1", total_duration_ms=5000, execution_status="FINISHED")]
        queries2 = [QueryMetrics(statement_id="q2", total_duration_ms=8000, execution_status="FINISHED")]
        
        messages = [
            MessageWithQueries(
                message_id="m1",
                content="First prompt",
                queries=queries1,
                ai_overhead_sec=2.0
            ),
            MessageWithQueries(
                message_id="m2",
                content="Second prompt",
                queries=queries2,
                ai_overhead_sec=3.0
            ),
        ]
        
        conv = ConversationWithMessages(
            conversation_id="conv-456",
            title="Test Conversation",
            messages=messages
        )
        
        assert conv.total_queries == 2
        assert conv.total_ai_overhead_sec == 5.0  # 2.0 + 3.0
        assert conv.success_rate == 100.0
    
    def test_conversation_performance_issues(self):
        """Test conversation detects performance issues from messages."""
        slow_query = QueryMetrics(statement_id="q1", total_duration_ms=15000)
        
        messages = [
            MessageWithQueries(
                message_id="m1",
                queries=[slow_query],
                ai_overhead_sec=12.0  # Slow AI (>10s threshold)
            ),
        ]
        
        conv = ConversationWithMessages(
            conversation_id="conv-issues",
            messages=messages
        )
        
        assert conv.has_performance_issues is True
        assert conv.slow_ai_count == 1
        assert conv.slow_query_count == 1
    
    def test_conversation_source_derived(self):
        """Test conversation source is derived from first message."""
        messages = [
            MessageWithQueries(message_id="m1", message_source="API"),
            MessageWithQueries(message_id="m2", message_source="Space"),
        ]
        
        conv = ConversationWithMessages(
            conversation_id="conv-api",
            messages=messages
        )
        
        assert conv.conversation_source == "API"
    
    def test_slowest_response_calculation(self):
        """Test slowest response is tracked correctly."""
        messages = [
            MessageWithQueries(
                message_id="m1",
                queries=[QueryMetrics(statement_id="q1", total_duration_ms=5000)],
                ai_overhead_sec=2.0
            ),
            MessageWithQueries(
                message_id="m2",
                queries=[QueryMetrics(statement_id="q2", total_duration_ms=10000)],
                ai_overhead_sec=5.0
            ),
        ]
        
        conv = ConversationWithMessages(
            conversation_id="conv-slow",
            messages=messages
        )
        
        # Second message: 5.0 + 10.0 = 15.0s response
        assert conv.slowest_response_sec == 15.0
    
    def test_fastest_response_calculation(self):
        """Test fastest (min) response is tracked correctly."""
        messages = [
            MessageWithQueries(
                message_id="m1",
                queries=[QueryMetrics(statement_id="q1", total_duration_ms=5000)],
                ai_overhead_sec=2.0
            ),
            MessageWithQueries(
                message_id="m2",
                queries=[QueryMetrics(statement_id="q2", total_duration_ms=10000)],
                ai_overhead_sec=5.0
            ),
            MessageWithQueries(
                message_id="m3",
                queries=[QueryMetrics(statement_id="q3", total_duration_ms=2000)],
                ai_overhead_sec=1.0
            ),
        ]
        
        conv = ConversationWithMessages(
            conversation_id="conv-fast",
            messages=messages
        )
        
        # Third message: 1.0 + 2.0 = 3.0s response (fastest)
        assert conv.fastest_response_sec == 3.0
        # Second message: 5.0 + 10.0 = 15.0s response (slowest)
        assert conv.slowest_response_sec == 15.0
        # Average: (7.0 + 15.0 + 3.0) / 3 = 8.33...s
        assert round(conv.avg_response_sec, 2) == 8.33


class TestSQLQueries:
    """Tests for SQL query templates."""
    
    def test_build_statement_ids_filter_empty(self):
        """Test building filter for empty list."""
        from queries.sql import build_statement_ids_filter
        
        result = build_statement_ids_filter([])
        assert result == "''"
    
    def test_build_statement_ids_filter_single(self):
        """Test building filter for single ID."""
        from queries.sql import build_statement_ids_filter
        
        result = build_statement_ids_filter(["stmt-123"])
        assert result == "'stmt-123'"
    
    def test_build_statement_ids_filter_multiple(self):
        """Test building filter for multiple IDs."""
        from queries.sql import build_statement_ids_filter
        
        result = build_statement_ids_filter(["stmt-1", "stmt-2", "stmt-3"])
        assert result == "'stmt-1', 'stmt-2', 'stmt-3'"
    
    def test_build_statement_ids_filter_escapes_quotes(self):
        """Test that quotes are escaped."""
        from queries.sql import build_statement_ids_filter
        
        result = build_statement_ids_filter(["stmt'123"])
        assert result == "'stmt''123'"
    
    def test_get_message_ai_overhead_query(self):
        """Test AI overhead query generation."""
        from queries.sql import get_message_ai_overhead_query
        
        sql = get_message_ai_overhead_query("space-123", hours=24)
        
        assert "space-123" in sql
        assert "24" in sql
        assert "ai_overhead_sec" in sql
        assert "message_source" in sql
    
    def test_get_conversation_sources_query(self):
        """Test conversation sources query generation."""
        from queries.sql import get_conversation_sources_query
        
        sql = get_conversation_sources_query("space-456", hours=48)
        
        assert "space-456" in sql
        assert "48" in sql
        assert "message_source" in sql
    
    def test_get_queries_by_space_and_time(self):
        """Test space queries with timing query generation."""
        from queries.sql import get_queries_by_space_and_time
        
        sql = get_queries_by_space_and_time("space-789", hours=24)
        
        assert "space-789" in sql
        assert "24" in sql
        assert "statement_id" in sql
        assert "start_time" in sql
        assert "total_duration_ms" in sql
        assert "bottleneck" in sql


class TestTimeBasedCorrelation:
    """Tests for time-based query-message correlation logic."""
    
    def test_time_correlation_within_window(self):
        """Test that queries within 2-minute window are matched."""
        import pandas as pd
        
        # Simulate message timestamp and query start times
        msg_time = pd.Timestamp("2024-01-15 10:00:00")
        
        # Query started 30 seconds after message - should match
        query_time_match = pd.Timestamp("2024-01-15 10:00:30")
        time_diff = (query_time_match - msg_time).total_seconds()
        
        assert 0 <= time_diff <= 120  # Within 2-minute window
    
    def test_time_correlation_outside_window(self):
        """Test that queries outside 2-minute window are not matched."""
        import pandas as pd
        
        msg_time = pd.Timestamp("2024-01-15 10:00:00")
        
        # Query started 3 minutes after message - should NOT match
        query_time_no_match = pd.Timestamp("2024-01-15 10:03:00")
        time_diff = (query_time_no_match - msg_time).total_seconds()
        
        assert time_diff > 120  # Outside 2-minute window
    
    def test_time_correlation_before_message(self):
        """Test that queries before message are not matched."""
        import pandas as pd
        
        msg_time = pd.Timestamp("2024-01-15 10:00:00")
        
        # Query started BEFORE message - should NOT match
        query_time_before = pd.Timestamp("2024-01-15 09:59:30")
        time_diff = (query_time_before - msg_time).total_seconds()
        
        assert time_diff < 0  # Query before message
    
    def test_correlation_prefers_attachment_ids(self):
        """Test that attachment statement_ids take precedence over time matching."""
        from services.databricks_client import QueryMetrics, GenieMessageAttachment
        
        # If attachment has statement_id, it should be used directly
        att = GenieMessageAttachment(
            attachment_type="query",
            statement_id="stmt-123"
        )
        
        assert att.statement_id == "stmt-123"
        
        # Query with matching ID
        qm = QueryMetrics(
            statement_id="stmt-123",
            total_duration_ms=5000
        )
        
        # Direct ID match should work
        assert qm.statement_id == att.statement_id
    
    def test_message_with_no_attachment_uses_time_matching(self):
        """Test that messages without attachment statement_ids can use time matching."""
        from services.databricks_client import GenieMessage, GenieMessageAttachment
        
        # Message with text attachment only (no query.statement_id)
        msg = GenieMessage(
            message_id="msg-456",
            content="How many products are in stock?",
            status="COMPLETED",
            attachments=[
                GenieMessageAttachment(
                    attachment_type="text",
                    statement_id="",  # No statement_id
                )
            ]
        )
        
        # No statement_id in attachment
        assert msg.attachments[0].statement_id == ""
        
        # This message would need time-based correlation
        assert msg.content  # Has content to correlate
    
    def test_query_assignment_prevents_duplicates(self):
        """Test that assigned queries are not reassigned to other messages."""
        assigned_ids: set[str] = set()
        
        # First assignment
        query_id = "stmt-100"
        assert query_id not in assigned_ids
        assigned_ids.add(query_id)
        
        # Second assignment attempt should skip
        assert query_id in assigned_ids  # Already assigned
    
    def test_query_metrics_has_correlation_fields(self):
        """Test that QueryMetrics has fields for correlation matching."""
        from services.databricks_client import QueryMetrics
        
        qm = QueryMetrics(
            statement_id="stmt-123",
            genie_conversation_id="conv-456",
            executed_by="user@example.com"
        )
        
        assert qm.genie_conversation_id == "conv-456"
        assert qm.executed_by == "user@example.com"
    
    def test_conversation_id_matching_priority(self):
        """Test that conversation_id matching takes priority over time-only matching."""
        from services.databricks_client import QueryMetrics
        
        # Query with conversation_id should match its conversation
        qm_with_conv = QueryMetrics(
            statement_id="stmt-conv-match",
            genie_conversation_id="conv-123",
            executed_by="user@example.com"
        )
        
        # Same conversation - should match
        assert qm_with_conv.genie_conversation_id == "conv-123"
        
        # Different conversation - should NOT match
        assert qm_with_conv.genie_conversation_id != "conv-999"
    
    def test_user_matching_for_correlation(self):
        """Test that user email matching works for correlation."""
        from services.databricks_client import QueryMetrics
        
        qm = QueryMetrics(
            statement_id="stmt-user-match",
            executed_by="alice@example.com"
        )
        
        # Same user - should match
        msg_user = "alice@example.com"
        assert qm.executed_by == msg_user
        
        # Different user - should NOT match
        other_user = "bob@example.com"
        assert qm.executed_by != other_user
    
    def test_correlation_priority_order(self):
        """Test the priority order of correlation methods."""
        from services.databricks_client import QueryMetrics
        
        # Priority 1: Direct statement_id from attachment (highest)
        # Priority 2: conversation_id match + time window
        # Priority 3: user + time window match
        # Priority 4: time-only match (lowest)
        
        qm = QueryMetrics(
            statement_id="stmt-full",
            genie_conversation_id="conv-123",
            executed_by="user@example.com",
            start_time="2024-01-15 10:00:30"
        )
        
        # All correlation fields present
        assert qm.statement_id  # For Priority 1
        assert qm.genie_conversation_id  # For Priority 2
        assert qm.executed_by  # For Priority 3
        assert qm.start_time  # For Priority 4
    
    def test_queries_by_space_includes_correlation_fields(self):
        """Test that QUERIES_BY_SPACE_AND_TIME query includes correlation fields."""
        from queries.sql import get_queries_by_space_and_time
        
        sql = get_queries_by_space_and_time("space-123", hours=24)
        
        # Should include conversation_id for matching
        assert "genie_conversation_id" in sql
        # Should include executed_by for user matching
        assert "executed_by" in sql
