"""
Unit Tests for SQL Query Builders

Tests all SQL query building functions in queries/sql.py
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queries.sql import (
    build_space_filter,
    build_status_filter,
    SUMMARY_STATS_QUERY,
    GENIE_SPACES_QUERY,
    SPACE_METRICS_QUERY,
    BOTTLENECK_DISTRIBUTION_QUERY,
    DURATION_HISTOGRAM_QUERY,
    DAILY_TREND_QUERY,
    HOURLY_PATTERN_QUERY,
    TIME_BREAKDOWN_QUERY,
    QUERIES_LIST_QUERY,
    SLOW_QUERIES_QUERY,
    FAILED_QUERIES_QUERY,
    QUERY_DETAIL_QUERY,
    USER_STATS_QUERY,
    IO_HEAVY_QUERIES_QUERY,
)


class TestBuildSpaceFilter:
    """Tests for build_space_filter function."""
    
    def test_none_returns_empty(self):
        assert build_space_filter(None) == ""
    
    def test_empty_string_returns_empty(self):
        assert build_space_filter("") == ""
    
    def test_valid_space_id(self):
        result = build_space_filter("space-123")
        assert "genie_space_id = 'space-123'" in result
        assert result.startswith("AND")
    
    def test_space_id_with_special_chars(self):
        result = build_space_filter("space-abc-123-def")
        assert "space-abc-123-def" in result


class TestBuildStatusFilter:
    """Tests for build_status_filter function."""
    
    def test_none_returns_empty(self):
        assert build_status_filter(None) == ""
    
    def test_empty_string_returns_empty(self):
        assert build_status_filter("") == ""
    
    def test_success_filter(self):
        result = build_status_filter("success")
        assert "FINISHED" in result
        assert result.startswith("AND")
    
    def test_failed_filter(self):
        result = build_status_filter("failed")
        assert "FAILED" in result
    
    def test_cancelled_filter(self):
        result = build_status_filter("cancelled")
        assert "CANCELED" in result or "CANCELLED" in result
    
    def test_case_insensitive(self):
        result_lower = build_status_filter("success")
        result_upper = build_status_filter("SUCCESS")
        result_mixed = build_status_filter("Success")
        
        assert result_lower == result_upper == result_mixed
    
    def test_unknown_status_returns_empty(self):
        assert build_status_filter("running") == ""
        assert build_status_filter("pending") == ""


class TestSummaryStatsQuery:
    """Tests for SUMMARY_STATS_QUERY."""
    
    def test_query_is_valid_sql(self):
        assert "SELECT" in SUMMARY_STATS_QUERY
        assert "FROM system.query.history" in SUMMARY_STATS_QUERY
    
    def test_has_required_columns(self):
        assert "total_queries" in SUMMARY_STATS_QUERY
        assert "genie_spaces" in SUMMARY_STATS_QUERY
        assert "unique_users" in SUMMARY_STATS_QUERY
        assert "avg_duration_sec" in SUMMARY_STATS_QUERY
        assert "success_rate_pct" in SUMMARY_STATS_QUERY
    
    def test_has_hours_placeholder(self):
        assert "{hours}" in SUMMARY_STATS_QUERY
    
    def test_filters_for_genie_queries(self):
        assert "genie_space_id IS NOT NULL" in SUMMARY_STATS_QUERY
    
    def test_can_format_with_hours(self):
        formatted = SUMMARY_STATS_QUERY.format(hours=720)
        assert "720" in formatted
        assert "{hours}" not in formatted


class TestSpaceMetricsQuery:
    """Tests for SPACE_METRICS_QUERY."""
    
    def test_has_required_placeholders(self):
        assert "{space_id}" in SPACE_METRICS_QUERY
        assert "{hours}" in SPACE_METRICS_QUERY
    
    def test_has_required_columns(self):
        assert "total_queries" in SPACE_METRICS_QUERY
        assert "avg_duration_sec" in SPACE_METRICS_QUERY
        assert "successful_queries" in SPACE_METRICS_QUERY
        assert "failed_queries" in SPACE_METRICS_QUERY
    
    def test_can_format(self):
        formatted = SPACE_METRICS_QUERY.format(space_id="test-space", hours=720)
        assert "test-space" in formatted
        assert "720" in formatted


class TestBottleneckDistributionQuery:
    """Tests for BOTTLENECK_DISTRIBUTION_QUERY."""
    
    def test_has_required_placeholders(self):
        assert "{hours}" in BOTTLENECK_DISTRIBUTION_QUERY
        assert "{space_filter}" in BOTTLENECK_DISTRIBUTION_QUERY
    
    def test_classifies_bottlenecks(self):
        assert "CASE" in BOTTLENECK_DISTRIBUTION_QUERY
        assert "Compute Startup" in BOTTLENECK_DISTRIBUTION_QUERY
        assert "Queue Wait" in BOTTLENECK_DISTRIBUTION_QUERY
        assert "Compilation" in BOTTLENECK_DISTRIBUTION_QUERY
        assert "Large Scan" in BOTTLENECK_DISTRIBUTION_QUERY
        assert "Slow Execution" in BOTTLENECK_DISTRIBUTION_QUERY
        assert "Normal" in BOTTLENECK_DISTRIBUTION_QUERY
    
    def test_can_format_with_empty_filter(self):
        formatted = BOTTLENECK_DISTRIBUTION_QUERY.format(hours=720, space_filter="")
        assert "{" not in formatted


class TestDurationHistogramQuery:
    """Tests for DURATION_HISTOGRAM_QUERY."""
    
    def test_has_buckets(self):
        assert "< 1s" in DURATION_HISTOGRAM_QUERY
        assert "1-5s" in DURATION_HISTOGRAM_QUERY
        assert "5-10s" in DURATION_HISTOGRAM_QUERY
        assert "10-30s" in DURATION_HISTOGRAM_QUERY
        assert "30-60s" in DURATION_HISTOGRAM_QUERY
        assert "> 60s" in DURATION_HISTOGRAM_QUERY
    
    def test_has_bucket_order(self):
        assert "bucket_order" in DURATION_HISTOGRAM_QUERY
    
    def test_can_format(self):
        formatted = DURATION_HISTOGRAM_QUERY.format(hours=720, space_filter="")
        assert "{" not in formatted


class TestDailyTrendQuery:
    """Tests for DAILY_TREND_QUERY."""
    
    def test_groups_by_date(self):
        assert "DATE(start_time)" in DAILY_TREND_QUERY
        assert "GROUP BY" in DAILY_TREND_QUERY
    
    def test_has_trend_columns(self):
        assert "total_queries" in DAILY_TREND_QUERY
        assert "slow_queries" in DAILY_TREND_QUERY
        assert "p90_sec" in DAILY_TREND_QUERY
        assert "success_rate" in DAILY_TREND_QUERY
    
    def test_orders_by_date(self):
        assert "ORDER BY query_date" in DAILY_TREND_QUERY


class TestHourlyPatternQuery:
    """Tests for HOURLY_PATTERN_QUERY."""
    
    def test_groups_by_hour(self):
        assert "HOUR(start_time)" in HOURLY_PATTERN_QUERY
        assert "GROUP BY" in HOURLY_PATTERN_QUERY
    
    def test_has_volume_column(self):
        assert "query_count" in HOURLY_PATTERN_QUERY


class TestQueriesListQuery:
    """Tests for QUERIES_LIST_QUERY."""
    
    def test_has_required_placeholders(self):
        assert "{hours}" in QUERIES_LIST_QUERY
        assert "{space_filter}" in QUERIES_LIST_QUERY
        assert "{audit_space_filter}" in QUERIES_LIST_QUERY
        assert "{status_filter}" in QUERIES_LIST_QUERY
        assert "{limit}" in QUERIES_LIST_QUERY
    
    def test_has_timing_columns(self):
        assert "total_sec" in QUERIES_LIST_QUERY
        assert "compile_sec" in QUERIES_LIST_QUERY
        assert "execute_sec" in QUERIES_LIST_QUERY
        assert "queue_sec" in QUERIES_LIST_QUERY
    
    def test_has_ai_overhead_column(self):
        assert "ai_overhead_sec" in QUERIES_LIST_QUERY
    
    def test_has_message_source_column(self):
        assert "message_source" in QUERIES_LIST_QUERY
        assert "'API'" in QUERIES_LIST_QUERY
        assert "'Internal'" in QUERIES_LIST_QUERY
    
    def test_classifies_bottleneck(self):
        assert "bottleneck" in QUERIES_LIST_QUERY
        assert "CASE" in QUERIES_LIST_QUERY
    
    def test_classifies_speed(self):
        assert "speed_category" in QUERIES_LIST_QUERY
        assert "FAST" in QUERIES_LIST_QUERY
        assert "SLOW" in QUERIES_LIST_QUERY
        assert "CRITICAL" in QUERIES_LIST_QUERY
    
    def test_orders_by_duration(self):
        assert "ORDER BY total_duration_ms DESC" in QUERIES_LIST_QUERY
    
    def test_can_format(self):
        formatted = QUERIES_LIST_QUERY.format(
            hours=720,
            space_filter="AND query_source.genie_space_id = 'test'",
            audit_space_filter="AND request_params.space_id = 'test'",
            status_filter="",
            limit=100
        )
        assert "{" not in formatted


class TestQueryDetailQuery:
    """Tests for QUERY_DETAIL_QUERY."""
    
    def test_has_statement_id_placeholder(self):
        assert "{statement_id}" in QUERY_DETAIL_QUERY
    
    def test_has_detailed_columns(self):
        assert "statement_id" in QUERY_DETAIL_QUERY
        assert "start_time" in QUERY_DETAIL_QUERY
        assert "end_time" in QUERY_DETAIL_QUERY
        assert "total_duration_ms" in QUERY_DETAIL_QUERY
        assert "rows_scanned" in QUERY_DETAIL_QUERY
        assert "query_text" in QUERY_DETAIL_QUERY


class TestUserStatsQuery:
    """Tests for USER_STATS_QUERY."""
    
    def test_groups_by_user(self):
        assert "executed_by" in USER_STATS_QUERY
        assert "GROUP BY" in USER_STATS_QUERY
    
    def test_has_user_stats(self):
        assert "query_count" in USER_STATS_QUERY
        assert "avg_sec" in USER_STATS_QUERY
        assert "slow_queries" in USER_STATS_QUERY


class TestIOHeavyQueriesQuery:
    """Tests for IO_HEAVY_QUERIES_QUERY."""
    
    def test_filters_by_bytes(self):
        assert "read_bytes" in IO_HEAVY_QUERIES_QUERY
        assert ">" in IO_HEAVY_QUERIES_QUERY
    
    def test_has_io_columns(self):
        assert "read_gb" in IO_HEAVY_QUERIES_QUERY
        assert "files_read" in IO_HEAVY_QUERIES_QUERY
        assert "prune_pct" in IO_HEAVY_QUERIES_QUERY
    
    def test_has_recommendations(self):
        assert "io_recommendation" in IO_HEAVY_QUERIES_QUERY
