"""
Unit Tests for Analytics Service

Tests all analytics functions in services/analytics.py
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.analytics import (
    classify_bottleneck,
    get_query_timeline,
    get_query_optimizations,
    get_bottleneck_recommendation,
    get_speed_category,
    map_status,
    QueryOptimization,
    QueryTimeline,
)


class TestClassifyBottleneck:
    """Tests for classify_bottleneck function."""
    
    def test_zero_total_returns_normal(self):
        assert classify_bottleneck(0, 0, 0, 0, 0) == "NORMAL"
    
    def test_negative_total_returns_normal(self):
        assert classify_bottleneck(100, 100, 0, 0, -1) == "NORMAL"
    
    def test_compute_startup_bottleneck(self):
        # 60% of time in compute wait
        result = classify_bottleneck(100, 300, 0, 600, 1000)
        assert result == "COMPUTE_STARTUP"
    
    def test_queue_wait_bottleneck(self):
        # 40% of time in queue
        result = classify_bottleneck(100, 500, 400, 0, 1000)
        assert result == "QUEUE_WAIT"
    
    def test_compilation_bottleneck(self):
        # 50% of time in compilation
        result = classify_bottleneck(500, 400, 0, 0, 1000)
        assert result == "COMPILATION"
    
    def test_large_scan_bottleneck(self):
        # Over 1 GB scanned
        result = classify_bottleneck(100, 800, 0, 0, 1000, bytes_scanned=2_000_000_000)
        assert result == "LARGE_SCAN"
    
    def test_slow_execution_bottleneck(self):
        # Execution over 10 seconds
        result = classify_bottleneck(100, 15000, 0, 0, 20000)
        assert result == "SLOW_EXECUTION"
    
    def test_normal_query(self):
        # Normal fast query
        result = classify_bottleneck(100, 500, 50, 100, 1000)
        assert result == "NORMAL"
    
    def test_priority_order(self):
        # Compute startup takes priority over queue wait
        result = classify_bottleneck(100, 100, 350, 600, 1000)
        assert result == "COMPUTE_STARTUP"


class TestGetQueryTimeline:
    """Tests for get_query_timeline function."""
    
    def test_empty_query_returns_timeline(self):
        timeline = get_query_timeline({})
        assert len(timeline) == 5
        for phase in timeline:
            assert isinstance(phase, QueryTimeline)
    
    def test_timeline_phases_correct(self):
        query = {
            "queue_wait_ms": 1000,
            "compute_wait_ms": 2000,
            "compilation_ms": 500,
            "execution_ms": 6000,
            "result_fetch_ms": 500,
            "total_duration_ms": 10000,
        }
        timeline = get_query_timeline(query)
        
        phases = [t.phase for t in timeline]
        assert phases == ["Queue Wait", "Compute Startup", "Compilation", "Execution", "Result Fetch"]
    
    def test_timeline_durations_correct(self):
        query = {
            "queue_wait_ms": 1000,
            "compute_wait_ms": 2000,
            "compilation_ms": 500,
            "execution_ms": 6000,
            "result_fetch_ms": 500,
            "total_duration_ms": 10000,
        }
        timeline = get_query_timeline(query)
        
        durations = {t.phase: t.duration_ms for t in timeline}
        assert durations["Queue Wait"] == 1000
        assert durations["Compute Startup"] == 2000
        assert durations["Compilation"] == 500
        assert durations["Execution"] == 6000
        assert durations["Result Fetch"] == 500
    
    def test_timeline_percentages_sum_to_100(self):
        query = {
            "queue_wait_ms": 1000,
            "compute_wait_ms": 2000,
            "compilation_ms": 500,
            "execution_ms": 6000,
            "result_fetch_ms": 500,
            "total_duration_ms": 10000,
        }
        timeline = get_query_timeline(query)
        
        total_pct = sum(t.percentage for t in timeline)
        assert abs(total_pct - 100.0) < 0.1
    
    def test_timeline_handles_none_values(self):
        query = {
            "queue_wait_ms": None,
            "compute_wait_ms": None,
            "compilation_ms": 500,
            "execution_ms": 500,
            "result_fetch_ms": None,
            "total_duration_ms": 1000,
        }
        timeline = get_query_timeline(query)
        assert len(timeline) == 5


class TestGetQueryOptimizations:
    """Tests for get_query_optimizations function."""
    
    def test_normal_query_returns_ok(self):
        query = {
            "total_duration_ms": 1000,
            "compilation_ms": 100,
            "execution_ms": 800,
            "queue_wait_ms": 50,
            "compute_wait_ms": 50,
            "bytes_scanned": 1000000,
            "rows_scanned": 1000,
            "rows_returned": 100,
        }
        opts = get_query_optimizations(query)
        assert len(opts) >= 1
        # Title may include emoji prefix
        assert "Performing Well" in opts[0].title
    
    def test_slow_execution_recommendation(self):
        query = {
            "total_duration_ms": 120000,
            "execution_ms": 100000,
            "compilation_ms": 100,
            "queue_wait_ms": 100,
            "compute_wait_ms": 100,
            "bytes_scanned": 0,
            "rows_scanned": 0,
            "rows_returned": 0,
            "bottleneck": "SLOW_EXECUTION",
        }
        opts = get_query_optimizations(query)
        # Check if any title contains the key text
        assert any("Slow Query Execution" in o.title for o in opts)
    
    def test_large_scan_recommendation(self):
        query = {
            "total_duration_ms": 30000,
            "execution_ms": 25000,
            "compilation_ms": 100,
            "queue_wait_ms": 100,
            "compute_wait_ms": 100,
            "bytes_scanned": 5_000_000_000,  # 5 GB
            "rows_scanned": 0,
            "rows_returned": 0,
            "bottleneck": "LARGE_SCAN",
        }
        opts = get_query_optimizations(query)
        # Check if any title contains the key text
        assert any("Large Data Scan" in o.title for o in opts)
    
    def test_queue_wait_recommendation(self):
        query = {
            "total_duration_ms": 30000,
            "execution_ms": 5000,
            "compilation_ms": 100,
            "queue_wait_ms": 20000,
            "compute_wait_ms": 100,
            "bytes_scanned": 0,
            "rows_scanned": 0,
            "rows_returned": 0,
            "bottleneck": "QUEUE_WAIT",
        }
        opts = get_query_optimizations(query)
        # Check if any title contains queue-related text
        assert any("Queue" in o.title for o in opts)
    
    def test_compilation_recommendation(self):
        query = {
            "total_duration_ms": 15000,
            "execution_ms": 5000,
            "compilation_ms": 8000,
            "queue_wait_ms": 100,
            "compute_wait_ms": 100,
            "bytes_scanned": 0,
            "rows_scanned": 0,
            "rows_returned": 0,
            "bottleneck": "COMPILATION",
        }
        opts = get_query_optimizations(query)
        # Check if any title contains compilation-related text
        assert any("Compilation" in o.title for o in opts)
    
    def test_optimization_has_all_fields(self):
        query = {"total_duration_ms": 120000, "execution_ms": 100000}
        opts = get_query_optimizations(query)
        
        # Valid categories now include more types
        valid_categories = ["performance", "cost", "reliability", "query_design", 
                           "data_design", "infrastructure", "ai_processing"]
        
        for opt in opts:
            assert isinstance(opt, QueryOptimization)
            assert opt.category in valid_categories
            assert opt.severity in ["high", "medium", "low"]
            assert len(opt.title) > 0
            assert len(opt.description) > 0
            assert len(opt.recommendation) > 0


class TestGetBottleneckRecommendation:
    """Tests for get_bottleneck_recommendation function."""
    
    def test_compute_startup(self):
        rec = get_bottleneck_recommendation("COMPUTE_STARTUP")
        assert "warehouse" in rec.lower() or "auto-suspend" in rec.lower()
    
    def test_queue_wait(self):
        rec = get_bottleneck_recommendation("QUEUE_WAIT")
        assert "scale" in rec.lower() or "warehouse" in rec.lower()
    
    def test_compilation(self):
        rec = get_bottleneck_recommendation("COMPILATION")
        assert "simplify" in rec.lower() or "smaller" in rec.lower()
    
    def test_large_scan(self):
        rec = get_bottleneck_recommendation("LARGE_SCAN")
        assert "partition" in rec.lower() or "filter" in rec.lower()
    
    def test_slow_execution(self):
        rec = get_bottleneck_recommendation("SLOW_EXECUTION")
        assert "query plan" in rec.lower() or "index" in rec.lower() or "optimize" in rec.lower()
    
    def test_normal(self):
        rec = get_bottleneck_recommendation("NORMAL")
        assert "well" in rec.lower()
    
    def test_unknown(self):
        rec = get_bottleneck_recommendation("UNKNOWN_TYPE")
        # Should contain actionable advice
        assert "query profile" in rec.lower() or "optimization" in rec.lower()


class TestGetSpeedCategory:
    """Tests for get_speed_category function."""
    
    def test_fast_under_5s(self):
        assert get_speed_category(1000) == "FAST"
        assert get_speed_category(4999) == "FAST"
    
    def test_moderate_5_to_10s(self):
        assert get_speed_category(5000) == "MODERATE"
        assert get_speed_category(9999) == "MODERATE"
    
    def test_slow_10_to_30s(self):
        assert get_speed_category(10000) == "SLOW"
        assert get_speed_category(29999) == "SLOW"
    
    def test_critical_over_30s(self):
        assert get_speed_category(30000) == "CRITICAL"
        assert get_speed_category(120000) == "CRITICAL"


class TestMapStatus:
    """Tests for map_status function."""
    
    def test_finished_maps_to_success(self):
        assert map_status("FINISHED") == "success"
        assert map_status("finished") == "success"
        assert map_status("SUCCEEDED") == "success"
    
    def test_failed_maps_to_failed(self):
        assert map_status("FAILED") == "failed"
        assert map_status("failed") == "failed"
    
    def test_cancelled_maps_to_cancelled(self):
        assert map_status("CANCELED") == "cancelled"
        assert map_status("CANCELLED") == "cancelled"
        assert map_status("canceled") == "cancelled"
    
    def test_none_maps_to_unknown(self):
        assert map_status(None) == "unknown"
    
    def test_empty_maps_to_unknown(self):
        assert map_status("") == "unknown"
    
    def test_unknown_status_maps_to_unknown(self):
        assert map_status("RUNNING") == "unknown"
        assert map_status("PENDING") == "unknown"
