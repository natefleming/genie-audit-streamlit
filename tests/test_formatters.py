"""
Unit Tests for Formatting Utilities

Tests all formatting functions in utils/formatters.py
"""

import pytest
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.formatters import (
    format_duration,
    format_number,
    format_bytes,
    format_percentage,
    format_datetime,
    format_date,
    get_bottleneck_label,
    get_bottleneck_color,
    get_status_color,
    get_speed_color,
    BOTTLENECK_LABELS,
    BOTTLENECK_COLORS,
    STATUS_COLORS,
    SPEED_COLORS,
)


class TestFormatDuration:
    """Tests for format_duration function."""
    
    def test_none_returns_zero(self):
        assert format_duration(None) == "0s"
    
    def test_zero_returns_zero(self):
        assert format_duration(0) == "0s"
    
    def test_milliseconds(self):
        assert format_duration(500) == "500ms"
        assert format_duration(1) == "1ms"
        assert format_duration(999) == "999ms"
    
    def test_seconds(self):
        assert format_duration(1000) == "1.0s"
        assert format_duration(1500) == "1.5s"
        assert format_duration(30000) == "30.0s"
        assert format_duration(59999) == "60.0s"
    
    def test_minutes(self):
        assert format_duration(60000) == "1m"
        assert format_duration(90000) == "1m 30s"
        assert format_duration(120000) == "2m"
        assert format_duration(3599000) == "59m 59s"
    
    def test_hours(self):
        # 1 hour exactly
        result_1h = format_duration(3600000)
        assert "h" in result_1h
        
        # 1.5 hours = 90 minutes - formatted with rounded hours
        result_1_5h = format_duration(5400000)
        assert "h" in result_1_5h and "30" in result_1_5h and "m" in result_1_5h
        
        # 2 hours
        result_2h = format_duration(7200000)
        assert "2" in result_2h and "h" in result_2h
    
    def test_float_input(self):
        assert format_duration(1500.5) == "1.5s"


class TestFormatNumber:
    """Tests for format_number function."""
    
    def test_none_returns_zero(self):
        assert format_number(None) == "0"
    
    def test_small_integers(self):
        assert format_number(0) == "0"
        assert format_number(1) == "1"
        assert format_number(999) == "999"
    
    def test_thousands(self):
        assert format_number(1000) == "1.0K"
        assert format_number(1500) == "1.5K"
        assert format_number(999999) == "1000.0K"
    
    def test_millions(self):
        assert format_number(1000000) == "1.0M"
        assert format_number(5500000) == "5.5M"
    
    def test_billions(self):
        assert format_number(1000000000) == "1.0B"
        assert format_number(2500000000) == "2.5B"
    
    def test_negative_numbers(self):
        assert format_number(-1000) == "-1.0K"
        assert format_number(-1000000) == "-1.0M"
    
    def test_decimal_numbers(self):
        assert format_number(3.14159) == "3.14"


class TestFormatBytes:
    """Tests for format_bytes function."""
    
    def test_none_returns_zero(self):
        assert format_bytes(None) == "0 B"
    
    def test_zero_returns_zero(self):
        assert format_bytes(0) == "0 B"
    
    def test_bytes(self):
        assert format_bytes(100) == "100.0 B"
        assert format_bytes(1023) == "1023.0 B"
    
    def test_kilobytes(self):
        assert format_bytes(1024) == "1.0 KB"
        assert format_bytes(1536) == "1.5 KB"
    
    def test_megabytes(self):
        assert format_bytes(1048576) == "1.0 MB"
        assert format_bytes(104857600) == "100.0 MB"
    
    def test_gigabytes(self):
        assert format_bytes(1073741824) == "1.0 GB"
        assert format_bytes(5368709120) == "5.0 GB"
    
    def test_terabytes(self):
        assert format_bytes(1099511627776) == "1.0 TB"


class TestFormatPercentage:
    """Tests for format_percentage function."""
    
    def test_none_returns_zero(self):
        assert format_percentage(None) == "0%"
    
    def test_integer_percentage(self):
        assert format_percentage(100) == "100.0%"
        assert format_percentage(0) == "0.0%"
        assert format_percentage(50) == "50.0%"
    
    def test_decimal_percentage(self):
        assert format_percentage(97.5) == "97.5%"
        assert format_percentage(99.99) == "100.0%"
    
    def test_custom_decimals(self):
        assert format_percentage(97.567, decimals=2) == "97.57%"
        assert format_percentage(97.567, decimals=0) == "98%"


class TestFormatDatetime:
    """Tests for format_datetime function."""
    
    def test_none_returns_empty(self):
        assert format_datetime(None) == ""
    
    def test_datetime_object(self):
        dt = datetime(2024, 1, 15, 15, 45, 30)
        result = format_datetime(dt)
        assert "Jan 15, 2024" in result
        assert "03:45 PM" in result
    
    def test_iso_string(self):
        result = format_datetime("2024-01-15T15:45:30Z")
        assert "Jan 15, 2024" in result
    
    def test_invalid_string_returns_original(self):
        assert format_datetime("not a date") == "not a date"


class TestFormatDate:
    """Tests for format_date function."""
    
    def test_none_returns_empty(self):
        assert format_date(None) == ""
    
    def test_datetime_object(self):
        dt = datetime(2024, 1, 15, 15, 45, 30)
        result = format_date(dt)
        assert result == "Jan 15, 2024"
    
    def test_iso_string(self):
        result = format_date("2024-01-15T15:45:30Z")
        assert result == "Jan 15, 2024"


class TestGetBottleneckLabel:
    """Tests for get_bottleneck_label function."""
    
    def test_none_returns_normal(self):
        assert get_bottleneck_label(None) == "Normal"
    
    def test_empty_returns_normal(self):
        assert get_bottleneck_label("") == "Normal"
    
    def test_known_bottlenecks(self):
        assert get_bottleneck_label("NORMAL") == "Normal"
        assert get_bottleneck_label("QUEUE_WAIT") == "Queue Wait"
        assert get_bottleneck_label("COMPUTE_STARTUP") == "Compute Startup"
        assert get_bottleneck_label("COMPILATION") == "Compilation"
        assert get_bottleneck_label("SLOW_EXECUTION") == "Slow Execution"
        assert get_bottleneck_label("LARGE_SCAN") == "Large Scan"
    
    def test_lowercase_bottlenecks(self):
        assert get_bottleneck_label("normal") == "Normal"
        assert get_bottleneck_label("queue_wait") == "Queue Wait"
    
    def test_unknown_bottleneck_formatted(self):
        assert get_bottleneck_label("UNKNOWN_TYPE") == "Unknown Type"


class TestGetBottleneckColor:
    """Tests for get_bottleneck_color function."""
    
    def test_none_returns_green(self):
        assert get_bottleneck_color(None) == "#10b981"
    
    def test_empty_returns_green(self):
        assert get_bottleneck_color("") == "#10b981"
    
    def test_known_bottlenecks_have_colors(self):
        assert get_bottleneck_color("NORMAL") == "#10b981"
        assert get_bottleneck_color("QUEUE_WAIT") == "#8dd1e1"
        assert get_bottleneck_color("COMPUTE_STARTUP") == "#f59e0b"
        assert get_bottleneck_color("SLOW_EXECUTION") == "#a855f7"
    
    def test_unknown_returns_gray(self):
        assert get_bottleneck_color("UNKNOWN") == "#6b7280"


class TestGetStatusColor:
    """Tests for get_status_color function."""
    
    def test_none_returns_gray(self):
        assert get_status_color(None) == "#6b7280"
    
    def test_empty_returns_gray(self):
        assert get_status_color("") == "#6b7280"
    
    def test_success_statuses(self):
        assert get_status_color("success") == "#10b981"
        assert get_status_color("FINISHED") == "#10b981"
    
    def test_failed_statuses(self):
        assert get_status_color("failed") == "#ef4444"
        assert get_status_color("FAILED") == "#ef4444"
    
    def test_cancelled_statuses(self):
        assert get_status_color("cancelled") == "#f59e0b"
        assert get_status_color("CANCELED") == "#f59e0b"


class TestGetSpeedColor:
    """Tests for get_speed_color function."""
    
    def test_none_returns_green(self):
        assert get_speed_color(None) == "#10b981"
    
    def test_empty_returns_green(self):
        assert get_speed_color("") == "#10b981"
    
    def test_speed_categories(self):
        assert get_speed_color("FAST") == "#10b981"
        assert get_speed_color("MODERATE") == "#f59e0b"
        assert get_speed_color("SLOW") == "#f97316"
        assert get_speed_color("CRITICAL") == "#ef4444"
    
    def test_unknown_returns_green(self):
        assert get_speed_color("UNKNOWN") == "#10b981"
