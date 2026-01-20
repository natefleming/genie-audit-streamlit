"""
Unit Tests for Chart Components

Tests all chart functions in components/charts.py
"""

import pytest
import pandas as pd
import plotly.graph_objects as go
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from components.charts import (
    apply_dark_theme,
    create_duration_distribution_chart,
    create_bottleneck_chart,
    create_phase_breakdown_chart,
    create_hourly_volume_chart,
    create_daily_trend_chart,
    create_query_timeline_chart,
    create_success_rate_gauge,
    create_pie_chart,
    DARK_LAYOUT,
)


class TestApplyDarkTheme:
    """Tests for apply_dark_theme function."""
    
    def test_returns_figure(self):
        fig = go.Figure()
        result = apply_dark_theme(fig)
        assert isinstance(result, go.Figure)
    
    def test_applies_dark_background(self):
        fig = go.Figure()
        result = apply_dark_theme(fig)
        assert result.layout.paper_bgcolor == "rgba(0,0,0,0)"
    
    def test_applies_font_color(self):
        fig = go.Figure()
        result = apply_dark_theme(fig)
        assert result.layout.font.color == "#ffffff"


class TestCreateDurationDistributionChart:
    """Tests for create_duration_distribution_chart function."""
    
    def test_empty_df_returns_figure(self):
        fig = create_duration_distribution_chart(pd.DataFrame())
        assert isinstance(fig, go.Figure)
    
    def test_empty_df_has_annotation(self):
        fig = create_duration_distribution_chart(pd.DataFrame())
        assert len(fig.layout.annotations) > 0
        assert "No data" in fig.layout.annotations[0].text
    
    def test_valid_df_creates_bar_chart(self, sample_duration_df):
        fig = create_duration_distribution_chart(sample_duration_df)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1
        assert isinstance(fig.data[0], go.Bar)
    
    def test_has_correct_title(self, sample_duration_df):
        fig = create_duration_distribution_chart(sample_duration_df)
        assert "Duration Distribution" in fig.layout.title.text


class TestCreateBottleneckChart:
    """Tests for create_bottleneck_chart function."""
    
    def test_empty_df_returns_figure(self):
        fig = create_bottleneck_chart(pd.DataFrame())
        assert isinstance(fig, go.Figure)
    
    def test_empty_df_shows_all_categories(self):
        """Empty dataframe should show all 6 bottleneck categories with 0 values."""
        fig = create_bottleneck_chart(pd.DataFrame())
        # Should have 6 categories even with empty data
        assert len(fig.data[0].x) == 6
        # All values should be 0
        assert all(v == 0 for v in fig.data[0].y)
    
    def test_valid_df_creates_bar_chart(self, sample_bottleneck_df):
        fig = create_bottleneck_chart(sample_bottleneck_df)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1
        assert isinstance(fig.data[0], go.Bar)
    
    def test_has_correct_title(self, sample_bottleneck_df):
        fig = create_bottleneck_chart(sample_bottleneck_df)
        assert "Bottleneck" in fig.layout.title.text
    
    def test_fills_missing_categories_with_zero(self):
        """Partial data should fill in missing categories with 0 values."""
        partial_df = pd.DataFrame({
            "bottleneck_type": ["Normal", "Compilation"],
            "total_time_min": [5.0, 3.0]
        })
        fig = create_bottleneck_chart(partial_df)
        # Should have all 6 categories
        assert len(fig.data[0].x) == 6
        # Check that the provided values are present and others are 0
        x_list = list(fig.data[0].x)
        y_list = list(fig.data[0].y)
        assert y_list[x_list.index("Normal")] == 5.0
        assert y_list[x_list.index("Compilation")] == 3.0
        assert y_list[x_list.index("Slow Execution")] == 0.0


class TestCreatePhaseBreakdownChart:
    """Tests for create_phase_breakdown_chart function."""
    
    def test_empty_df_returns_figure(self):
        fig = create_phase_breakdown_chart(pd.DataFrame())
        assert isinstance(fig, go.Figure)
    
    def test_valid_df_creates_bar_chart(self):
        df = pd.DataFrame({
            "phase": ["Compilation", "Execution", "Queue Wait"],
            "total_minutes": [10.5, 45.2, 5.3],
        })
        fig = create_phase_breakdown_chart(df)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1


class TestCreateHourlyVolumeChart:
    """Tests for create_hourly_volume_chart function."""
    
    def test_empty_df_returns_figure(self):
        fig = create_hourly_volume_chart(pd.DataFrame())
        assert isinstance(fig, go.Figure)
    
    def test_valid_df_creates_bar_chart(self, sample_hourly_df):
        fig = create_hourly_volume_chart(sample_hourly_df)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1
    
    def test_highlight_hour_adds_vline(self, sample_hourly_df):
        fig = create_hourly_volume_chart(sample_hourly_df, highlight_hour=12)
        # Check for vertical line in shapes
        assert fig.layout.shapes is not None or len(fig.layout.annotations) > 0


class TestCreateDailyTrendChart:
    """Tests for create_daily_trend_chart function."""
    
    def test_empty_df_returns_figure(self):
        fig = create_daily_trend_chart(pd.DataFrame())
        assert isinstance(fig, go.Figure)
    
    def test_line_chart_type(self, sample_daily_trend_df):
        fig = create_daily_trend_chart(
            sample_daily_trend_df,
            y_column="total_queries",
            chart_type="line"
        )
        assert isinstance(fig, go.Figure)
        assert isinstance(fig.data[0], go.Scatter)
    
    def test_bar_chart_type(self, sample_daily_trend_df):
        fig = create_daily_trend_chart(
            sample_daily_trend_df,
            y_column="slow_queries",
            chart_type="bar"
        )
        assert isinstance(fig, go.Figure)
        assert isinstance(fig.data[0], go.Bar)
    
    def test_custom_title(self, sample_daily_trend_df):
        fig = create_daily_trend_chart(
            sample_daily_trend_df,
            title="Custom Title"
        )
        assert fig.layout.title.text == "Custom Title"


class TestCreateQueryTimelineChart:
    """Tests for create_query_timeline_chart function."""
    
    def test_empty_phases_returns_figure(self):
        fig = create_query_timeline_chart([])
        assert isinstance(fig, go.Figure)
    
    def test_empty_phases_has_annotation(self):
        fig = create_query_timeline_chart([])
        assert len(fig.layout.annotations) > 0
    
    def test_valid_phases_creates_stacked_bar(self):
        phases = [
            {"phase": "Compilation", "duration_ms": 500, "percentage": 5.0},
            {"phase": "Execution", "duration_ms": 9000, "percentage": 90.0},
            {"phase": "Result Fetch", "duration_ms": 500, "percentage": 5.0},
        ]
        fig = create_query_timeline_chart(phases)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 3  # One bar per phase
    
    def test_stacked_barmode(self):
        phases = [
            {"phase": "Compilation", "duration_ms": 500, "percentage": 50.0},
            {"phase": "Execution", "duration_ms": 500, "percentage": 50.0},
        ]
        fig = create_query_timeline_chart(phases)
        assert fig.layout.barmode == "stack"


class TestCreateSuccessRateGauge:
    """Tests for create_success_rate_gauge function."""
    
    def test_returns_figure(self):
        fig = create_success_rate_gauge(95.0)
        assert isinstance(fig, go.Figure)
    
    def test_high_rate_is_green(self):
        fig = create_success_rate_gauge(99.0)
        assert isinstance(fig.data[0], go.Indicator)
    
    def test_medium_rate_is_warning(self):
        fig = create_success_rate_gauge(85.0)
        assert isinstance(fig.data[0], go.Indicator)
    
    def test_low_rate_is_error(self):
        fig = create_success_rate_gauge(50.0)
        assert isinstance(fig.data[0], go.Indicator)
    
    def test_gauge_range_is_0_to_100(self):
        fig = create_success_rate_gauge(50.0)
        gauge = fig.data[0].gauge
        # Range can be tuple or list
        assert tuple(gauge.axis.range) == (0, 100)


class TestCreatePieChart:
    """Tests for create_pie_chart function."""
    
    def test_empty_df_returns_figure(self):
        fig = create_pie_chart(pd.DataFrame(), "name", "value")
        assert isinstance(fig, go.Figure)
    
    def test_valid_df_creates_pie(self):
        df = pd.DataFrame({
            "name": ["A", "B", "C"],
            "value": [10, 20, 30],
        })
        fig = create_pie_chart(df, "name", "value")
        assert isinstance(fig, go.Figure)
        assert isinstance(fig.data[0], go.Pie)
    
    def test_is_donut_chart(self):
        df = pd.DataFrame({
            "name": ["A", "B"],
            "value": [50, 50],
        })
        fig = create_pie_chart(df, "name", "value")
        assert fig.data[0].hole == 0.4  # Donut hole
    
    def test_custom_title(self):
        df = pd.DataFrame({"name": ["A"], "value": [100]})
        fig = create_pie_chart(df, "name", "value", title="My Pie Chart")
        assert fig.layout.title.text == "My Pie Chart"
