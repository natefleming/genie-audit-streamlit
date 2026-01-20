"""
Chart Components

Plotly chart components for the Genie Audit dashboard.
All charts use a dark theme consistent with the application.
"""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import Optional

from utils.formatters import (
    DURATION_BUCKET_COLORS,
    BOTTLENECK_COLORS,
    PHASE_COLORS,
    CHART_COLORS,
)


# Common layout settings for dark theme
DARK_LAYOUT = {
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(18,18,26,0.5)",
    "font": {"color": "#ffffff", "family": "Inter, sans-serif"},
    "margin": {"l": 40, "r": 20, "t": 40, "b": 40},
    "xaxis": {
        "gridcolor": "rgba(255,255,255,0.1)",
        "linecolor": "rgba(255,255,255,0.2)",
        "tickfont": {"color": "#888888"},
    },
    "yaxis": {
        "gridcolor": "rgba(255,255,255,0.1)",
        "linecolor": "rgba(255,255,255,0.2)",
        "tickfont": {"color": "#888888"},
    },
    "legend": {
        "bgcolor": "rgba(0,0,0,0)",
        "font": {"color": "#888888"},
    },
}


def apply_dark_theme(fig: go.Figure) -> go.Figure:
    """Apply dark theme to a Plotly figure."""
    fig.update_layout(**DARK_LAYOUT)
    return fig


def create_duration_distribution_chart(df: pd.DataFrame) -> go.Figure:
    """
    Create a bar chart showing query duration distribution.
    
    Args:
        df: DataFrame with 'duration_bucket' and 'query_count' columns
        
    Returns:
        Plotly Figure
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font={"color": "#888888", "size": 14}
        )
        return apply_dark_theme(fig)
    
    # Ensure proper order
    bucket_order = ["< 1s", "1-5s", "5-10s", "10-30s", "30-60s", "> 60s"]
    df = df.copy()
    df["duration_bucket"] = pd.Categorical(
        df["duration_bucket"], 
        categories=bucket_order, 
        ordered=True
    )
    df = df.sort_values("duration_bucket")
    
    colors = [DURATION_BUCKET_COLORS.get(b, CHART_COLORS["primary"]) for b in df["duration_bucket"]]
    
    fig = go.Figure(data=[
        go.Bar(
            x=df["duration_bucket"],
            y=df["query_count"],
            marker_color=colors,
            hovertemplate="<b>%{x}</b><br>Queries: %{y}<extra></extra>",
        )
    ])
    
    fig.update_layout(
        title="Query Duration Distribution",
        xaxis_title="Duration",
        yaxis_title="Number of Queries",
        showlegend=False,
        height=350,
    )
    
    return apply_dark_theme(fig)


def create_bottleneck_chart(df: pd.DataFrame) -> go.Figure:
    """
    Create a bar chart showing bottleneck distribution.
    
    Args:
        df: DataFrame with 'bottleneck_type' and 'total_time_min' columns
        
    Returns:
        Plotly Figure
    """
    # Define all possible bottleneck types in order (most severe to normal)
    ALL_BOTTLENECK_TYPES = [
        "Slow Execution",
        "Large Scan", 
        "Queue Wait",
        "Compute Startup",
        "Compilation",
        "Normal",
    ]
    
    if df.empty:
        # Show empty chart with all categories at 0
        chart_df = pd.DataFrame({
            "bottleneck_type": ALL_BOTTLENECK_TYPES,
            "total_time_min": [0.0] * len(ALL_BOTTLENECK_TYPES)
        })
    else:
        # Ensure all categories are present, filling missing ones with 0
        existing = dict(zip(df["bottleneck_type"], df["total_time_min"]))
        chart_df = pd.DataFrame({
            "bottleneck_type": ALL_BOTTLENECK_TYPES,
            "total_time_min": [float(existing.get(bt, 0)) for bt in ALL_BOTTLENECK_TYPES]
        })
    
    colors = [BOTTLENECK_COLORS.get(b, CHART_COLORS["muted"]) for b in chart_df["bottleneck_type"]]
    
    fig = go.Figure(data=[
        go.Bar(
            x=chart_df["bottleneck_type"],
            y=chart_df["total_time_min"],
            marker_color=colors,
            hovertemplate="<b>%{x}</b><br>Time Lost: %{y:.1f} min<extra></extra>",
        )
    ])
    
    fig.update_layout(
        title="Time Lost by Bottleneck Type",
        xaxis_title="Bottleneck Type",
        yaxis_title="Total Time (minutes)",
        showlegend=False,
        height=350,
    )
    
    return apply_dark_theme(fig)


def create_phase_breakdown_chart(df: pd.DataFrame) -> go.Figure:
    """
    Create a bar chart showing time by query phase.
    
    Args:
        df: DataFrame with 'phase' and 'total_minutes' columns
        
    Returns:
        Plotly Figure
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font={"color": "#888888", "size": 14}
        )
        return apply_dark_theme(fig)
    
    colors = [PHASE_COLORS.get(p, CHART_COLORS["primary"]) for p in df["phase"]]
    
    fig = go.Figure(data=[
        go.Bar(
            x=df["phase"],
            y=df["total_minutes"],
            marker_color=colors,
            hovertemplate="<b>%{x}</b><br>Time: %{y:.1f} min<extra></extra>",
        )
    ])
    
    fig.update_layout(
        title="Time by Query Phase",
        xaxis_title="Phase",
        yaxis_title="Total Time (minutes)",
        showlegend=False,
        height=350,
    )
    
    return apply_dark_theme(fig)


def create_response_time_breakdown_chart(df: pd.DataFrame, use_seconds: bool = False) -> go.Figure:
    """
    Create a horizontal bar chart showing response time breakdown by phase.
    
    Shows time spent in each phase of the Genie response pipeline:
    AI Processing -> Queue Wait -> Compute Startup -> Compilation -> Execution
    
    Args:
        df: DataFrame with 'phase', 'time_min', 'avg_sec', and optionally 'pct' columns
        use_seconds: If True, display values in seconds (for individual queries).
                     If False, display in minutes (for room aggregate).
        
    Returns:
        Plotly Figure
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font={"color": "#888888", "size": 14}
        )
        return apply_dark_theme(fig)
    
    # Choose the appropriate column and labels based on display mode
    if use_seconds and "avg_sec" in df.columns:
        time_col = "avg_sec"
        time_label = "seconds"
        time_suffix = "s"
    else:
        time_col = "time_min"
        time_label = "minutes"
        time_suffix = " min"
    
    # Calculate percentages if not provided
    if "pct" not in df.columns:
        total = df[time_col].sum()
        if total > 0:
            df = df.copy()
            df["pct"] = (df[time_col] / total * 100).round(1)
        else:
            df = df.copy()
            df["pct"] = 0
    
    # Get colors for each phase
    colors = [PHASE_COLORS.get(p, CHART_COLORS["primary"]) for p in df["phase"]]
    
    # Format text labels based on display mode
    if use_seconds:
        text_labels = [f"{t:.2f}s ({p:.0f}%)" for t, p in zip(df[time_col], df["pct"])]
        hover_template = "<b>%{y}</b><br>Time: %{x:.2f}s<br>Percentage: %{customdata:.1f}%<extra></extra>"
    else:
        text_labels = [f"{t:.1f} min ({p:.0f}%)" for t, p in zip(df[time_col], df["pct"])]
        hover_template = "<b>%{y}</b><br>Time: %{x:.1f} min<br>Percentage: %{customdata:.1f}%<extra></extra>"
    
    # Create horizontal bar chart
    fig = go.Figure(data=[
        go.Bar(
            y=df["phase"],
            x=df[time_col],
            orientation='h',
            marker_color=colors,
            text=text_labels,
            textposition='auto',
            textfont={"color": "#ffffff", "size": 11},
            hovertemplate=hover_template,
            customdata=df["pct"],
        )
    ])
    
    fig.update_layout(
        title="Response Time Breakdown",
        xaxis_title=f"Time ({time_label})",
        yaxis_title="",
        showlegend=False,
        height=300,
        yaxis={"categoryorder": "array", "categoryarray": df["phase"].tolist()[::-1]},  # Reverse order for top-to-bottom
    )
    
    return apply_dark_theme(fig)


def create_hourly_volume_chart(df: pd.DataFrame, highlight_hour: Optional[int] = None) -> go.Figure:
    """
    Create a bar chart showing query volume by hour.
    
    Args:
        df: DataFrame with 'hour_of_day' and 'query_count' columns
        highlight_hour: Optional hour to highlight (0-23)
        
    Returns:
        Plotly Figure
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font={"color": "#888888", "size": 14}
        )
        return apply_dark_theme(fig)
    
    # Create color list with optional highlight
    colors = [CHART_COLORS["info"]] * len(df)
    if highlight_hour is not None:
        for i, hour in enumerate(df["hour_of_day"]):
            if hour == highlight_hour:
                colors[i] = CHART_COLORS["accent"]
    
    fig = go.Figure(data=[
        go.Bar(
            x=df["hour_of_day"],
            y=df["query_count"],
            marker_color=colors,
            hovertemplate="<b>Hour %{x}:00</b><br>Queries: %{y}<extra></extra>",
        )
    ])
    
    # Add highlight reference line
    if highlight_hour is not None:
        fig.add_vline(
            x=highlight_hour,
            line_dash="dash",
            line_color=CHART_COLORS["primary"],
            annotation_text="Selected",
            annotation_position="top",
        )
    
    fig.update_layout(
        title="Query Volume by Hour (UTC)",
        xaxis_title="Hour of Day",
        yaxis_title="Number of Queries",
        showlegend=False,
        height=350,
    )
    
    fig.update_xaxes(tickmode="linear", tick0=0, dtick=3)
    
    return apply_dark_theme(fig)


def create_daily_trend_chart(
    df: pd.DataFrame, 
    y_column: str = "total_queries",
    title: str = "Daily Query Volume",
    chart_type: str = "line"
) -> go.Figure:
    """
    Create a line or bar chart showing daily trends.
    
    Args:
        df: DataFrame with 'query_date' and the specified y_column
        y_column: Column name for y-axis values
        title: Chart title
        chart_type: 'line' or 'bar'
        
    Returns:
        Plotly Figure
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font={"color": "#888888", "size": 14}
        )
        return apply_dark_theme(fig)
    
    df = df.copy()
    df["query_date"] = pd.to_datetime(df["query_date"])
    
    if chart_type == "bar":
        fig = go.Figure(data=[
            go.Bar(
                x=df["query_date"],
                y=df[y_column],
                marker_color=CHART_COLORS["info"],
                hovertemplate="<b>%{x|%b %d}</b><br>%{y}<extra></extra>",
            )
        ])
    else:
        fig = go.Figure(data=[
            go.Scatter(
                x=df["query_date"],
                y=df[y_column],
                mode="lines",
                line={"color": CHART_COLORS["info"], "width": 2},
                hovertemplate="<b>%{x|%b %d}</b><br>%{y}<extra></extra>",
            )
        ])
    
    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title=y_column.replace("_", " ").title(),
        showlegend=False,
        height=250,
    )
    
    return apply_dark_theme(fig)


def create_query_timeline_chart(phases: list[dict]) -> go.Figure:
    """
    Create a horizontal timeline chart showing query phase breakdown.
    
    Args:
        phases: List of dicts with 'phase', 'duration_ms', 'percentage' keys
        
    Returns:
        Plotly Figure
    """
    if not phases:
        fig = go.Figure()
        fig.add_annotation(
            text="No timeline data",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font={"color": "#888888", "size": 14}
        )
        return apply_dark_theme(fig)
    
    # Create stacked horizontal bar
    fig = go.Figure()
    
    cumulative = 0
    for phase in phases:
        color = PHASE_COLORS.get(phase["phase"], CHART_COLORS["muted"])
        duration_sec = phase["duration_ms"] / 1000
        
        fig.add_trace(go.Bar(
            y=["Query Timeline"],
            x=[duration_sec],
            name=phase["phase"],
            orientation="h",
            marker_color=color,
            text=f"{phase['phase']}: {duration_sec:.1f}s ({phase['percentage']:.1f}%)",
            textposition="inside",
            insidetextanchor="middle",
            hovertemplate=f"<b>{phase['phase']}</b><br>{duration_sec:.2f}s ({phase['percentage']:.1f}%)<extra></extra>",
        ))
        cumulative += duration_sec
    
    fig.update_layout(
        barmode="stack",
        title="Query Execution Timeline",
        xaxis_title="Duration (seconds)",
        showlegend=True,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "center", "x": 0.5},
        height=150,
        margin={"l": 20, "r": 20, "t": 60, "b": 40},
    )
    
    fig.update_yaxes(visible=False)
    
    return apply_dark_theme(fig)


def create_success_rate_gauge(success_rate: float) -> go.Figure:
    """
    Create a gauge chart showing success rate.
    
    Args:
        success_rate: Success rate percentage (0-100)
        
    Returns:
        Plotly Figure
    """
    # Determine color based on rate
    if success_rate >= 95:
        color = CHART_COLORS["success"]
    elif success_rate >= 80:
        color = CHART_COLORS["warning"]
    else:
        color = CHART_COLORS["error"]
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=success_rate,
        number={"suffix": "%", "font": {"size": 24, "color": "#ffffff"}},
        gauge={
            "axis": {"range": [0, 100], "tickcolor": "#888888"},
            "bar": {"color": color},
            "bgcolor": "rgba(255,255,255,0.1)",
            "borderwidth": 0,
            "steps": [
                {"range": [0, 80], "color": "rgba(239,68,68,0.2)"},
                {"range": [80, 95], "color": "rgba(245,158,11,0.2)"},
                {"range": [95, 100], "color": "rgba(16,185,129,0.2)"},
            ],
        },
        title={"text": "Success Rate", "font": {"color": "#888888", "size": 14}},
    ))
    
    fig.update_layout(
        height=200,
        margin={"l": 20, "r": 20, "t": 40, "b": 20},
    )
    
    return apply_dark_theme(fig)


def create_pie_chart(df: pd.DataFrame, names_col: str, values_col: str, title: str = "") -> go.Figure:
    """
    Create a pie/donut chart.
    
    Args:
        df: DataFrame with data
        names_col: Column for segment names
        values_col: Column for segment values
        title: Chart title
        
    Returns:
        Plotly Figure
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font={"color": "#888888", "size": 14}
        )
        return apply_dark_theme(fig)
    
    fig = go.Figure(data=[
        go.Pie(
            labels=df[names_col],
            values=df[values_col],
            hole=0.4,
            marker={"colors": [BOTTLENECK_COLORS.get(n, CHART_COLORS["primary"]) for n in df[names_col]]},
            textinfo="percent+label",
            textposition="outside",
            textfont={"color": "#ffffff"},
        )
    ])
    
    fig.update_layout(
        title=title,
        showlegend=False,
        height=300,
    )
    
    return apply_dark_theme(fig)


def create_conversation_activity_chart(
    df: pd.DataFrame,
    time_col: str = "event_date",
    count_col: str = "message_count",
    type_col: str = "message_type",
    title: str = "AI Conversation Activity",
    chart_type: str = "stacked_bar",
) -> go.Figure:
    """
    Create a stacked bar chart showing conversation activity over time by message type.
    
    Args:
        df: DataFrame with time, count, and type columns
        time_col: Column name for time values
        count_col: Column name for count values
        type_col: Column name for message type (for stacking)
        title: Chart title
        chart_type: "stacked_bar", "bar", or "line"
        
    Returns:
        Plotly figure
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No conversation data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font={"color": "#888888", "size": 14}
        )
        return apply_dark_theme(fig)
    
    # Convert count column to numeric
    df = df.copy()
    df[count_col] = pd.to_numeric(df[count_col], errors='coerce').fillna(0)
    
    # Colors for each message type
    type_colors = {
        "New Conversation": "#22C55E",      # Green - new conversations
        "Follow-up Message": "#3B82F6",     # Blue - follow-ups
        "Message Created": "#8B5CF6",       # Purple - general messages
        "Regenerate Response": "#F59E0B",   # Orange - regenerations
        "Other": "#6B7280",                 # Gray - other
    }
    
    if chart_type == "stacked_bar" and type_col in df.columns:
        fig = go.Figure()
        
        # Get unique message types in a consistent order
        type_order = ["New Conversation", "Follow-up Message", "Message Created", "Regenerate Response", "Other"]
        unique_types = [t for t in type_order if t in df[type_col].unique()]
        
        # Add a bar trace for each message type
        for msg_type in unique_types:
            type_df = df[df[type_col] == msg_type]
            color = type_colors.get(msg_type, "#888888")
            
            fig.add_trace(go.Bar(
                x=type_df[time_col],
                y=type_df[count_col],
                name=msg_type,
                marker_color=color,
                hovertemplate=f"{msg_type}<br>%{{x}}<br>Messages: %{{y}}<extra></extra>",
            ))
        
        fig.update_layout(barmode="stack")
    elif chart_type == "bar":
        fig = go.Figure(data=[
            go.Bar(
                x=df[time_col],
                y=df[count_col],
                marker_color=CHART_COLORS["primary"],
                hovertemplate="%{x}<br>Messages: %{y}<extra></extra>",
            )
        ])
    else:
        fig = go.Figure(data=[
            go.Scatter(
                x=df[time_col],
                y=df[count_col],
                mode="lines+markers",
                line={"color": CHART_COLORS["primary"], "width": 2},
                marker={"size": 6},
                fill="tozeroy",
                fillcolor="rgba(124,58,237,0.2)",
                hovertemplate="%{x}<br>Messages: %{y}<extra></extra>",
            )
        ])
    
    fig.update_layout(
        title=title,
        xaxis_title="",
        yaxis_title="Messages",
        height=350,
        hovermode="x unified",
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "center",
            "x": 0.5,
        },
    )
    
    return apply_dark_theme(fig)


def create_latency_percentile_chart(metrics: dict) -> go.Figure:
    """
    Create a horizontal bar chart showing latency percentiles.
    
    Args:
        metrics: Dictionary with p50_sec, p90_sec, p95_sec, p99_sec, avg_duration_sec
        
    Returns:
        Plotly figure
    """
    # Extract latency values
    avg = float(metrics.get("avg_duration_sec", 0) or 0)
    p50 = float(metrics.get("p50_sec", 0) or 0)
    p90 = float(metrics.get("p90_sec", 0) or 0)
    p95 = float(metrics.get("p95_sec", 0) or 0)
    p99 = float(metrics.get("p99_sec", 0) or 0)
    
    # Create data for horizontal bar chart
    percentiles = ["Avg", "P50", "P90", "P95", "P99"]
    values = [avg, p50, p90, p95, p99]
    colors = ["#8B5CF6", "#3B82F6", "#F59E0B", "#EF4444", "#DC2626"]
    
    fig = go.Figure()
    
    for i, (p, v, c) in enumerate(zip(percentiles, values, colors)):
        fig.add_trace(go.Bar(
            y=[p],
            x=[v],
            orientation="h",
            name=p,
            marker_color=c,
            text=f"{v:.1f}s",
            textposition="auto",
            hovertemplate=f"{p}: {v:.2f}s<extra></extra>",
        ))
    
    fig.update_layout(
        title="Latency Percentiles",
        xaxis_title="Duration (seconds)",
        yaxis_title="",
        height=250,
        showlegend=False,
        barmode="group",
    )
    
    return apply_dark_theme(fig)


def create_latency_percentiles_chart(metrics: dict) -> go.Figure:
    """
    Create a horizontal bar chart showing latency percentiles (P50, P90, P95, P99).
    
    Args:
        metrics: Dictionary with percentile metrics (p50_sec, p90_sec, p95_sec, p99_sec)
        
    Returns:
        Plotly figure with horizontal bar chart
    """
    # Extract percentile values
    p50 = float(metrics.get("p50_sec", 0) or 0)
    p90 = float(metrics.get("p90_sec", 0) or 0)
    p95 = float(metrics.get("p95_sec", 0) or 0)
    p99 = float(metrics.get("p99_sec", 0) or 0)
    
    # Data for the chart
    percentiles = ["P99", "P95", "P90", "P50"]  # Reversed for horizontal bar (bottom to top)
    values = [p99, p95, p90, p50]
    
    # Color gradient: green (fast) to red (slow)
    colors = ["#EF4444", "#F59E0B", "#FBBF24", "#22C55E"]
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=percentiles,
        x=values,
        orientation="h",
        marker_color=colors,
        text=[f"{v:.1f}s" for v in values],
        textposition="outside",
        textfont={"color": "#ffffff", "size": 12},
        hovertemplate="%{y}: %{x:.2f}s<extra></extra>",
    ))
    
    # Calculate a reasonable max for x-axis (round up to nice number)
    max_val = max(values) if values else 1
    x_max = max(1, max_val * 1.3)  # Add 30% padding for labels
    
    fig.update_layout(
        title={
            "text": "Latency Percentiles",
            "font": {"size": 14, "color": "#ffffff"},
            "x": 0.5,
            "xanchor": "center",
        },
        height=200,
        showlegend=False,
        xaxis={
            "title": "Duration (seconds)",
            "range": [0, x_max],
            "gridcolor": "rgba(255,255,255,0.1)",
            "tickfont": {"color": "#888888"},
        },
        yaxis={
            "gridcolor": "rgba(255,255,255,0.1)",
            "tickfont": {"color": "#ffffff", "size": 12},
        },
        margin={"l": 50, "r": 60, "t": 40, "b": 40},
    )
    
    return apply_dark_theme(fig)


def create_performance_summary_chart(metrics: dict) -> go.Figure:
    """
    Create a combined performance summary with gauge and indicators.
    
    Args:
        metrics: Dictionary with performance metrics
        
    Returns:
        Plotly figure with subplots
    """
    from plotly.subplots import make_subplots
    
    # Extract metrics
    total_queries = int(float(metrics.get("total_queries", 0) or 0))
    success_rate = float(metrics.get("success_rate_pct", 0) or 0)
    avg_duration = float(metrics.get("avg_duration_sec", 0) or 0)
    p50 = float(metrics.get("p50_sec", 0) or 0)
    p90 = float(metrics.get("p90_sec", 0) or 0)
    p95 = float(metrics.get("p95_sec", 0) or 0)
    slow_queries = int(float(metrics.get("slow_10s", 0) or 0))
    unique_users = int(float(metrics.get("unique_users", 0) or 0))
    
    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "indicator"}, {"type": "bar"}]],
        column_widths=[0.4, 0.6],
    )
    
    # Success rate gauge
    fig.add_trace(
        go.Indicator(
            mode="gauge+number",
            value=success_rate,
            title={"text": "Success Rate", "font": {"size": 14}},
            number={"suffix": "%", "font": {"size": 24}},
            gauge={
                "axis": {"range": [0, 100], "tickwidth": 1},
                "bar": {"color": "#22C55E" if success_rate >= 95 else "#F59E0B" if success_rate >= 80 else "#EF4444"},
                "bgcolor": "rgba(255,255,255,0.1)",
                "borderwidth": 0,
                "steps": [
                    {"range": [0, 80], "color": "rgba(239,68,68,0.2)"},
                    {"range": [80, 95], "color": "rgba(245,158,11,0.2)"},
                    {"range": [95, 100], "color": "rgba(34,197,94,0.2)"},
                ],
                "threshold": {
                    "line": {"color": "white", "width": 2},
                    "thickness": 0.75,
                    "value": success_rate,
                },
            },
        ),
        row=1, col=1
    )
    
    # Latency percentiles bar chart
    percentiles = ["Avg", "P50", "P90", "P95"]
    latency_values = [avg_duration, p50, p90, p95]
    colors = ["#8B5CF6", "#3B82F6", "#F59E0B", "#EF4444"]
    
    fig.add_trace(
        go.Bar(
            x=percentiles,
            y=latency_values,
            marker_color=colors,
            text=[f"{v:.1f}s" for v in latency_values],
            textposition="outside",
            hovertemplate="%{x}: %{y:.2f}s<extra></extra>",
        ),
        row=1, col=2
    )
    
    fig.update_layout(
        height=280,
        showlegend=False,
        margin={"l": 20, "r": 20, "t": 30, "b": 40},
    )
    
    # Update bar chart axis
    fig.update_xaxes(title_text="", row=1, col=2)
    fig.update_yaxes(title_text="Latency (s)", row=1, col=2)
    
    return apply_dark_theme(fig)
