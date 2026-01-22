"""
Metric Card Components

Streamlit components for displaying metric cards with custom styling.
"""

import streamlit as st
from typing import Optional


# Metric card CSS styles
METRIC_CARD_CSS = """
<style>
.metric-card {
    background: linear-gradient(135deg, rgba(18,18,26,0.9) 0%, rgba(26,26,38,0.9) 100%);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 10px;
    transition: all 0.3s ease;
    min-height: 140px;
    display: flex;
    flex-direction: column;
}
.metric-card:hover {
    border-color: rgba(124,58,237,0.5);
    box-shadow: 0 4px 20px rgba(124,58,237,0.1);
}
.metric-card-icon {
    width: 48px;
    height: 48px;
    border-radius: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    margin-bottom: 12px;
    font-size: 24px;
}
.metric-card-icon.violet { background: rgba(124,58,237,0.2); }
.metric-card-icon.cyan { background: rgba(6,182,212,0.2); }
.metric-card-icon.orange { background: rgba(249,115,22,0.2); }
.metric-card-icon.green { background: rgba(16,185,129,0.2); }
.metric-card-icon.red { background: rgba(239,68,68,0.2); }
.metric-card-title {
    color: #888888;
    font-size: 14px;
    margin-bottom: 4px;
}
.metric-card-value {
    color: #ffffff;
    font-size: 28px;
    font-weight: 600;
    font-family: 'Monaco', 'Menlo', monospace;
}
.metric-card-subtitle {
    color: #666666;
    font-size: 12px;
    margin-top: 4px;
}
</style>
"""

# Icons for different metric types
ICONS = {
    "queries": "📊",
    "duration": "⏱️",
    "slow": "⚠️",
    "success": "✅",
    "failed": "❌",
    "users": "👥",
    "data": "💾",
}


def render_metric_card(
    title: str,
    value: str,
    subtitle: Optional[str] = None,
    icon: str = "📊",
    color: str = "violet",
) -> None:
    """
    Render a metric card with custom styling.
    
    Args:
        title: Card title (e.g., "Total Queries")
        value: Main value to display
        subtitle: Optional subtitle text
        icon: Emoji or icon character
        color: Color theme (violet, cyan, orange, green, red)
    """
    subtitle_html = f'<div class="metric-card-subtitle">{subtitle}</div>' if subtitle else ""
    
    # Note: METRIC_CARD_CSS should be injected once at app startup, not here
    html = f"""
    <div class="metric-card">
        <div class="metric-card-icon {color}">{icon}</div>
        <div class="metric-card-title">{title}</div>
        <div class="metric-card-value">{value}</div>
        {subtitle_html}
    </div>
    """
    
    st.markdown(html, unsafe_allow_html=True)


def render_metrics_row(metrics: list[dict]) -> None:
    """
    Render a row of metric cards using Streamlit columns.
    
    Args:
        metrics: List of metric dicts with keys: title, value, subtitle, icon, color
    """
    cols = st.columns(len(metrics))
    
    for col, metric in zip(cols, metrics):
        with col:
            render_metric_card(
                title=metric.get("title", ""),
                value=metric.get("value", ""),
                subtitle=metric.get("subtitle"),
                icon=metric.get("icon", "📊"),
                color=metric.get("color", "violet"),
            )


def render_stat_box(label: str, value: str, color: str = "#ffffff") -> None:
    """
    Render a simple stat box for detail pages.
    
    Args:
        label: Stat label
        value: Stat value
        color: Value text color
    """
    st.markdown(f"""
    <div style="
        background: rgba(18,18,26,0.5);
        border-radius: 8px;
        padding: 12px;
        text-align: center;
    ">
        <div style="color: #888888; font-size: 12px; margin-bottom: 4px;">{label}</div>
        <div style="color: {color}; font-size: 18px; font-weight: 600; font-family: monospace;">{value}</div>
    </div>
    """, unsafe_allow_html=True)


def render_badge(text: str, color: str = "#7c3aed") -> str:
    """
    Generate HTML for a badge.
    
    Args:
        text: Badge text
        color: Background color
        
    Returns:
        HTML string for the badge
    """
    return f"""
    <span style="
        background: {color}33;
        color: {color};
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: 500;
    ">{text}</span>
    """


def render_status_badge(status: str) -> str:
    """
    Generate HTML for a status badge.
    
    Args:
        status: Status string (success, failed, cancelled)
        
    Returns:
        HTML string for the badge
    """
    colors = {
        "success": "#10b981",
        "FINISHED": "#10b981",
        "failed": "#ef4444",
        "FAILED": "#ef4444",
        "cancelled": "#f59e0b",
        "CANCELED": "#f59e0b",
        "CANCELLED": "#f59e0b",
    }
    color = colors.get(status, "#6b7280")
    display_text = status.lower().replace("finished", "success").replace("canceled", "cancelled")
    
    return render_badge(display_text.title(), color)


# ============================================================================
# CONVERSATION-LEVEL METRICS
# ============================================================================

CONVERSATION_METRICS_CSS = """
<style>
.conv-metrics-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
    gap: 12px;
    margin-bottom: 16px;
}
.conv-metric-box {
    background: rgba(18,18,26,0.6);
    border-radius: 8px;
    padding: 12px;
    text-align: center;
}
.conv-metric-value {
    color: #06b6d4;
    font-size: 20px;
    font-weight: 600;
    font-family: monospace;
}
.conv-metric-label {
    color: #888888;
    font-size: 11px;
    margin-top: 4px;
}
.msg-metrics-inline {
    display: flex;
    gap: 16px;
    align-items: center;
    color: #888888;
    font-size: 12px;
}
.msg-metric-item {
    display: flex;
    align-items: center;
    gap: 4px;
}
.msg-metric-value {
    color: #ffffff;
    font-weight: 500;
}
</style>
"""


def render_conversation_metrics(
    total_queries: int,
    avg_duration_ms: float,
    slowest_query_ms: int,
    success_rate: float,
    created_time: str = "",
) -> None:
    """
    Render aggregated metrics for a conversation.
    
    Args:
        total_queries: Total number of SQL queries in this conversation
        avg_duration_ms: Average query duration in milliseconds
        slowest_query_ms: Slowest query duration in milliseconds
        success_rate: Success rate as percentage (0-100)
        created_time: Optional creation timestamp
    """
    st.markdown(CONVERSATION_METRICS_CSS, unsafe_allow_html=True)
    
    avg_sec = avg_duration_ms / 1000.0 if avg_duration_ms else 0
    slowest_sec = slowest_query_ms / 1000.0 if slowest_query_ms else 0
    
    metrics_html = f"""
    <div class="conv-metrics-grid">
        <div class="conv-metric-box">
            <div class="conv-metric-value">{total_queries}</div>
            <div class="conv-metric-label">Queries</div>
        </div>
        <div class="conv-metric-box">
            <div class="conv-metric-value">{avg_sec:.1f}s</div>
            <div class="conv-metric-label">Avg Duration</div>
        </div>
        <div class="conv-metric-box">
            <div class="conv-metric-value">{slowest_sec:.1f}s</div>
            <div class="conv-metric-label">Slowest</div>
        </div>
        <div class="conv-metric-box">
            <div class="conv-metric-value">{success_rate:.0f}%</div>
            <div class="conv-metric-label">Success Rate</div>
        </div>
    </div>
    """
    
    st.markdown(metrics_html, unsafe_allow_html=True)


def render_message_metrics_inline(
    query_count: int,
    total_duration_ms: int,
) -> str:
    """
    Generate inline HTML for message-level metrics.
    
    Args:
        query_count: Number of queries for this message
        total_duration_ms: Total duration of all queries in milliseconds
        
    Returns:
        HTML string for inline metrics display
    """
    total_sec = total_duration_ms / 1000.0 if total_duration_ms else 0
    query_label = "query" if query_count == 1 else "queries"
    
    return f"""
    <div class="msg-metrics-inline">
        <div class="msg-metric-item">
            🔍 <span class="msg-metric-value">{query_count}</span> {query_label}
        </div>
        <div class="msg-metric-item">
            ⏱️ <span class="msg-metric-value">{total_sec:.1f}s</span> total
        </div>
    </div>
    """


def render_query_metrics_row(
    duration_ms: int,
    compilation_ms: int,
    execution_ms: int,
    queue_wait_ms: int,
    bottleneck: str,
    speed_category: str,
) -> str:
    """
    Generate HTML for a compact query metrics row.
    
    Args:
        duration_ms: Total query duration
        compilation_ms: Compilation time
        execution_ms: Execution time
        queue_wait_ms: Queue wait time
        bottleneck: Bottleneck type
        speed_category: Speed category (FAST, MODERATE, SLOW, CRITICAL)
        
    Returns:
        HTML string for the metrics row
    """
    duration_sec = duration_ms / 1000.0 if duration_ms else 0
    compile_sec = compilation_ms / 1000.0 if compilation_ms else 0
    exec_sec = execution_ms / 1000.0 if execution_ms else 0
    queue_sec = queue_wait_ms / 1000.0 if queue_wait_ms else 0
    
    # Speed category colors
    speed_colors = {
        "FAST": "#22c55e",
        "MODERATE": "#f59e0b",
        "SLOW": "#ef4444",
        "CRITICAL": "#ef4444",
    }
    speed_color = speed_colors.get(speed_category.upper(), "#888888")
    
    # Bottleneck colors
    bottleneck_colors = {
        "NORMAL": "#22c55e",
        "QUEUE_WAIT": "#f59e0b",
        "COMPUTE_STARTUP": "#ef4444",
        "COMPILATION": "#a855f7",
        "SLOW_EXECUTION": "#ef4444",
        "LARGE_SCAN": "#3b82f6",
    }
    bottleneck_color = bottleneck_colors.get(bottleneck.upper(), "#888888")
    bottleneck_display = bottleneck.replace("_", " ").title()
    
    return f"""
    <div style="display:flex;gap:16px;align-items:center;font-size:12px;">
        <span style="color:{speed_color};font-weight:600;">{duration_sec:.1f}s</span>
        <span style="color:#888;">Compile: {compile_sec:.1f}s</span>
        <span style="color:#888;">Execute: {exec_sec:.1f}s</span>
        <span style="color:#888;">Queue: {queue_sec:.1f}s</span>
        <span style="background:{bottleneck_color}20;color:{bottleneck_color};padding:2px 8px;border-radius:4px;font-size:10px;">{bottleneck_display}</span>
    </div>
    """


def render_conversations_summary_metrics(
    total_conversations: int,
    total_messages: int,
    total_queries: int,
    avg_queries_per_conversation: float,
    overall_avg_duration_ms: float,
    overall_success_rate: float,
) -> None:
    """
    Render summary metrics for all conversations in a space.
    
    Args:
        total_conversations: Total number of conversations
        total_messages: Total number of messages across conversations
        total_queries: Total number of SQL queries
        avg_queries_per_conversation: Average queries per conversation
        overall_avg_duration_ms: Average query duration across all conversations
        overall_success_rate: Overall success rate percentage
    """
    avg_sec = overall_avg_duration_ms / 1000.0 if overall_avg_duration_ms else 0
    
    metrics = [
        {
            "title": "Conversations",
            "value": str(total_conversations),
            "icon": "💬",
            "color": "violet",
        },
        {
            "title": "Messages",
            "value": str(total_messages),
            "icon": "📝",
            "color": "cyan",
        },
        {
            "title": "SQL Queries",
            "value": str(total_queries),
            "icon": "🔍",
            "color": "green",
        },
        {
            "title": "Avg Queries/Conv",
            "value": f"{avg_queries_per_conversation:.1f}",
            "icon": "📊",
            "color": "orange",
        },
        {
            "title": "Avg Duration",
            "value": f"{avg_sec:.1f}s",
            "icon": "⏱️",
            "color": "cyan",
        },
    ]
    
    render_metrics_row(metrics)
