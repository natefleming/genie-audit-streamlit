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
