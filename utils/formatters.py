"""
Formatting Utilities

Helper functions for formatting durations, numbers, bytes, and other values
for display in the Genie Audit dashboard.
"""

from datetime import datetime
from typing import Union


def format_duration(ms: Union[int, float, None]) -> str:
    """
    Format a duration in milliseconds to a human-readable string.
    
    Args:
        ms: Duration in milliseconds
        
    Returns:
        Formatted string like "1.5s", "2m 30s", "1h 5m"
    """
    if ms is None or ms == 0:
        return "0s"
    
    ms = float(ms)
    
    if ms < 1000:
        return f"{int(ms)}ms"
    
    seconds = ms / 1000
    
    if seconds < 60:
        return f"{seconds:.1f}s"
    
    minutes = seconds / 60
    
    if minutes < 60:
        mins = int(minutes)
        secs = int(seconds % 60)
        if secs > 0:
            return f"{mins}m {secs}s"
        return f"{mins}m"
    
    hours = minutes / 60
    mins = int(minutes % 60)
    
    if mins > 0:
        return f"{hours:.0f}h {mins}m"
    return f"{hours:.1f}h"


def format_number(value: Union[int, float, None, str]) -> str:
    """
    Format a number with thousands separators.
    
    Args:
        value: Number to format (can be int, float, string, or None)
        
    Returns:
        Formatted string like "1,234" or "1.2M"
    """
    if value is None:
        return "0"
    
    try:
        value = float(value)
    except (ValueError, TypeError):
        return "0"
    
    if abs(value) >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.1f}K"
    
    if value == int(value):
        return f"{int(value):,}"
    return f"{value:,.2f}"


def format_bytes(bytes_val: Union[int, float, None]) -> str:
    """
    Format bytes to a human-readable size string.
    
    Args:
        bytes_val: Number of bytes
        
    Returns:
        Formatted string like "1.5 GB", "256 MB"
    """
    if bytes_val is None or bytes_val == 0:
        return "0 B"
    
    bytes_val = float(bytes_val)
    
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if abs(bytes_val) < 1024:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024
    
    return f"{bytes_val:.1f} PB"


def format_percentage(value: Union[int, float, None], decimals: int = 1) -> str:
    """
    Format a percentage value.
    
    Args:
        value: Percentage value (0-100)
        decimals: Number of decimal places
        
    Returns:
        Formatted string like "95.5%"
    """
    if value is None:
        return "0%"
    
    return f"{float(value):.{decimals}f}%"


def format_datetime(dt: Union[str, datetime, None]) -> str:
    """
    Format a datetime value to a readable string.
    
    Args:
        dt: Datetime string or object
        
    Returns:
        Formatted string like "Jan 15, 2024 3:45 PM"
    """
    if dt is None:
        return ""
    
    if isinstance(dt, str):
        try:
            # Handle ISO format
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except ValueError:
            return dt
    
    return dt.strftime("%b %d, %Y %I:%M %p")


def format_date(dt: Union[str, datetime, None]) -> str:
    """
    Format a datetime to just the date.
    
    Args:
        dt: Datetime string or object
        
    Returns:
        Formatted string like "Jan 15, 2024"
    """
    if dt is None:
        return ""
    
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except ValueError:
            return dt
    
    return dt.strftime("%b %d, %Y")


# Bottleneck type mappings
BOTTLENECK_LABELS = {
    "NORMAL": "Normal",
    "normal": "Normal",
    "QUEUE_WAIT": "Queue Wait",
    "queue_wait": "Queue Wait",
    "COMPUTE_STARTUP": "Compute Startup",
    "compute_startup": "Compute Startup",
    "COMPILATION": "Compilation",
    "compilation": "Compilation",
    "SLOW_EXECUTION": "Slow Execution",
    "slow_execution": "Slow Execution",
    "LARGE_SCAN": "Large Scan",
    "large_scan": "Large Scan",
    "OTHER": "Other",
}

BOTTLENECK_COLORS = {
    "NORMAL": "#10b981",  # Green
    "normal": "#10b981",
    "Normal": "#10b981",
    "QUEUE_WAIT": "#8dd1e1",  # Cyan
    "queue_wait": "#8dd1e1",
    "Queue Wait": "#8dd1e1",
    "COMPUTE_STARTUP": "#f59e0b",  # Orange
    "compute_startup": "#f59e0b",
    "Compute Startup": "#f59e0b",
    "COMPILATION": "#0088aa",  # Blue
    "compilation": "#0088aa",
    "Compilation": "#0088aa",
    "SLOW_EXECUTION": "#a855f7",  # Purple
    "slow_execution": "#a855f7",
    "Slow Execution": "#a855f7",
    "LARGE_SCAN": "#22c55e",  # Light green
    "large_scan": "#22c55e",
    "Large Scan": "#22c55e",
    "OTHER": "#6b7280",  # Gray
}

STATUS_COLORS = {
    "success": "#10b981",
    "FINISHED": "#10b981",
    "failed": "#ef4444",
    "FAILED": "#ef4444",
    "cancelled": "#f59e0b",
    "CANCELED": "#f59e0b",
    "CANCELLED": "#f59e0b",
}

SPEED_COLORS = {
    "FAST": "#10b981",
    "MODERATE": "#f59e0b",
    "SLOW": "#f97316",
    "CRITICAL": "#ef4444",
}


def get_bottleneck_label(bottleneck_type: str | None) -> str:
    """
    Get human-readable label for a bottleneck type.
    
    Args:
        bottleneck_type: Bottleneck type string
        
    Returns:
        Human-readable label
    """
    if not bottleneck_type:
        return "Normal"
    return BOTTLENECK_LABELS.get(bottleneck_type, bottleneck_type.replace("_", " ").title())


def get_bottleneck_color(bottleneck_type: str | None) -> str:
    """
    Get color for a bottleneck type.
    
    Args:
        bottleneck_type: Bottleneck type string
        
    Returns:
        Hex color code
    """
    if not bottleneck_type:
        return "#10b981"
    return BOTTLENECK_COLORS.get(bottleneck_type, "#6b7280")


def get_status_color(status: str | None) -> str:
    """
    Get color for an execution status.
    
    Args:
        status: Status string
        
    Returns:
        Hex color code
    """
    if not status:
        return "#6b7280"
    return STATUS_COLORS.get(status, "#6b7280")


def get_speed_color(speed_category: str | None) -> str:
    """
    Get color for a speed category.
    
    Args:
        speed_category: Speed category (FAST, MODERATE, SLOW, CRITICAL)
        
    Returns:
        Hex color code
    """
    if not speed_category:
        return "#10b981"
    return SPEED_COLORS.get(speed_category, "#10b981")


# Chart color palette
CHART_COLORS = {
    "primary": "#7c3aed",  # Void purple
    "secondary": "#06b6d4",  # Electric cyan
    "accent": "#f97316",  # Orange
    "success": "#10b981",  # Green
    "warning": "#f59e0b",  # Amber
    "error": "#ef4444",  # Red
    "info": "#0088aa",  # Blue
    "muted": "#6b7280",  # Gray
}

DURATION_BUCKET_COLORS = {
    "< 1s": "#0088aa",
    "1-5s": "#22c55e",
    "5-10s": "#a855f7",
    "10-30s": "#f97316",
    "30-60s": "#8dd1e1",
    "> 60s": "#ef4444",
}

PHASE_COLORS = {
    "AI Overhead": "#a855f7",  # Purple - GenAI processing overhead
    "AI Processing": "#a855f7",  # Purple (alias)
    "Queue Wait": "#8dd1e1",  # Cyan
    "Compute Startup": "#f59e0b",  # Orange
    "Compilation": "#0088aa",  # Blue
    "Execution": "#22c55e",  # Green
    "Result Fetch": "#ef4444",  # Red
    "Wait for Compute": "#f59e0b",  # Orange (alias)
}
