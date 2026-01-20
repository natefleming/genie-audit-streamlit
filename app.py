"""
Genie Performance Audit - Single Page Application

A Streamlit application for analyzing Databricks Genie query performance,
identifying problematic queries, and providing optimization recommendations.
"""

import streamlit as st
import pandas as pd
from typing import Optional

# Configure page - no sidebar
st.set_page_config(
    page_title="Genie Performance Audit",
    page_icon="🔮",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Import services and components
from services.databricks_client import get_client, DatabricksClient
from services.analytics import classify_bottleneck, get_query_optimizations, get_diagnostic_queries, map_status
from services.report_generator import generate_pdf_report, generate_query_pdf_report
from components.charts import (
    create_duration_distribution_chart,
    create_bottleneck_chart,
    create_daily_trend_chart,
    create_conversation_activity_chart,
    create_response_time_breakdown_chart,
    create_latency_percentiles_chart,
)
from components.metrics import render_metrics_row, METRIC_CARD_CSS
from utils.formatters import (
    format_duration,
    format_number,
    format_percentage,
    get_bottleneck_label,
    get_bottleneck_color,
)
from queries.sql import (
    SPACE_METRICS_QUERY,
    BOTTLENECK_DISTRIBUTION_QUERY,
    PER_REQUEST_BREAKDOWN_QUERY,
    DURATION_HISTOGRAM_QUERY,
    DAILY_TREND_QUERY,
    QUERIES_LIST_QUERY,
    QUERY_CONCURRENCY_QUERY,
    CONVERSATION_ACTIVITY_QUERY,
    CONVERSATION_DAILY_QUERY,
    CONVERSATION_PEAK_QUERY,
    AI_LATENCY_METRICS_QUERY,
    AI_LATENCY_TREND_QUERY,
    build_space_filter,
    build_status_filter,
    build_audit_space_filter,
    build_query_space_filter,
)


# Custom CSS - minimal dark theme
CUSTOM_CSS = """
<style>
.stApp {
    background-color: #0a0a0f;
}

/* Hide sidebar */
section[data-testid="stSidebar"] {
    display: none;
}

/* Header styling */
.main-header {
    background: linear-gradient(135deg, rgba(124,58,237,0.15) 0%, rgba(6,182,212,0.15) 100%);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 16px;
    padding: 20px 28px;
    margin-bottom: 24px;
}
.main-header h1 {
    color: #ffffff;
    font-size: 26px;
    font-weight: 700;
    margin: 0 0 4px 0;
}
.main-header p {
    color: #888888;
    font-size: 14px;
    margin: 0;
}

/* Section headers */
.section-header {
    color: #ffffff;
    font-size: 18px;
    font-weight: 600;
    margin: 20px 0 12px 0;
}

/* Query detail panel */
.query-detail-panel {
    background: rgba(18,18,26,0.9);
    border: 1px solid rgba(124,58,237,0.3);
    border-radius: 12px;
    padding: 20px;
    margin-top: 16px;
}

/* Optimization cards */
.optimization-card {
    background: rgba(18,18,26,0.8);
    border-left: 3px solid;
    border-radius: 8px;
    padding: 12px 16px;
    margin-bottom: 12px;
}
.optimization-card.high {
    border-left-color: #ef4444;
}
.optimization-card.medium {
    border-left-color: #f59e0b;
}
.optimization-card.low {
    border-left-color: #22c55e;
}
.optimization-title {
    color: #ffffff;
    font-weight: 600;
    font-size: 14px;
    margin-bottom: 4px;
}
.optimization-desc {
    color: #888888;
    font-size: 13px;
    margin-bottom: 8px;
}
.optimization-rec {
    color: #06b6d4;
    font-size: 13px;
}

/* Stat grid */
.stat-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 12px;
    margin-bottom: 16px;
}
.stat-item {
    background: rgba(18,18,26,0.6);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 8px;
    padding: 12px;
    text-align: center;
}
.stat-value {
    color: #ffffff;
    font-size: 20px;
    font-weight: 600;
}
.stat-label {
    color: #666666;
    font-size: 12px;
    margin-top: 4px;
}

/* Hide checkbox column in dataframe while keeping row selection */
[data-testid="stDataFrame"] [data-testid="glideDataEditor"] > div:first-child {
    /* Hide the row selection checkbox column */
}
[data-testid="stDataFrame"] th:first-child,
[data-testid="stDataFrame"] td:first-child {
    display: none !important;
}
/* Alternative approach for glide-data-grid */
.dvn-scroller [data-testid="data-grid-canvas"] {
    /* Shift content left to hide checkbox */
}
/* Hide the selection column header and cells */
[data-testid="stDataFrameResizable"] [role="columnheader"]:first-child,
[data-testid="stDataFrameResizable"] [role="gridcell"]:first-child {
    display: none !important;
    width: 0 !important;
    min-width: 0 !important;
    max-width: 0 !important;
}
</style>
"""


def init_session_state() -> None:
    """Initialize session state variables."""
    if "selected_room_id" not in st.session_state:
        st.session_state.selected_room_id = None
    if "selected_query_id" not in st.session_state:
        st.session_state.selected_query_id = None
    if "hours_filter" not in st.session_state:
        st.session_state.hours_filter = 720  # Default 30 days in hours
    if "room_data" not in st.session_state:
        st.session_state.room_data = {}  # Cache for room data
    if "force_refresh" not in st.session_state:
        st.session_state.force_refresh = False
    if "room_pdf_bytes" not in st.session_state:
        st.session_state.room_pdf_bytes = None
    if "room_pdf_filename" not in st.session_state:
        st.session_state.room_pdf_filename = None


def render_header() -> None:
    """Render the main page header."""
    st.markdown("""
    <div class="main-header">
        <h1>🔮 Genie Performance Audit</h1>
        <p>Select a Genie room to analyze query performance and identify optimization opportunities</p>
    </div>
    """, unsafe_allow_html=True)


def load_genie_rooms_with_progress(client: DatabricksClient, status_text, progress_bar=None) -> list:
    """Load all Genie rooms with progress feedback."""
    last_update = [0]  # Track last update time for throttling
    
    def progress_callback(count: int, has_more: bool, total: int = None):
        import time
        current_time = time.time()
        
        # Throttle updates to every 100ms to avoid UI lag
        if has_more and current_time - last_update[0] < 0.1:
            return
        last_update[0] = current_time
        
        if total and total > 0:
            # Show progress with total when known (fallback path)
            pct = min(count / total, 1.0)
            if progress_bar:
                progress_bar.progress(pct)
            status_text.markdown(f"🔮 Loading room details: **{count}** / {total}")
        elif has_more:
            status_text.markdown(f"🔮 Loaded **{count}** Genie rooms... (fetching more)")
        else:
            status_text.markdown(f"🔮 Loaded **{count}** Genie rooms ✓")
            if progress_bar:
                progress_bar.progress(1.0)
    
    spaces = client.list_genie_spaces(progress_callback=progress_callback)
    # Return tuple of (id, name, owner)
    return [(s.id, s.name, s.owner) for s in spaces]


@st.cache_data(ttl=60, show_spinner=False)
def load_genie_rooms_cached(_client: DatabricksClient) -> list:
    """Load all Genie rooms with owner info (cached, no progress)."""
    spaces = _client.list_genie_spaces()
    # Return tuple of (id, name, owner)
    return [(s.id, s.name, s.owner) for s in spaces]


def get_current_user(client: DatabricksClient) -> Optional[str]:
    """Get the current user's email."""
    return client.get_current_user()


@st.cache_data(ttl=60, show_spinner=False)
def load_space_metrics(_client: DatabricksClient, space_id: str, hours: float) -> dict:
    """Load metrics for a specific space."""
    sql = SPACE_METRICS_QUERY.format(space_id=space_id, hours=hours)
    df = _client.execute_sql(sql)
    
    if df.empty:
        return {
            "total_queries": 0,
            "avg_duration_sec": 0,
            "p90_sec": 0,
            "slow_10s": 0,
            "successful_queries": 0,
            "failed_queries": 0,
            "success_rate_pct": 100,
        }
    
    return df.iloc[0].to_dict()


@st.cache_data(ttl=60, show_spinner=False)
def load_bottleneck_data(_client: DatabricksClient, space_id: str, hours: float) -> pd.DataFrame:
    """Load bottleneck distribution data."""
    space_filter = build_space_filter(space_id)
    sql = BOTTLENECK_DISTRIBUTION_QUERY.format(hours=hours, space_filter=space_filter)
    return _client.execute_sql(sql)


@st.cache_data(ttl=60, show_spinner=False)
def load_duration_distribution(_client: DatabricksClient, space_id: str, hours: float) -> pd.DataFrame:
    """Load duration distribution data."""
    space_filter = build_space_filter(space_id)
    sql = DURATION_HISTOGRAM_QUERY.format(hours=hours, space_filter=space_filter)
    return _client.execute_sql(sql)


@st.cache_data(ttl=60, show_spinner=False)
def load_daily_trends(_client: DatabricksClient, space_id: str, hours: float) -> pd.DataFrame:
    """Load daily trend data."""
    space_filter = build_space_filter(space_id)
    sql = DAILY_TREND_QUERY.format(hours=hours, space_filter=space_filter)
    return _client.execute_sql(sql)


@st.cache_data(ttl=60, show_spinner=False)
def load_queries(_client: DatabricksClient, space_id: str, hours: float, limit: int = 100) -> pd.DataFrame:
    """Load query list for a space with AI overhead correlation."""
    space_filter = build_space_filter(space_id)
    audit_space_filter = build_audit_space_filter(space_id)
    sql = QUERIES_LIST_QUERY.format(
        hours=hours,
        space_filter=space_filter,
        audit_space_filter=audit_space_filter,
        status_filter="",
        limit=limit
    )
    return _client.execute_sql(sql)


@st.cache_data(ttl=60, show_spinner=False)
def load_conversation_activity(_client: DatabricksClient, space_id: str, hours: float) -> pd.DataFrame:
    """Load hourly conversation activity from audit logs."""
    space_filter = build_audit_space_filter(space_id)
    sql = CONVERSATION_ACTIVITY_QUERY.format(hours=hours, space_filter=space_filter)
    try:
        return _client.execute_sql(sql)
    except Exception as e:
        print(f"Could not load conversation activity: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def load_conversation_daily(_client: DatabricksClient, space_id: str, hours: float) -> pd.DataFrame:
    """Load daily conversation activity from audit logs."""
    space_filter = build_audit_space_filter(space_id)
    sql = CONVERSATION_DAILY_QUERY.format(hours=hours, space_filter=space_filter)
    try:
        return _client.execute_sql(sql)
    except Exception as e:
        print(f"Could not load daily conversation data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def load_conversation_peak(_client: DatabricksClient, space_id: str, hours: float) -> dict:
    """Load peak conversation metrics from audit logs."""
    space_filter = build_audit_space_filter(space_id)
    sql = CONVERSATION_PEAK_QUERY.format(hours=hours, space_filter=space_filter)
    try:
        df = _client.execute_sql(sql)
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception as e:
        print(f"Could not load conversation peak data: {e}")
        return {}


def load_ai_latency_metrics(_client: DatabricksClient, space_id: str, hours: float) -> dict:
    """
    Load AI latency metrics by correlating message events with query execution.
    
    This estimates GenAI processing time (question understanding + SQL generation)
    by measuring the time between when a user sends a message and when the first
    SQL query starts executing.
    """
    space_filter = build_audit_space_filter(space_id)
    query_space_filter = build_query_space_filter(space_id)
    sql = AI_LATENCY_METRICS_QUERY.format(
        hours=hours,
        space_filter=space_filter,
        query_space_filter=query_space_filter
    )
    try:
        df = _client.execute_sql(sql)
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception as e:
        print(f"Could not load AI latency metrics: {e}")
        return {}


def load_ai_latency_trend(_client: DatabricksClient, space_id: str, hours: float) -> pd.DataFrame:
    """
    Load AI latency trend over time.
    
    Returns daily average AI latency for trending charts.
    """
    space_filter = build_audit_space_filter(space_id)
    query_space_filter = build_query_space_filter(space_id)
    sql = AI_LATENCY_TREND_QUERY.format(
        hours=hours,
        space_filter=space_filter,
        query_space_filter=query_space_filter
    )
    try:
        return _client.execute_sql(sql)
    except Exception as e:
        print(f"Could not load AI latency trend: {e}")
        return pd.DataFrame()


def load_query_concurrency(_client: DatabricksClient, query: dict) -> tuple[int, int]:
    """
    Load concurrency metrics for a specific query.
    
    Returns (genie_concurrent, warehouse_concurrent) - the number of concurrent
    queries running at the time this query was submitted.
    """
    statement_id = query.get("statement_id", "")
    genie_space_id = query.get("genie_space_id", "")
    warehouse_id = query.get("warehouse_id", "")
    start_time = query.get("start_time", "")
    
    if not all([statement_id, genie_space_id, start_time]):
        return (0, 0)
    
    # Format start_time for SQL
    if hasattr(start_time, 'strftime'):
        start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    else:
        start_time_str = str(start_time)
    
    sql = QUERY_CONCURRENCY_QUERY.format(
        statement_id=statement_id,
        genie_space_id=genie_space_id,
        warehouse_id=warehouse_id or "",
        start_time=start_time_str,
    )
    
    try:
        df = _client.execute_sql(sql)
        if df.empty:
            return (0, 0)
        row = df.iloc[0]
        genie_conc = int(float(row.get("genie_concurrent", 0) or 0))
        wh_conc = int(float(row.get("warehouse_concurrent", 0) or 0))
        return (genie_conc, wh_conc)
    except Exception as e:
        print(f"Could not load query concurrency: {e}")
        return (0, 0)


def load_phase_breakdown(_client: DatabricksClient, space_id: str, hours: float) -> pd.DataFrame:
    """
    Load per-request response time breakdown by correlating message events with SQL queries.
    
    This properly calculates AI overhead as the time from message start to first SQL query,
    giving an accurate picture of where response time is spent:
    - AI Overhead: Time from user message to SQL execution (GenAI processing)
    - Queue Wait: Time waiting in queue
    - Compute Startup: Time waiting for compute resources
    - Compilation: SQL compilation time
    - Execution: Query execution time
    """
    space_filter = build_space_filter(space_id)
    audit_space_filter = build_audit_space_filter(space_id)
    
    sql = PER_REQUEST_BREAKDOWN_QUERY.format(
        hours=hours,
        space_filter=space_filter,
        audit_space_filter=audit_space_filter
    )
    
    try:
        df = _client.execute_sql(sql)
    except Exception as e:
        print(f"Could not load phase breakdown: {e}")
        return pd.DataFrame()
    
    if df.empty:
        return pd.DataFrame()
    
    # Convert to proper types
    result_df = pd.DataFrame({
        "phase": df["phase"],
        "phase_order": df["phase_order"].astype(int),
        "time_min": df["time_min"].astype(float),
        "avg_sec": df["avg_sec"].astype(float)
    })
    
    # Calculate percentages
    total_time = result_df["time_min"].sum()
    if total_time > 0:
        result_df["pct"] = (result_df["time_min"] / total_time * 100).round(1)
    else:
        result_df["pct"] = 0.0
    
    return result_df.sort_values("phase_order")


def build_query_phase_breakdown(query: dict) -> pd.DataFrame:
    """
    Build a phase breakdown DataFrame from a single query's timing data.
    
    Args:
        query: Dictionary containing query timing fields (ai_overhead_sec, compile_sec, etc.)
        
    Returns:
        DataFrame with phase breakdown for the specific query
    """
    # Extract timing values, converting to float and handling strings
    ai_overhead = float(query.get("ai_overhead_sec", 0) or 0)
    queue_wait = float(query.get("queue_sec", 0) or 0)
    compute_startup = float(query.get("wait_compute_sec", 0) or 0)
    compilation = float(query.get("compile_sec", 0) or 0)
    execution = float(query.get("execute_sec", 0) or 0)
    
    # Build phases list (times in seconds, convert to minutes for consistency)
    phases = [
        {"phase": "AI Overhead", "phase_order": 0, "time_min": ai_overhead / 60.0, "avg_sec": ai_overhead},
        {"phase": "Queue Wait", "phase_order": 1, "time_min": queue_wait / 60.0, "avg_sec": queue_wait},
        {"phase": "Compute Startup", "phase_order": 2, "time_min": compute_startup / 60.0, "avg_sec": compute_startup},
        {"phase": "Compilation", "phase_order": 3, "time_min": compilation / 60.0, "avg_sec": compilation},
        {"phase": "Execution", "phase_order": 4, "time_min": execution / 60.0, "avg_sec": execution},
    ]
    
    result_df = pd.DataFrame(phases)
    
    # Calculate percentages
    total_time = result_df["time_min"].sum()
    if total_time > 0:
        result_df["pct"] = (result_df["time_min"] / total_time * 100).round(1)
    else:
        result_df["pct"] = 0.0
    
    return result_df.sort_values("phase_order")


def render_room_selector(client: DatabricksClient) -> Optional[str]:
    """Render room selection dropdown and return selected room ID."""
    # Check if rooms are already loaded in session state
    if "genie_rooms" not in st.session_state or st.session_state.get("rooms_need_refresh"):
        # Use a placeholder for the loading state with progress
        room_placeholder = st.empty()
        
        with room_placeholder.container():
            status_text = st.empty()
            progress_bar = st.progress(0)
            status_text.markdown("🔮 Discovering Genie rooms...")
            
            try:
                rooms = load_genie_rooms_with_progress(client, status_text, progress_bar)
                current_user = get_current_user(client)
                
                # Store in session state
                st.session_state["genie_rooms"] = rooms
                st.session_state["current_user"] = current_user
                st.session_state["rooms_need_refresh"] = False
            except Exception as e:
                st.error(f"Failed to load Genie rooms: {e}")
                return None
        
        # Clear the loading placeholder
        room_placeholder.empty()
    else:
        # Use cached rooms from session state
        rooms = st.session_state["genie_rooms"]
        current_user = st.session_state.get("current_user")
    
    if not rooms:
        st.warning("No Genie rooms found. Make sure there are queries from Genie rooms in the last 90 days.")
        return None
    
    # Sort rooms: user's rooms first (marked with ⭐), then alphabetically
    my_rooms = []
    other_rooms = []
    
    def format_display_name(name: str, is_mine: bool) -> str:
        """Format display name (clean, without ID)."""
        prefix = "⭐ " if is_mine else ""
        return f"{prefix}{name}"
    
    for room_id, name, owner in rooms:
        is_mine = False
        if current_user and owner:
            # Check if owner matches current user (case-insensitive)
            is_mine = owner.lower() == current_user.lower()
        
        display_name = format_display_name(name, is_mine)
        
        if is_mine:
            my_rooms.append((room_id, display_name, name))  # (id, display_name, sort_key)
        else:
            other_rooms.append((room_id, display_name, name))
    
    # Sort each group alphabetically by name
    my_rooms.sort(key=lambda x: x[2].lower())
    other_rooms.sort(key=lambda x: x[2].lower())
    
    # Combine: my rooms first, then others
    sorted_rooms = my_rooms + other_rooms
    
    # Search input for filtering by name or space ID
    search_term = st.text_input(
        "Search rooms",
        placeholder="Search by name or space ID...",
        label_visibility="collapsed",
        key="room_search"
    )
    
    # Filter rooms based on search term (matches name OR ID)
    if search_term:
        search_lower = search_term.lower().strip()
        filtered_rooms = [
            (room_id, display_name, name) 
            for room_id, display_name, name in sorted_rooms
            if search_lower in name.lower() or search_lower in room_id.lower()
        ]
    else:
        filtered_rooms = sorted_rooms
    
    # Create options with display name and ID
    room_options = {display_name: room_id for room_id, display_name, _ in filtered_rooms}
    room_names = ["Select a Genie Room..."] + [display_name for _, display_name, _ in filtered_rooms]
    
    # Show count with breakdown
    count_msg = f"📋 {len(rooms)} Genie rooms"
    if my_rooms:
        count_msg += f" ({len(my_rooms)} owned by you)"
    if search_term and len(filtered_rooms) != len(sorted_rooms):
        count_msg += f" • Showing {len(filtered_rooms)} matching '{search_term}'"
    st.caption(count_msg)
    
    selected_name = st.selectbox(
        "Genie Room",
        options=room_names,
        index=0,
        label_visibility="collapsed",
        key="genie_room_selector",
    )
    
    if selected_name == "Select a Genie Room...":
        return None
    
    # Store room name for report generation
    st.session_state["selected_room_name"] = selected_name
    
    return room_options[selected_name]


# Time period options: (hours, display_label)
TIME_PERIODS = [
    (0.25, "Last 15 minutes"),
    (1, "Last 1 hour"),
    (12, "Last 12 hours"),
    (24, "Last 1 day"),
    (168, "Last 7 days"),
    (336, "Last 14 days"),
    (720, "Last 30 days"),
    (1440, "Last 60 days"),
    (2160, "Last 90 days"),
]


def render_filters(client: DatabricksClient, pdf_bytes: bytes = None, pdf_filename: str = None) -> float:
    """Render time range filter, refresh button, and optional download button. Returns hours."""
    # Use 3 columns if PDF is provided, otherwise 2
    if pdf_bytes:
        col1, col2, col3 = st.columns([3, 1, 1])
    else:
        col1, col2 = st.columns([4, 1])
        col3 = None
    
    # Build options as hours values with display labels
    hours_options = [h for h, _ in TIME_PERIODS]
    labels = {h: label for h, label in TIME_PERIODS}
    
    with col1:
        hours = st.selectbox(
            "Time Range",
            options=hours_options,
            index=6,  # Default to "Last 30 days" (720 hours)
            format_func=lambda x: labels[x],
            label_visibility="collapsed",
            key="time_range_selector",
        )
        st.session_state.hours_filter = hours
    
    with col2:
        if st.button("🔄 Refresh", width="stretch", key="refresh_button"):
            client.clear_cache()
            st.cache_data.clear()
            # Clear cached room data and PDF, trigger reload
            st.session_state["room_data"] = {}
            st.session_state["room_pdf_bytes"] = None
            st.session_state["room_pdf_filename"] = None
            st.session_state["force_refresh"] = True
            st.session_state["rooms_need_refresh"] = True
            st.rerun()
    
    if col3 and pdf_bytes:
        with col3:
            st.download_button(
                label="📄 Room Report",
                data=pdf_bytes,
                file_name=pdf_filename or "genie_room_report.pdf",
                mime="application/pdf",
                width="stretch",
                key="room_report_download",
            )
    
    return hours


def render_room_metrics(metrics: dict) -> None:
    """Render room metrics cards."""
    # Convert to numeric types (SQL may return strings)
    total = int(float(metrics.get("total_queries", 0) or 0))
    successful = int(float(metrics.get("successful_queries", 0) or 0))
    failed = int(float(metrics.get("failed_queries", 0) or 0))
    avg_duration = float(metrics.get("avg_duration_sec", 0) or 0)
    p90 = float(metrics.get("p90_sec", 0) or 0)
    slow_10s = int(float(metrics.get("slow_10s", 0) or 0))
    unique_users = int(float(metrics.get("unique_users", 0) or 0))
    
    success_rate = (successful / total * 100) if total > 0 else 100
    
    metrics_data = [
        {
            "title": "Total Queries",
            "value": format_number(total),
            "icon": "📊",
            "color": "violet",
        },
        {
            "title": "Avg Duration",
            "value": f"{avg_duration:.1f}s",
            "subtitle": f"P90: {p90:.1f}s",
            "icon": "⏱️",
            "color": "cyan",
        },
        {
            "title": "Slow Queries",
            "value": format_number(slow_10s),
            "subtitle": ">10 seconds",
            "icon": "⚠️",
            "color": "orange",
        },
        {
            "title": "Success Rate",
            "value": format_percentage(success_rate),
            "subtitle": f"{failed} failed",
            "icon": "✅",
            "color": "green",
        },
        {
            "title": "Unique Users",
            "value": format_number(unique_users),
            "icon": "👥",
            "color": "blue",
        },
    ]
    
    render_metrics_row(metrics_data)


def render_overview_charts(daily_df: pd.DataFrame, duration_df: pd.DataFrame, metrics: dict = None) -> None:
    """Render overview charts: Daily Query Volume, Query Duration Distribution, and Latency Percentiles."""
    col1, col2 = st.columns(2)
    
    with col1:
        if not daily_df.empty:
            fig = create_daily_trend_chart(daily_df, "total_queries", "Daily Query Volume", "line")
            st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
    
    with col2:
        if not duration_df.empty:
            fig = create_duration_distribution_chart(duration_df)
            st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
    
    # Latency Percentiles chart in a new row
    if metrics:
        st.markdown("") # Small spacer
        col3, col4 = st.columns(2)
        with col3:
            fig = create_latency_percentiles_chart(metrics)
            st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


def render_phase_breakdown(
    phase_df: pd.DataFrame, 
    is_query_selected: bool = False,
    query_data: dict = None,
    room_name: str = "",
    room_id: str = "",
    genie_concurrent: int = 0,
    warehouse_concurrent: int = 0,
    user_prompt: Optional[str] = None,
) -> None:
    """Render Response Time Breakdown chart with optional export button for selected query."""
    if phase_df.empty:
        return
    
    if is_query_selected:
        # Header with export button
        col_header, col_export = st.columns([4, 1])
        with col_header:
            st.markdown("### ⏱️ Response Time Breakdown (Selected Query)")
        with col_export:
            if query_data:
                try:
                    from datetime import datetime
                    query_pdf = generate_query_pdf_report(
                        query=query_data,
                        room_name=room_name,
                        room_id=room_id,
                        phase_df=phase_df,
                        genie_concurrent=genie_concurrent,
                        warehouse_concurrent=warehouse_concurrent,
                        user_prompt=user_prompt,
                    )
                    statement_id = query_data.get("statement_id", "query")[:8]
                    filename = f"query_{statement_id}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
                    st.download_button(
                        label="📄 Export Query",
                        data=query_pdf,
                        file_name=filename,
                        mime="application/pdf",
                        width="stretch",
                    )
                except Exception as e:
                    st.error(f"Export failed: {str(e)}")
        st.caption("Time breakdown for the selected query")
    else:
        st.markdown("### ⏱️ Response Time Breakdown (Room Aggregate)")
        st.caption("Where time is spent from question to answer across all queries")
    
    # Use seconds for individual queries, minutes for room aggregate
    fig = create_response_time_breakdown_chart(phase_df, use_seconds=is_query_selected)
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
    
    # Show summary stats - use seconds for individual query, minutes for room aggregate
    col1, col2, col3 = st.columns(3)
    
    if is_query_selected:
        # For individual query, show in seconds
        total_sec = phase_df["avg_sec"].sum()
        ai_sec = phase_df[phase_df["phase"] == "AI Overhead"]["avg_sec"].sum()
        sql_sec = total_sec - ai_sec
        
        with col1:
            st.metric("Total Time", f"{total_sec:.1f}s")
        with col2:
            ai_pct = (ai_sec / total_sec * 100) if total_sec > 0 else 0
            st.metric("AI Overhead", f"{ai_sec:.1f}s", f"{ai_pct:.0f}%")
        with col3:
            sql_pct = (sql_sec / total_sec * 100) if total_sec > 0 else 0
            st.metric("SQL Execution", f"{sql_sec:.1f}s", f"{sql_pct:.0f}%")
    else:
        # For room aggregate, show in minutes
        total_time = phase_df["time_min"].sum()
        ai_time = phase_df[phase_df["phase"] == "AI Overhead"]["time_min"].sum()
        sql_time = total_time - ai_time
        
        with col1:
            st.metric("Total Time", f"{total_time:.1f} min")
        with col2:
            ai_pct = (ai_time / total_time * 100) if total_time > 0 else 0
            st.metric("AI Overhead", f"{ai_time:.1f} min", f"{ai_pct:.0f}%")
        with col3:
            sql_pct = (sql_time / total_time * 100) if total_time > 0 else 0
            st.metric("SQL Execution", f"{sql_time:.1f} min", f"{sql_pct:.0f}%")


def render_query_list(queries_df: pd.DataFrame) -> Optional[str]:
    """Render query list and return selected query ID."""
    st.markdown('<div class="section-header">🔍 Queries</div>', unsafe_allow_html=True)
    
    if queries_df.empty:
        st.info("No queries found for this room in the selected time range.")
        return None
    
    # Search filter
    search = st.text_input("Search queries", placeholder="Filter by query text, user, or prompt...", label_visibility="collapsed")
    
    # Ensure user_prompt column exists
    if "user_prompt" not in queries_df.columns:
        queries_df["user_prompt"] = ""
    
    if search:
        mask = (
            queries_df["query_text"].str.lower().str.contains(search.lower(), na=False) |
            queries_df["executed_by"].str.lower().str.contains(search.lower(), na=False) |
            queries_df["user_prompt"].fillna("").str.lower().str.contains(search.lower(), na=False)
        )
        queries_df = queries_df[mask]
    
    # Count how many have prompts
    prompts_found = queries_df["user_prompt"].fillna("").str.len().gt(0).sum()
    st.caption(f"{len(queries_df)} queries (sorted by duration, slowest first) • {prompts_found} prompts resolved")
    
    # Prepare display dataframe
    display_df = queries_df.copy()
    # Convert to numeric (SQL may return strings)
    ai_overhead_numeric = pd.to_numeric(display_df.get("ai_overhead_sec", 0), errors='coerce').fillna(0)
    sql_duration_numeric = pd.to_numeric(display_df["total_sec"], errors='coerce').fillna(0)
    total_time_numeric = ai_overhead_numeric + sql_duration_numeric
    
    display_df["AI Overhead"] = ai_overhead_numeric.apply(lambda x: f"{x:.1f}s" if x > 0 else "-")
    display_df["SQL Duration"] = sql_duration_numeric.apply(lambda x: f"{x:.1f}s")
    display_df["Total Time"] = total_time_numeric.apply(lambda x: f"{x:.1f}s")
    display_df["Compile"] = pd.to_numeric(display_df["compile_sec"], errors='coerce').fillna(0).apply(lambda x: f"{x:.1f}s")
    display_df["Execute"] = pd.to_numeric(display_df["execute_sec"], errors='coerce').fillna(0).apply(lambda x: f"{x:.1f}s")
    display_df["Queue"] = pd.to_numeric(display_df["queue_sec"], errors='coerce').fillna(0).apply(lambda x: f"{x:.1f}s")
    display_df["Query Preview"] = display_df["query_text"].str[:100] + "..."
    display_df["User"] = display_df["executed_by"].str.split("@").str[0]
    display_df["Time"] = pd.to_datetime(display_df["start_time"]).dt.strftime("%b %d %H:%M")
    display_df["Status"] = display_df["execution_status"].apply(map_status)
    display_df["Bottleneck"] = display_df["bottleneck"].apply(get_bottleneck_label)
    display_df["Speed"] = display_df["speed_category"]
    # User Prompt - truncated to 50 chars, show "—" if not available
    display_df["Question"] = display_df["user_prompt"].fillna("").apply(
        lambda x: (x[:50] + "..." if len(x) > 50 else x) if x else "—"
    )
    
    columns_to_show = [
        "Question",
        "Query Preview",
        "User",
        "Time",
        "Total Time",
        "AI Overhead",
        "SQL Duration",
        "Compile",
        "Execute",
        "Queue",
        "Bottleneck",
    ]
    
    # Dataframe with selection
    selection = st.dataframe(
        display_df[columns_to_show],
        width="stretch",
        hide_index=True,
        height=350,
        selection_mode="single-row",
        on_select="rerun",
    )
    
    if selection and selection.selection.rows:
        selected_idx = selection.selection.rows[0]
        return queries_df.iloc[selected_idx]["statement_id"]
    
    return None


def render_query_detail(
    query_id: str, 
    queries_df: pd.DataFrame,
    genie_concurrent: int = 0,
    warehouse_concurrent: int = 0,
) -> None:
    """Render detailed view for a selected query with concurrency metrics."""
    query_row = queries_df[queries_df["statement_id"] == query_id]
    
    if query_row.empty:
        return
    
    query = query_row.iloc[0].to_dict()
    
    st.markdown("---")
    st.markdown('<div class="section-header">📋 Query Details</div>', unsafe_allow_html=True)
    
    # Display identifiers
    statement_id = query.get("statement_id", "")
    api_request_id = query.get("api_request_id", "") or "N/A"
    conversation_id = query.get("conversation_id", "") or "N/A"
    genie_space_id = query.get("genie_space_id", "")
    
    st.markdown(f"""
    <div style="background: rgba(18,18,26,0.8); border-radius: 8px; padding: 12px 16px; margin-bottom: 16px; display: flex; flex-wrap: wrap; gap: 24px;">
        <div>
            <span style="color: #888; font-size: 11px; text-transform: uppercase;">Statement ID</span><br/>
            <code style="color: #06b6d4; font-size: 12px;">{statement_id}</code>
        </div>
        <div>
            <span style="color: #888; font-size: 11px; text-transform: uppercase;">API Request ID</span><br/>
            <code style="color: #a78bfa; font-size: 12px;">{api_request_id}</code>
        </div>
        <div>
            <span style="color: #888; font-size: 11px; text-transform: uppercase;">Conversation ID</span><br/>
            <code style="color: #22c55e; font-size: 12px;">{conversation_id}</code>
        </div>
        <div>
            <span style="color: #888; font-size: 11px; text-transform: uppercase;">Genie Space ID</span><br/>
            <code style="color: #f59e0b; font-size: 12px;">{genie_space_id}</code>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Display the user's original question/prompt (pre-loaded during data fetch)
    user_prompt = query.get("user_prompt", "") or ""
    if user_prompt:
        st.markdown("**💬 User Question**")
        st.info(user_prompt)
    else:
        st.caption("💬 User question not available (could not correlate with Genie conversation)")
    
    # Convert to numeric (SQL may return strings)
    total_sec = float(query.get('total_sec', 0) or 0)
    compile_sec = float(query.get('compile_sec', 0) or 0)
    execute_sec = float(query.get('execute_sec', 0) or 0)
    queue_sec = float(query.get('queue_sec', 0) or 0)
    read_rows = int(float(query.get('read_rows', 0) or 0))
    read_mb = float(query.get('read_mb', 0) or 0)
    
    # Bottleneck
    bottleneck = query.get("bottleneck", "NORMAL")
    bottleneck_label = get_bottleneck_label(bottleneck)
    bottleneck_color = get_bottleneck_color(bottleneck)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Query SQL with increased height
        st.markdown("**SQL Query**")
        query_text = query.get("query_text", "") or ""
        # Escape HTML and format SQL
        escaped_sql = query_text[:2000].replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        # Use a scrollable container for the SQL with syntax highlighting colors
        st.markdown(f"""
        <div style="background: #1e1e2e; border-radius: 8px; padding: 16px; font-family: 'Consolas', 'Monaco', monospace; 
                    font-size: 13px; line-height: 1.5; color: #e0e0e0; overflow-x: auto; white-space: pre-wrap; 
                    min-height: 180px; max-height: 300px; overflow-y: auto; border: 1px solid rgba(255,255,255,0.1);">{escaped_sql}</div>
        """, unsafe_allow_html=True)
    
    with col2:
        # Stats - aligned with SQL Query header
        st.markdown("**Query Metrics**")
        st.markdown(f"""
        <div class="stat-grid" style="margin-top: 8px;">
            <div class="stat-item">
                <div class="stat-value">{total_sec:.1f}s</div>
                <div class="stat-label">Total Duration</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{compile_sec:.1f}s</div>
                <div class="stat-label">Compile</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{execute_sec:.1f}s</div>
                <div class="stat-label">Execute</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{queue_sec:.1f}s</div>
                <div class="stat-label">Queue Wait</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{format_number(read_rows)}</div>
                <div class="stat-label">Rows Scanned</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{read_mb:.1f} MB</div>
                <div class="stat-label">Data Read</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{genie_concurrent}</div>
                <div class="stat-label">Genie Concurrency</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{warehouse_concurrent}</div>
                <div class="stat-label">Warehouse Concurrency</div>
            </div>
        </div>
        <div style="background: rgba(18,18,26,0.8); border-radius: 8px; padding: 12px; margin-top: 12px;">
            <div style="color: #888; font-size: 12px; margin-bottom: 4px;">Primary Bottleneck</div>
            <div style="color: {bottleneck_color}; font-size: 16px; font-weight: 600;">{bottleneck_label}</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Optimizations
    st.markdown("### 💡 Recommendations")
    
    # Convert to numeric (SQL may return strings)
    optimizations = get_query_optimizations({
        "total_duration_ms": int(float(query.get("total_duration_ms", 0) or 0)),
        "compilation_ms": int(float(query.get("compilation_ms", 0) or 0)),
        "execution_ms": int(float(query.get("execution_ms", 0) or 0)),
        "queue_wait_ms": int(float(query.get("queue_wait_ms", 0) or 0)),
        "compute_wait_ms": int(float(query.get("compute_wait_ms", 0) or 0)),
        "bytes_scanned": int(float(query.get("bytes_scanned", 0) or 0)),
        "rows_scanned": int(float(query.get("read_rows", 0) or 0)),
        "rows_returned": int(float(query.get("produced_rows", 0) or 0)),
        "ai_overhead_sec": float(query.get("ai_overhead_sec", 0) or 0),
        "bottleneck": bottleneck,
    })
    
    # Severity color mapping
    severity_colors = {
        "high": "#ff6b6b",
        "medium": "#ffa94d",
        "low": "#51cf66",
    }
    
    for opt in optimizations:
        color = severity_colors.get(opt.severity, "#888")
        severity_badge = opt.severity.upper()
        
        with st.expander(f"{opt.title}", expanded=(opt.severity == "high")):
            st.markdown(f"""
            <span style="background: {color}; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600;">{severity_badge}</span>
            <span style="color: #888; font-size: 12px; margin-left: 8px;">{opt.category.replace('_', ' ').title()}</span>
            """, unsafe_allow_html=True)
            st.markdown(f"*{opt.description}*")
            st.markdown("---")
            st.markdown(opt.recommendation)
    
    # ==========================================================================
    # Diagnostic Queries Section
    # ==========================================================================
    st.markdown("### 🔬 Diagnostic Queries")
    st.caption("Copy and paste these SQL queries into Databricks SQL Editor to investigate further")
    
    diagnostic_queries = get_diagnostic_queries({
        "statement_id": query.get("statement_id", ""),
        "genie_space_id": query.get("genie_space_id", ""),
        "bottleneck": bottleneck,
        "query_text": query.get("query_text", ""),
    })
    
    # Category icons
    category_icons = {
        "monitoring": "📊",
        "performance": "⚡",
        "statistics": "📈",
        "data": "🗃️",
    }
    
    for diag in diagnostic_queries:
        icon = category_icons.get(diag.category, "📋")
        with st.expander(f"{icon} {diag.title}"):
            st.caption(diag.description)
            st.code(diag.sql, language="sql")


def main() -> None:
    """Main application entry point."""
    # Inject CSS
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
    st.markdown(METRIC_CARD_CSS, unsafe_allow_html=True)
    
    # Initialize state
    init_session_state()
    
    # Header
    render_header()
    
    try:
        client = get_client()
        
        # Room selector
        room_id = render_room_selector(client)
        
        if not room_id:
            st.info("👆 Select a Genie room above to view performance metrics and queries.")
            return
        
        st.markdown("---")
        
        # Check for cached PDF from previous run
        cached_pdf = st.session_state.get("room_pdf_bytes")
        cached_pdf_filename = st.session_state.get("room_pdf_filename")
        
        # Time filter, refresh, and download button (if PDF is cached from previous load)
        hours = render_filters(client, pdf_bytes=cached_pdf, pdf_filename=cached_pdf_filename)
        
        # Cache key for this room/time combination
        cache_key = f"{room_id}_{hours}"
        
        # Check if we need to load data (not cached or force refresh)
        need_load = (
            cache_key not in st.session_state.get("room_data", {}) or 
            st.session_state.get("force_refresh", False)
        )
        
        if need_load:
            # Load data with progress feedback
            progress_placeholder = st.empty()
            
            with progress_placeholder.container():
                st.markdown("##### 📊 Analyzing Genie Room Performance")
                st.caption("Querying system.query.history and audit logs for insights...")
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                status_text.markdown("**Step 1/6:** Calculating aggregate metrics (query count, avg duration, success rate)...")
                metrics = load_space_metrics(client, room_id, hours)
                progress_bar.progress(15)
                
                status_text.markdown("**Step 2/6:** Loading daily query volume and latency trends...")
                daily_df = load_daily_trends(client, room_id, hours)
                progress_bar.progress(30)
                
                status_text.markdown("**Step 3/6:** Analyzing query duration distribution (< 1s, 1-5s, 5-10s, etc.)...")
                duration_df = load_duration_distribution(client, room_id, hours)
                progress_bar.progress(45)
                
                status_text.markdown("**Step 4/6:** Analyzing response time breakdown (AI processing + SQL execution phases)...")
                phase_df = load_phase_breakdown(client, room_id, hours)
                progress_bar.progress(60)
                
                status_text.markdown("**Step 5/7:** Fetching individual queries with timing breakdown...")
                queries_df = load_queries(client, room_id, hours)
                progress_bar.progress(65)
                
                status_text.markdown("**Step 6/7:** Resolving user prompts via Genie Conversations API...")
                # Populate prompts for all queries using reverse lookup
                if not queries_df.empty:
                    prompts_dict = client.get_prompts_for_queries(room_id, queries_df)
                    queries_df["user_prompt"] = queries_df["statement_id"].map(
                        lambda sid: prompts_dict.get(sid, "")
                    )
                else:
                    queries_df["user_prompt"] = ""
                progress_bar.progress(85)
                
                status_text.markdown("**Step 7/7:** Loading AI conversation activity from audit logs...")
                conversation_daily_df = load_conversation_daily(client, room_id, hours)
                conversation_peak = load_conversation_peak(client, room_id, hours)
                progress_bar.progress(100)
                
                status_text.markdown("✅ **Analysis complete!**")
            
            # Brief pause to show completion, then clear
            import time
            time.sleep(0.3)
            progress_placeholder.empty()
            
            # Cache the data in session state
            st.session_state["room_data"][cache_key] = {
                "metrics": metrics,
                "daily_df": daily_df,
                "duration_df": duration_df,
                "phase_df": phase_df,
                "queries_df": queries_df,
                "conversation_daily_df": conversation_daily_df,
                "conversation_peak": conversation_peak,
            }
            
            # Reset force refresh flag
            st.session_state["force_refresh"] = False
        else:
            # Use cached data
            cached = st.session_state["room_data"][cache_key]
            metrics = cached["metrics"]
            daily_df = cached["daily_df"]
            duration_df = cached["duration_df"]
            phase_df = cached["phase_df"]
            queries_df = cached["queries_df"]
            conversation_daily_df = cached["conversation_daily_df"]
            conversation_peak = cached["conversation_peak"]
        
        # Generate room report PDF and update filter row with download button
        room_name = st.session_state.get("selected_room_name", room_id)
        try:
            from datetime import datetime
            room_pdf_bytes = generate_pdf_report(
                room_name=room_name,
                room_id=room_id,
                hours=hours,
                metrics=metrics,
                queries_df=queries_df,
                phase_df=phase_df,
                conversation_peak=conversation_peak,
            )
            room_pdf_filename = f"genie_room_{room_id[:8]}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
            
            # Cache PDF in session state for display in filter row
            st.session_state["room_pdf_bytes"] = room_pdf_bytes
            st.session_state["room_pdf_filename"] = room_pdf_filename
        except Exception as e:
            st.error(f"Report generation failed: {str(e)}")
            st.session_state["room_pdf_bytes"] = None
            st.session_state["room_pdf_filename"] = None
        
        # Room metrics
        render_room_metrics(metrics)
        
        st.markdown("---")
        
        # Overview charts: Daily Query Volume, Duration Distribution, and Latency Percentiles
        render_overview_charts(daily_df, duration_df, metrics)
        
        st.markdown("---")
        
        # Room Aggregate Response Time Breakdown - always shown above queries
        render_phase_breakdown(phase_df, is_query_selected=False)
        
        # AI Conversation Activity Section (room-level aggregate)
        if not conversation_daily_df.empty:
            st.markdown("---")
            st.markdown("### 💬 AI Conversation Activity")
            
            # Peak metrics
            if conversation_peak:
                peak_col1, peak_col2, peak_col3 = st.columns(3)
                with peak_col1:
                    total_msgs = int(float(conversation_peak.get("total_messages", 0) or 0))
                    st.metric("Total Messages", format_number(total_msgs))
                with peak_col2:
                    peak_per_min = int(float(conversation_peak.get("peak_messages_per_minute", 0) or 0))
                    st.metric("Peak per Minute", peak_per_min)
                with peak_col3:
                    avg_per_min = float(conversation_peak.get("avg_messages_per_minute", 0) or 0)
                    st.metric("Avg per Active Min", f"{avg_per_min:.1f}")
            
            # Daily conversation activity chart (stacked by message type)
            fig = create_conversation_activity_chart(
                conversation_daily_df,
                time_col="event_date",
                count_col="message_count",
                type_col="message_type",
                title="Daily Conversation Messages by Type",
                chart_type="stacked_bar"
            )
            st.plotly_chart(fig, width="stretch")
        
        st.markdown("---")
        
        # Query list with selection
        selected_query = render_query_list(queries_df)
        
        # If a query is selected, show its specific breakdown below
        if selected_query:
            query_row = queries_df[queries_df["statement_id"] == selected_query]
            if not query_row.empty:
                query_dict = query_row.iloc[0].to_dict()
                query_phase_df = build_query_phase_breakdown(query_dict)
                
                # Load concurrency metrics for this specific query
                genie_conc, wh_conc = load_query_concurrency(client, query_dict)
                
                # Get pre-loaded user prompt (populated during data load)
                user_prompt = query_dict.get("user_prompt", "") or None
                
                st.markdown("---")
                
                # Query-specific Response Time Breakdown with export button
                render_phase_breakdown(
                    query_phase_df, 
                    is_query_selected=True,
                    query_data=query_dict,
                    room_name=room_name,
                    room_id=room_id,
                    genie_concurrent=genie_conc,
                    warehouse_concurrent=wh_conc,
                    user_prompt=user_prompt,
                )
                
                # Query detail with concurrency
                render_query_detail(selected_query, queries_df, genie_conc, wh_conc)
        
    except Exception as e:
        st.error(f"Failed to connect to Databricks: {str(e)}")
        st.info("Make sure you have configured your Databricks credentials correctly.")
        
        with st.expander("Debug Information"):
            st.code(str(e))


if __name__ == "__main__":
    main()
