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
from services.databricks_client import (
    get_client, 
    DatabricksClient, 
    ConversationWithMessages, 
    MessageWithQueries, 
    QueryMetrics,
)
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
        st.warning("No Genie rooms found via API. You can manually enter a space ID below.")
    
    # Allow manual space ID entry (useful when room isn't visible in list)
    with st.expander("📝 Enter Space ID manually", expanded=not rooms):
        manual_space_id = st.text_input(
            "Space ID",
            placeholder="e.g., 01f0c482e842191587af6a40ad4044d8",
            help="Enter a Genie space ID directly if the room isn't showing in the list above",
            key="manual_space_id"
        )
        if manual_space_id and manual_space_id.strip():
            # Validate it looks like a space ID (hex string)
            clean_id = manual_space_id.strip()
            if len(clean_id) >= 16:
                st.success(f"Using manual space ID: {clean_id[:12]}...")
                st.session_state["selected_room_name"] = f"Manual: {clean_id[:12]}..."
                return clean_id
            else:
                st.error("Space ID should be at least 16 characters (hex string)")
    
    if not rooms:
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
    
    # Create options with display name and ID
    room_options = {display_name: room_id for room_id, display_name, _ in sorted_rooms}
    room_names = ["Select a Genie Room..."] + [display_name for _, display_name, _ in sorted_rooms]
    
    # Show count with breakdown and refresh button
    count_col, refresh_col = st.columns([4, 1])
    with count_col:
        count_msg = f"📋 {len(rooms)} Genie rooms"
        if my_rooms:
            count_msg += f" ({len(my_rooms)} owned by you)"
        st.caption(count_msg)
    with refresh_col:
        if st.button("🔄", help="Refresh room list from API", key="refresh_rooms"):
            st.session_state["rooms_need_refresh"] = True
            st.rerun()
    
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


# ============================================================================
# CONVERSATION TREE VIEW - Hierarchical view of Conversations → Messages → Queries
# ============================================================================

CONVERSATION_TREE_CSS = """
<style>
.conversation-card {
    background: rgba(18,18,26,0.8);
    border: 1px solid rgba(124,58,237,0.3);
    border-radius: 12px;
    padding: 16px;
    margin-bottom: 12px;
}
.conversation-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 8px;
}
.conversation-title {
    color: #ffffff;
    font-size: 15px;
    font-weight: 600;
}
.conversation-meta {
    color: #888888;
    font-size: 12px;
}
.conversation-metrics {
    display: flex;
    gap: 16px;
    margin-top: 8px;
}
.conv-metric {
    text-align: center;
}
.conv-metric-value {
    color: #06b6d4;
    font-size: 16px;
    font-weight: 600;
}
.conv-metric-label {
    color: #888888;
    font-size: 11px;
}
.message-card {
    background: rgba(30,30,40,0.6);
    border-left: 3px solid #7c3aed;
    border-radius: 0 8px 8px 0;
    padding: 12px 16px;
    margin: 8px 0 8px 16px;
}
.message-prompt {
    color: #ffffff;
    font-size: 14px;
    margin-bottom: 6px;
}
.message-meta {
    color: #888888;
    font-size: 11px;
}
.query-row {
    background: rgba(40,40,50,0.5);
    border-radius: 6px;
    padding: 10px 14px;
    margin: 6px 0 6px 32px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}
.query-row:hover {
    background: rgba(50,50,60,0.7);
}
.query-preview {
    color: #a3e635;
    font-family: monospace;
    font-size: 12px;
    max-width: 400px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.query-metrics-row {
    display: flex;
    gap: 12px;
    align-items: center;
}
.query-metric {
    color: #888888;
    font-size: 11px;
}
.query-metric-value {
    color: #ffffff;
    font-weight: 500;
}
.bottleneck-badge {
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
}
.bottleneck-normal { background: rgba(34,197,94,0.2); color: #22c55e; }
.bottleneck-queue_wait { background: rgba(245,158,11,0.2); color: #f59e0b; }
.bottleneck-compute_startup { background: rgba(239,68,68,0.2); color: #ef4444; }
.bottleneck-compilation { background: rgba(168,85,247,0.2); color: #a855f7; }
.bottleneck-slow_execution { background: rgba(239,68,68,0.2); color: #ef4444; }
.bottleneck-large_scan { background: rgba(59,130,246,0.2); color: #3b82f6; }
.speed-fast { color: #22c55e; }
.speed-moderate { color: #f59e0b; }
.speed-slow { color: #ef4444; }
.speed-critical { color: #ef4444; font-weight: bold; }
</style>
"""


def render_conversations_table(
    conversations: list[ConversationWithMessages],
) -> tuple[list[str], str, bool]:
    """
    Render a sortable table of conversations with key metrics and row selection.
    
    Columns:
    - Conversation ID
    - Conversation (title)
    - Queries (count)
    - Start Time
    - Avg Response (seconds)
    - Max Response (seconds)
    - Issues (count of slow AI + slow queries)
    
    Includes search functionality to filter by conversation ID.
    Returns tuple of (selected_ids, sort_by, ascending) for synchronizing with tree view.
    """
    if not conversations:
        return [], "Start Time", False
    
    # Build DataFrame from conversations
    data = []
    for conv in conversations:
        # Truncate long titles
        title = conv.title if conv.title else f"Conversation {conv.conversation_id[:8]}..."
        if len(title) > 50:
            title = title[:50] + "..."
        
        # Parse and format start time (handle epoch timestamps)
        start_time_display = ""
        if conv.created_time:
            try:
                # Try to parse as numeric epoch (milliseconds or seconds)
                ts_value = conv.created_time
                if ts_value.isdigit() or (ts_value.replace(".", "", 1).isdigit()):
                    ts_num = float(ts_value)
                    # If > 1e12, it's in milliseconds; convert to seconds
                    if ts_num > 1e12:
                        ts_num = ts_num / 1000
                    dt = pd.to_datetime(ts_num, unit="s")
                else:
                    # Try standard datetime parsing
                    dt = pd.to_datetime(ts_value)
                start_time_display = dt.strftime("%b %d, %Y %I:%M %p")
            except Exception:
                start_time_display = str(conv.created_time)[:16]
        
        # Count total issues
        issue_count = conv.slow_ai_count + conv.slow_query_count
        
        data.append({
            "Conversation ID": conv.conversation_id,
            "Conversation": title,
            "User": conv.user_email or "Unknown",
            "Queries": conv.total_queries,
            "Start Time": start_time_display,
            "AI (s)": round(conv.total_ai_overhead_sec, 2),
            "Avg (s)": round(conv.avg_response_sec, 2),
            "Max (s)": round(conv.slowest_response_sec, 2),
            "Issues": issue_count,
            "Source": conv.conversation_source,
        })
    
    df = pd.DataFrame(data)
    
    # Display section header
    st.markdown("### Conversations Summary")
    
    # Search input for filtering by conversation ID
    search_col1, search_col2 = st.columns([3, 1])
    with search_col1:
        search_query = st.text_input(
            "Search by Conversation ID",
            placeholder="Enter conversation ID to filter...",
            key="conv_id_search",
            label_visibility="collapsed"
        )
    with search_col2:
        st.caption(f"{len(conversations)} conversations")
    
    # Filter DataFrame if search query provided
    filtered_df = df
    if search_query:
        # Case-insensitive partial match on Conversation ID
        mask = df["Conversation ID"].str.lower().str.contains(search_query.lower(), na=False)
        filtered_df = df[mask]
        if len(filtered_df) == 0:
            st.warning(f"No conversations found matching '{search_query}'")
        else:
            st.caption(f"Showing {len(filtered_df)} of {len(df)} conversations matching '{search_query}'")
    
    # Sort controls
    sort_col1, sort_col2 = st.columns([2, 1])
    with sort_col1:
        sort_options = ["Start Time", "AI (s)", "Avg (s)", "Max (s)", "Issues", "Queries"]
        sort_by = st.selectbox(
            "Sort by",
            options=sort_options,
            index=0,
            key="conv_sort_by",
            label_visibility="collapsed"
        )
    with sort_col2:
        ascending = st.checkbox("Ascending", value=False, key="conv_sort_asc")
    
    # Apply sorting to filtered DataFrame
    if sort_by and len(filtered_df) > 0:
        filtered_df = filtered_df.sort_values(by=sort_by, ascending=ascending)
    
    st.caption("Select rows to filter the conversation details below")
    
    # Display sortable table with row selection enabled
    event = st.dataframe(
        filtered_df,
        hide_index=True,
        use_container_width=True,
        on_select="rerun",
        selection_mode="multi-row",
        key="conv_table_selection",
        column_config={
            "Conversation ID": st.column_config.TextColumn(
                "Conversation ID",
                width="medium",
                help="Unique identifier for the conversation"
            ),
            "Conversation": st.column_config.TextColumn(
                "Conversation",
                width="large",
                help="Conversation title or first message"
            ),
            "User": st.column_config.TextColumn(
                "User",
                width="medium",
                help="User who started the conversation"
            ),
            "Queries": st.column_config.NumberColumn(
                "Queries",
                format="%d",
                help="Number of SQL queries executed"
            ),
            "Start Time": st.column_config.TextColumn(
                "Start Time",
                width="medium",
                help="When the conversation started"
            ),
            "AI (s)": st.column_config.NumberColumn(
                "AI Overhead",
                format="%.2f",
                help="Total Genie AI model inference time in seconds"
            ),
            "Avg (s)": st.column_config.NumberColumn(
                "Avg Response",
                format="%.2f",
                help="Average response time (AI + SQL) in seconds"
            ),
            "Max (s)": st.column_config.NumberColumn(
                "Max Response",
                format="%.2f",
                help="Slowest response time in seconds"
            ),
            "Issues": st.column_config.NumberColumn(
                "Issues",
                format="%d",
                help="Count of performance issues (slow AI > 10s, slow SQL > 10s)"
            ),
            "Source": st.column_config.TextColumn(
                "Source",
                width="small",
                help="API or Space UI"
            ),
        },
        height=min(400, 50 + len(filtered_df) * 35),  # Dynamic height based on filtered rows
    )
    
    # Extract selected conversation IDs from the event
    selected_ids: list[str] = []
    if event and event.selection and event.selection.rows:
        selected_rows = event.selection.rows
        selected_ids = filtered_df.iloc[selected_rows]["Conversation ID"].tolist()
        st.caption(f"{len(selected_ids)} conversation(s) selected")
    
    st.markdown("---")
    
    return selected_ids, sort_by, ascending


def render_conversation_tree(
    conversations: list[ConversationWithMessages],
    selected_ids: list[str] | None = None,
    room_name: str = "",
    room_id: str = "",
    sort_by: str = "Start Time",
    sort_ascending: bool = False,
    on_query_select: Optional[callable] = None,
) -> Optional[str]:
    """
    Render an expandable tree view of conversations with messages and queries.
    
    Structure:
    ▼ Conversation: "Show sales trends" (3 queries, avg 2.5s)
      ├─ 💬 Message: "What were total sales?"
      │   ├─ 🔍 Query 1: SELECT... (1.2s, Normal)
      │   └─ 🔍 Query 2: SELECT... (3.1s, Large Scan)
      └─ 💬 Message: "Break down by product"
          └─ 🔍 Query 3: SELECT... (2.8s, Normal)
    
    Args:
        conversations: List of ConversationWithMessages with full hierarchy
        selected_ids: Optional list of conversation IDs to filter (show only these)
        room_name: Genie room display name (for PDF reports)
        room_id: Genie space ID (for PDF reports and profile URLs)
        sort_by: Column to sort by (matches summary table options)
        sort_ascending: Sort order (True=ascending, False=descending)
        on_query_select: Optional callback when a query is selected
        
    Returns:
        Selected statement_id if a query is clicked, None otherwise
    """
    st.markdown(CONVERSATION_TREE_CSS, unsafe_allow_html=True)
    st.markdown('<div class="section-header">💬 Conversations</div>', unsafe_allow_html=True)
    
    # Filter conversations if selected_ids is provided and non-empty
    display_conversations = conversations
    if selected_ids:
        display_conversations = [c for c in conversations if c.conversation_id in selected_ids]
        st.caption(f"Showing {len(display_conversations)} selected conversation(s)")
    
    if not display_conversations:
        st.warning("""
        **No conversations found for this Genie space.**
        
        Possible reasons:
        - No one has started a conversation in this Genie space yet
        - All conversations in this space have no messages with content
        - The Genie API returned an empty response (check server logs for debug info)
        
        **Troubleshooting:**
        1. Open the Genie space directly in Databricks and start a conversation
        2. Ask a question that generates a SQL query
        3. Click the 🔄 Refresh button above to reload data
        """)
        return None
    
    # Summary stats with source breakdown and performance issues
    total_convs = len(display_conversations)
    total_msgs = sum(len(c.messages) for c in display_conversations)
    total_queries = sum(c.total_queries for c in display_conversations)
    
    # Performance issue counts
    convs_with_issues = sum(1 for c in display_conversations if c.has_performance_issues)
    total_slow_ai = sum(c.slow_ai_count for c in display_conversations)
    total_slow_queries = sum(c.slow_query_count for c in display_conversations)
    
    # Count by source (API vs Space UI)
    api_convs = sum(1 for c in display_conversations if c.conversation_source == "API")
    space_convs = sum(1 for c in display_conversations if c.conversation_source == "Space")
    unknown_convs = total_convs - api_convs - space_convs
    
    source_breakdown = []
    if api_convs:
        source_breakdown.append(f"🔌 {api_convs} API")
    if space_convs:
        source_breakdown.append(f"🖥️ {space_convs} Space")
    if unknown_convs:
        source_breakdown.append(f"❓ {unknown_convs} Unknown")
    
    source_str = " | ".join(source_breakdown) if source_breakdown else ""
    
    # Main summary
    st.caption(f"{total_convs} conversations • {total_msgs} messages • {total_queries} SQL queries")
    if source_str:
        st.caption(f"Source: {source_str}")
    
    selected_statement_id = None
    
    # Sort conversations using the same order as the summary table
    sort_key_map = {
        "Start Time": lambda c: c.created_time or "",
        "AI (s)": lambda c: c.total_ai_overhead_sec,
        "Avg (s)": lambda c: c.avg_response_sec,
        "Max (s)": lambda c: c.slowest_response_sec,
        "Issues": lambda c: c.slow_ai_count + c.slow_query_count,
        "Queries": lambda c: c.total_queries,
    }
    
    sort_key = sort_key_map.get(sort_by, lambda c: c.created_time or "")
    sorted_conversations = sorted(
        display_conversations,
        key=sort_key,
        reverse=not sort_ascending
    )
    
    # Render each conversation as an expander
    for conv_idx, conv in enumerate(sorted_conversations):
        # Build conversation header with metrics
        conv_title = conv.title if conv.title else f"Conversation {conv.conversation_id[:8]}..."
        if len(conv_title) > 60:
            conv_title = conv_title[:60] + "..."
        
        avg_duration = conv.avg_duration_ms / 1000.0 if conv.avg_duration_ms else 0
        slowest = conv.slowest_query_ms / 1000.0 if conv.slowest_query_ms else 0
        
        # Determine if conversation has issues using computed flags
        has_failed = conv.success_rate < 100 if conv.success_rate else False
        
        icon = "⚠️" if conv.has_performance_issues or has_failed else "💬"
        
        # Show avg response time (AI + SQL combined) in expander
        expander_label = f"{icon} {conv_title} ({conv.total_queries} queries, avg {conv.avg_response_sec:.1f}s response)"
        
        with st.expander(expander_label, expanded=False):
            # Source indicator badge (API vs Space UI from audit logs)
            source = conv.conversation_source
            if source == "API":
                source_badge = "🔌 API"
                source_help = "Initiated via Genie API (genieStartConversationMessage or genieCreateConversationMessage)"
            elif source == "Space":
                source_badge = "🖥️ Space"
                source_help = "Initiated via Genie Space UI (createConversationMessage)"
            else:
                source_badge = "❓ Unknown"
                source_help = "Source could not be determined from audit logs"
            
            st.markdown(f"""
            <div style="margin-bottom: 8px;">
                <span style="background: {'#2563eb' if source == 'API' else '#7c3aed' if source == 'Space' else '#6b7280'}; 
                            color: white; padding: 2px 8px; border-radius: 4px; font-size: 12px; 
                            font-weight: 500;" title="{source_help}">
                    {source_badge}
                </span>
                <span style="color: #888; font-size: 12px; margin-left: 8px;">
                    Conversation ID: {conv.conversation_id[:12]}...
                </span>
            </div>
            """, unsafe_allow_html=True)
            
            # Conversation metrics row with AI overhead
            met_cols = st.columns(6)
            with met_cols[0]:
                st.metric("Queries", conv.total_queries)
            with met_cols[1]:
                st.metric("Avg Response", f"{conv.avg_response_sec:.1f}s")
            with met_cols[2]:
                st.metric("AI Overhead", f"{conv.total_ai_overhead_sec:.1f}s")
            with met_cols[3]:
                st.metric("Slowest", f"{conv.slowest_response_sec:.1f}s")
            with met_cols[4]:
                st.metric("Success Rate", f"{conv.success_rate:.0f}%")
            with met_cols[5]:
                # Show issue count if any
                issue_count = conv.slow_ai_count + conv.slow_query_count
                if issue_count > 0:
                    st.metric("Issues", f"⚠️ {issue_count}")
                else:
                    st.metric("Issues", "✓ None")
            
            # Render each message
            for msg_idx, msg in enumerate(conv.messages):
                if not msg.content and not msg.queries:
                    continue
                
                # Message container with AI overhead and performance indicators
                msg_prompt = msg.content if msg.content else "(No prompt captured)"
                if len(msg_prompt) > 150:
                    msg_prompt = msg_prompt[:150] + "..."
                
                query_count_label = f"{msg.query_count} {'query' if msg.query_count == 1 else 'queries'}"
                
                # Build performance indicators
                ai_label = f"AI: {msg.ai_overhead_sec:.1f}s"
                ai_color = "#ef4444" if msg.has_slow_ai else "#10b981"
                ai_icon = "⚠️" if msg.has_slow_ai else "✓"
                
                sql_duration = msg.total_duration_ms / 1000.0 if msg.total_duration_ms else 0
                sql_color = "#ef4444" if msg.has_slow_query else "#10b981"
                
                total_response = f"Total: {msg.total_response_sec:.1f}s"
                
                st.markdown(f"""
                <div class="message-card" style="border-left: 3px solid {'#ef4444' if msg.has_performance_issue else '#7c3aed'};">
                    <div class="message-prompt">💬 {msg_prompt}</div>
                    <div class="message-meta">
                        {query_count_label} • 
                        <span style="color: {ai_color};">{ai_icon} {ai_label}</span> • 
                        <span style="color: {sql_color};">SQL: {sql_duration:.1f}s</span> • 
                        <strong>{total_response}</strong>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                # Render queries for this message
                for q_idx, query in enumerate(msg.queries):
                    query_preview = query.query_text[:80] + "..." if len(query.query_text) > 80 else query.query_text
                    query_preview = query_preview.replace("<", "&lt;").replace(">", "&gt;")
                    
                    duration_sec = query.total_duration_ms / 1000.0 if query.total_duration_ms else 0
                    bottleneck_class = f"bottleneck-{query.bottleneck.lower()}"
                    speed_class = f"speed-{query.speed_category.lower()}"
                    
                    # Create columns for query row: sql, duration, ai, compile, execute, queue, genie_conc, wh_conc, bottleneck, profile, pdf
                    q_cols = st.columns([2.5, 0.7, 0.7, 0.7, 0.7, 0.7, 0.6, 0.6, 0.9, 0.4, 0.4])
                    
                    with q_cols[0]:
                        st.code(query_preview, language="sql")
                    
                    with q_cols[1]:
                        st.markdown(f"<span class='{speed_class}'>{duration_sec:.1f}s</span>", unsafe_allow_html=True)
                    
                    with q_cols[2]:
                        st.caption(f"AI: {msg.ai_overhead_sec:.1f}s")
                    
                    with q_cols[3]:
                        st.caption(f"Compile: {query.compilation_ms/1000:.1f}s")
                    
                    with q_cols[4]:
                        st.caption(f"Execute: {query.execution_ms/1000:.1f}s")
                    
                    with q_cols[5]:
                        st.caption(f"Queue: {query.queue_wait_ms/1000:.1f}s")
                    
                    with q_cols[6]:
                        st.caption(f"Genie: {query.genie_concurrent}")
                    
                    with q_cols[7]:
                        st.caption(f"WH: {query.warehouse_concurrent}")
                    
                    with q_cols[8]:
                        bottleneck_label = get_bottleneck_label(query.bottleneck)
                        color = get_bottleneck_color(query.bottleneck)
                        st.markdown(f"<span style='background:{color}20;color:{color};padding:2px 6px;border-radius:4px;font-size:10px;'>{bottleneck_label}</span>", unsafe_allow_html=True)
                    
                    with q_cols[9]:
                        # Query profile link
                        client = get_client()
                        profile_url = client.get_query_profile_url(query.statement_id)
                        if profile_url:
                            st.link_button("🔗", profile_url, help="View query profile in Databricks")
                    
                    with q_cols[10]:
                        # PDF download button
                        query_dict = {
                            "statement_id": query.statement_id,
                            "query_text": query.query_text,
                            "total_sec": query.total_duration_ms / 1000 if query.total_duration_ms else 0,
                            "compile_sec": query.compilation_ms / 1000 if query.compilation_ms else 0,
                            "execute_sec": query.execution_ms / 1000 if query.execution_ms else 0,
                            "queue_sec": query.queue_wait_ms / 1000 if query.queue_wait_ms else 0,
                            "wait_compute_sec": query.compute_wait_ms / 1000 if query.compute_wait_ms else 0,
                            "read_rows": query.rows_scanned,
                            "read_mb": query.bytes_scanned / 1024.0 / 1024.0 if query.bytes_scanned else 0,
                            "bottleneck": query.bottleneck,
                            "execution_status": query.execution_status,
                            "genie_space_id": room_id,
                            # Additional metrics for complete PDF
                            "ai_overhead_sec": msg.ai_overhead_sec,
                            "executed_by": query.executed_by,
                            "start_time": query.start_time,
                            "conversation_id": conv.conversation_id,
                        }
                        phase_df = build_query_phase_breakdown(query_dict)
                        pdf_bytes = generate_query_pdf_report(
                            query_dict, room_name, room_id, phase_df,
                            user_prompt=msg.content if msg.content else None
                        )
                        st.download_button(
                            "📄", 
                            pdf_bytes,
                            file_name=f"query_{query.statement_id[:8]}.pdf",
                            mime="application/pdf",
                            help="Download PDF report",
                            key=f"pdf_{conv.conversation_id}_{msg.message_id}_{query.statement_id}"
                        )
    
    # Check if selection was made via session state
    if "selected_query_from_tree" in st.session_state:
        selected_statement_id = st.session_state.get("selected_query_from_tree")
    
    return selected_statement_id


def load_conversations_with_metrics(
    client: DatabricksClient, 
    space_id: str, 
    max_conversations: int = 50
) -> list[ConversationWithMessages]:
    """Load conversations with their messages and query metrics."""
    try:
        return client.get_conversations_with_query_metrics(
            space_id=space_id,
            max_conversations=max_conversations,
        )
    except Exception as e:
        print(f"Error loading conversations: {e}")
        return []


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
    
    # View Query Profile button - opens Databricks Query History UI
    client = get_client()
    profile_url = client.get_query_profile_url(statement_id)
    if profile_url:
        st.link_button(
            "📊 View Query Profile in Databricks",
            profile_url,
            help="Open in Databricks to view full query profile and download execution plan as JSON",
        )
    
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
                
                status_text.markdown("**Step 1/8:** Calculating aggregate metrics (query count, avg duration, success rate)...")
                metrics = load_space_metrics(client, room_id, hours)
                progress_bar.progress(12)
                
                status_text.markdown("**Step 2/8:** Loading daily query volume and latency trends...")
                daily_df = load_daily_trends(client, room_id, hours)
                progress_bar.progress(25)
                
                status_text.markdown("**Step 3/8:** Analyzing query duration distribution (< 1s, 1-5s, 5-10s, etc.)...")
                duration_df = load_duration_distribution(client, room_id, hours)
                progress_bar.progress(37)
                
                status_text.markdown("**Step 4/8:** Analyzing response time breakdown (AI processing + SQL execution phases)...")
                phase_df = load_phase_breakdown(client, room_id, hours)
                progress_bar.progress(50)
                
                status_text.markdown("**Step 5/8:** Fetching individual queries with timing breakdown...")
                queries_df = load_queries(client, room_id, hours)
                progress_bar.progress(62)
                
                status_text.markdown("**Step 6/8:** Resolving user prompts via Genie Conversations API...")
                # Populate prompts for all queries using reverse lookup
                if not queries_df.empty:
                    prompts_dict = client.get_prompts_for_queries(room_id, queries_df)
                    queries_df["user_prompt"] = queries_df["statement_id"].map(
                        lambda sid: prompts_dict.get(sid, "")
                    )
                else:
                    queries_df["user_prompt"] = ""
                progress_bar.progress(75)
                
                status_text.markdown("**Step 7/8:** Loading AI conversation activity from audit logs...")
                conversation_daily_df = load_conversation_daily(client, room_id, hours)
                conversation_peak = load_conversation_peak(client, room_id, hours)
                progress_bar.progress(87)
                
                status_text.markdown("**Step 8/8:** Loading conversation tree with SQL query lineage...")
                conversations_with_metrics = load_conversations_with_metrics(client, room_id, max_conversations=50)
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
                "conversations_with_metrics": conversations_with_metrics,
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
            conversations_with_metrics = cached.get("conversations_with_metrics", [])
        
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
        
        # Conversation-centric view (primary) - shows conversations with messages and queries
        st.markdown("""
        <div style="background: rgba(124,58,237,0.1); border: 1px solid rgba(124,58,237,0.3); border-radius: 8px; padding: 12px; margin-bottom: 16px;">
            <strong>💬 Conversation Performance Analysis</strong><br/>
            <span style="color: #888; font-size: 13px;">
                Analyze Genie interactions from conversation → prompt → SQL query. 
                Identify slow AI responses and SQL execution bottlenecks.
                Click the 📊 button next to any query to view detailed metrics.
            </span>
        </div>
        """, unsafe_allow_html=True)
        
        # Sortable summary table of all conversations (returns selected IDs and sort params)
        selected_conv_ids, sort_by, sort_ascending = render_conversations_table(conversations_with_metrics)
        
        # Expandable conversation tree with details (filtered by selection, sorted same as table)
        selected_query = render_conversation_tree(
            conversations_with_metrics, 
            selected_ids=selected_conv_ids,
            room_name=room_name,
            room_id=room_id,
            sort_by=sort_by,
            sort_ascending=sort_ascending,
        )
        
        # If a query is selected (from either view), show its specific breakdown below
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
