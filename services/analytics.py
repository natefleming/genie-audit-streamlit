"""
Analytics Service

Provides bottleneck classification, query timeline generation,
and optimization recommendations for Genie queries.
"""

from typing import Optional
from dataclasses import dataclass

from queries.sql import QUERY_HISTORY_TABLE, AUDIT_TABLE


@dataclass
class QueryOptimization:
    """Represents a query optimization recommendation."""
    category: str  # performance, cost, reliability
    severity: str  # high, medium, low
    title: str
    description: str
    recommendation: str


@dataclass
class QueryTimeline:
    """Represents a phase in the query execution timeline."""
    phase: str
    start_ms: int
    duration_ms: int
    percentage: float


@dataclass
class DiagnosticQuery:
    """Represents a recommended diagnostic SQL query."""
    title: str
    description: str
    sql: str
    category: str  # statistics, performance, data, monitoring


def _to_int(value, default: int = 0) -> int:
    """Safely convert a value to int, handling None and strings."""
    if value is None:
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def _to_float(value, default: float = 0.0) -> float:
    """Safely convert a value to float, handling None and strings."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def classify_bottleneck(
    compilation_ms,
    execution_ms,
    queue_wait_ms,
    compute_wait_ms,
    total_ms,
    bytes_scanned=0,
) -> str:
    """
    Classify the bottleneck type for a query based on timing metrics.
    
    Args:
        compilation_ms: Time spent in SQL compilation
        execution_ms: Time spent executing the query
        queue_wait_ms: Time waiting in queue
        compute_wait_ms: Time waiting for compute resources
        total_ms: Total query duration
        bytes_scanned: Bytes scanned by the query
        
    Returns:
        Bottleneck type string
    """
    # Convert all inputs to numeric types
    total_ms = _to_int(total_ms)
    compilation_ms = _to_int(compilation_ms)
    execution_ms = _to_int(execution_ms)
    queue_wait_ms = _to_int(queue_wait_ms)
    compute_wait_ms = _to_int(compute_wait_ms)
    bytes_scanned = _to_int(bytes_scanned)
    
    if total_ms <= 0:
        return "NORMAL"
    
    # Calculate percentages
    queue_pct = queue_wait_ms / total_ms
    compute_pct = compute_wait_ms / total_ms
    comp_pct = compilation_ms / total_ms
    
    # Classify based on thresholds
    if compute_pct > 0.5:
        return "COMPUTE_STARTUP"
    if queue_pct > 0.3:
        return "QUEUE_WAIT"
    if comp_pct > 0.4:
        return "COMPILATION"
    if bytes_scanned > 1073741824:  # 1 GB
        return "LARGE_SCAN"
    if execution_ms > 10000:
        return "SLOW_EXECUTION"
    
    return "NORMAL"


def get_query_timeline(query: dict) -> list[QueryTimeline]:
    """
    Generate a timeline of query execution phases.
    
    Args:
        query: Query dict with timing fields
        
    Returns:
        List of QueryTimeline objects
    """
    total_ms = _to_int(query.get("total_duration_ms") or query.get("duration_ms"), 0)
    if total_ms <= 0:
        total_ms = 1
    
    phases = [
        ("Queue Wait", _to_int(query.get("queue_wait_ms"))),
        ("Compute Startup", _to_int(query.get("compute_wait_ms"))),
        ("Compilation", _to_int(query.get("compilation_ms"))),
        ("Execution", _to_int(query.get("execution_ms"))),
        ("Result Fetch", _to_int(query.get("result_fetch_ms"))),
    ]
    
    timeline = []
    current_start = 0
    
    for phase_name, duration in phases:
        percentage = round((duration / total_ms) * 100, 1)
        
        timeline.append(QueryTimeline(
            phase=phase_name,
            start_ms=current_start,
            duration_ms=duration,
            percentage=percentage,
        ))
        
        current_start += duration
    
    return timeline


def get_query_optimizations(query: dict) -> list[QueryOptimization]:
    """
    Generate optimization recommendations for a query based on bottleneck type.
    
    Args:
        query: Query dict with metrics and timing fields
        
    Returns:
        List of QueryOptimization objects with specific, actionable recommendations
    """
    optimizations = []
    
    # Extract metrics with safe conversion
    duration_ms = _to_int(query.get("total_duration_ms") or query.get("duration_ms"))
    compilation_ms = _to_int(query.get("compilation_ms"))
    execution_ms = _to_int(query.get("execution_ms"))
    queue_wait_ms = _to_int(query.get("queue_wait_ms"))
    compute_wait_ms = _to_int(query.get("compute_wait_ms"))
    bytes_scanned = _to_int(query.get("bytes_scanned"))
    rows_scanned = _to_int(query.get("rows_scanned") or query.get("read_rows"))
    rows_returned = _to_int(query.get("rows_returned") or query.get("produced_rows"))
    ai_overhead_sec = _to_float(query.get("ai_overhead_sec"))
    
    bottleneck = query.get("bottleneck", "") or classify_bottleneck(
        compilation_ms, execution_ms, queue_wait_ms, 
        compute_wait_ms, duration_ms, bytes_scanned
    )
    
    # ==========================================================================
    # COMPUTE STARTUP - Warehouse cold start issues
    # ==========================================================================
    if bottleneck == "COMPUTE_STARTUP" or compute_wait_ms > 30000:
        wait_sec = compute_wait_ms / 1000
        optimizations.append(QueryOptimization(
            category="infrastructure",
            severity="high" if wait_sec > 60 else "medium",
            title="⏳ Compute Startup Delay",
            description=f"Waited {wait_sec:.1f}s for compute resources. The SQL warehouse was likely suspended or scaling up.",
            recommendation=(
                "**Immediate actions:**\n"
                "• Switch to Serverless SQL Warehouse for instant startup (< 5s)\n"
                "• Increase auto-suspend timeout to 30+ minutes during business hours\n\n"
                "**Configuration changes:**\n"
                "• Enable 'Pre-warming' in warehouse settings\n"
                "• Set min clusters > 0 during peak hours\n"
                "• Schedule a lightweight query every 10 min to keep warehouse warm"
            ),
        ))
    
    # ==========================================================================
    # QUEUE WAIT - Warehouse capacity issues
    # ==========================================================================
    if bottleneck == "QUEUE_WAIT" or queue_wait_ms > 10000:
        wait_sec = queue_wait_ms / 1000
        optimizations.append(QueryOptimization(
            category="infrastructure",
            severity="high" if wait_sec > 30 else "medium",
            title="🚦 Query Queue Congestion",
            description=f"Query waited {wait_sec:.1f}s in queue. The warehouse is at capacity with concurrent queries.",
            recommendation=(
                "**Immediate actions:**\n"
                "• Increase warehouse size (e.g., Medium → Large)\n"
                "• Enable auto-scaling with max clusters = 3-5\n\n"
                "**Architectural improvements:**\n"
                "• Create separate warehouses for different workloads (BI vs Genie)\n"
                "• Schedule batch analytics during off-peak hours\n"
                "• Use query queues with priorities (premium users first)"
            ),
        ))
    
    # ==========================================================================
    # COMPILATION - Complex query structure
    # ==========================================================================
    if bottleneck == "COMPILATION" or compilation_ms > 5000:
        compile_sec = compilation_ms / 1000
        optimizations.append(QueryOptimization(
            category="query_design",
            severity="high" if compile_sec > 15 else "medium",
            title="🔧 Complex Query Compilation",
            description=f"Compilation took {compile_sec:.1f}s. Query structure is too complex for efficient planning.",
            recommendation=(
                "**Query simplification:**\n"
                "• Break query into multiple smaller CTEs or temporary views\n"
                "• Reduce the number of JOINs (aim for < 5 tables)\n"
                "• Replace SELECT * with explicit column lists\n\n"
                "**Genie configuration:**\n"
                "• Review table instructions to guide simpler queries\n"
                "• Add sample queries showing preferred join patterns\n"
                "• Consider pre-aggregating complex metrics into summary tables"
            ),
        ))
    
    # ==========================================================================
    # LARGE SCAN - Data access inefficiency
    # ==========================================================================
    if bottleneck == "LARGE_SCAN" or bytes_scanned > 1073741824:
        gb_scanned = bytes_scanned / (1024 * 1024 * 1024)
        optimizations.append(QueryOptimization(
            category="data_design",
            severity="high" if gb_scanned > 10 else "medium",
            title="📊 Large Data Scan ({:.1f} GB)".format(gb_scanned),
            description=f"Query scanned {gb_scanned:.2f} GB of data, which impacts performance and cost.",
            recommendation=(
                "**Add partition filters:**\n"
                "• Filter on partition columns (date, region, etc.) in WHERE clause\n"
                "• Ensure Genie instructions mention partition columns for filtering\n\n"
                "**Optimize table design:**\n"
                "• Use Z-ORDER on frequently filtered columns\n"
                "• Enable Predictive Optimization for automatic Z-ORDER\n"
                "• Create aggregated/summary tables for common analytics\n\n"
                "**Column pruning:**\n"
                "• Select only required columns instead of SELECT *\n"
                "• Consider columnar format (Delta) if not already using"
            ),
        ))
    
    # ==========================================================================
    # SLOW EXECUTION - Query execution inefficiency
    # ==========================================================================
    if bottleneck == "SLOW_EXECUTION" or execution_ms > 60000:
        exec_sec = execution_ms / 1000
        optimizations.append(QueryOptimization(
            category="query_design",
            severity="high" if exec_sec > 120 else "medium",
            title="🐢 Slow Query Execution ({:.1f}s)".format(exec_sec),
            description=f"Execution took {exec_sec:.1f}s. Query operations (joins, aggregations) are expensive.",
            recommendation=(
                "**Query optimization:**\n"
                "• Review Query Profile in SQL Editor for bottleneck stages\n"
                "• Check for Cartesian products or missing join conditions\n"
                "• Add filters early in CTEs to reduce intermediate data\n\n"
                "**Join optimization:**\n"
                "• Put smaller tables on the right side of JOINs\n"
                "• Use broadcast hints for small dimension tables\n"
                "• Consider denormalizing frequently joined tables\n\n"
                "**Statistics & caching:**\n"
                "• Run ANALYZE TABLE to update statistics\n"
                "• Enable result caching for repeated queries\n"
                "• Use materialized views for complex aggregations"
            ),
        ))
    
    # ==========================================================================
    # AI OVERHEAD - GenAI processing time
    # ==========================================================================
    if ai_overhead_sec > 10:
        optimizations.append(QueryOptimization(
            category="ai_processing",
            severity="high" if ai_overhead_sec > 30 else "medium",
            title="🤖 High AI Processing Time ({:.1f}s)".format(ai_overhead_sec),
            description=f"Genie spent {ai_overhead_sec:.1f}s understanding the question and generating SQL.",
            recommendation=(
                "**Improve Genie instructions:**\n"
                "• Add clear table descriptions with business context\n"
                "• Include column descriptions for ambiguous field names\n"
                "• Provide sample questions and their expected queries\n\n"
                "**Simplify data model:**\n"
                "• Reduce the number of tables in the Genie space\n"
                "• Create views with business-friendly names\n"
                "• Pre-join commonly related tables\n\n"
                "**Question phrasing:**\n"
                "• Use specific table/column names when possible\n"
                "• Break complex questions into simpler parts\n"
                "• Avoid ambiguous terms that require interpretation"
            ),
        ))
    
    # ==========================================================================
    # LOW SELECTIVITY - Inefficient filtering
    # ==========================================================================
    if rows_scanned > 1000000 and rows_returned > 0:
        selectivity = rows_returned / rows_scanned
        if selectivity < 0.001:
            optimizations.append(QueryOptimization(
                category="data_design",
                severity="low",
                title="🎯 Low Row Selection Efficiency",
                description=f"Only {rows_returned:,} rows returned from {rows_scanned:,} scanned ({selectivity * 100:.4f}% selectivity).",
                recommendation=(
                    "**Improve filtering:**\n"
                    "• Add more selective WHERE conditions\n"
                    "• Filter on indexed or Z-ORDERed columns\n"
                    "• Consider creating filtered views for common subsets\n\n"
                    "**Data organization:**\n"
                    "• Partition table by commonly filtered columns\n"
                    "• Run OPTIMIZE with Z-ORDER on filter columns\n"
                    "• Update table statistics with ANALYZE TABLE"
                ),
            ))
    
    # ==========================================================================
    # NORMAL - Query performing well
    # ==========================================================================
    if not optimizations:
        optimizations.append(QueryOptimization(
            category="performance",
            severity="low",
            title="✅ Query Performing Well",
            description="This query is executing within expected performance parameters.",
            recommendation=(
                "**Maintain good performance:**\n"
                "• Continue monitoring for performance degradation\n"
                "• Run OPTIMIZE periodically to prevent data fragmentation\n"
                "• Keep table statistics up to date with ANALYZE TABLE\n"
                "• Review Query Profile occasionally for optimization opportunities"
            ),
        ))
    
    return optimizations


def get_bottleneck_recommendation(bottleneck: str) -> str:
    """
    Get a brief recommendation summary for a bottleneck type.
    
    Args:
        bottleneck: Bottleneck type string
        
    Returns:
        Recommendation string
    """
    recommendations = {
        "COMPUTE_STARTUP": "Switch to Serverless SQL Warehouse or increase auto-suspend timeout",
        "QUEUE_WAIT": "Scale up warehouse size, enable auto-scaling, or distribute workloads",
        "COMPILATION": "Simplify query structure, reduce JOINs, or break into smaller CTEs",
        "LARGE_SCAN": "Add partition filters, use Z-ORDER, or select only needed columns",
        "SLOW_EXECUTION": "Review Query Profile, optimize JOINs, or create materialized views",
        "NORMAL": "Query performing well - continue monitoring",
    }
    
    return recommendations.get(bottleneck, "Review Query Profile for specific optimization opportunities")


def get_speed_category(duration_ms) -> str:
    """
    Get speed category based on duration.
    
    Args:
        duration_ms: Query duration in milliseconds
        
    Returns:
        Speed category string (FAST, MODERATE, SLOW, CRITICAL)
    """
    duration_ms = _to_int(duration_ms)
    if duration_ms >= 30000:
        return "CRITICAL"
    if duration_ms >= 10000:
        return "SLOW"
    if duration_ms >= 5000:
        return "MODERATE"
    return "FAST"


def get_diagnostic_queries(query: dict) -> list[DiagnosticQuery]:
    """
    Generate recommended diagnostic SQL queries based on the query's bottleneck type.
    
    These queries help users investigate and resolve performance issues.
    
    Args:
        query: Query dict with metrics including bottleneck, table names, etc.
        
    Returns:
        List of DiagnosticQuery objects with copy-pastable SQL
    """
    queries = []
    
    bottleneck = query.get("bottleneck", "NORMAL")
    statement_id = query.get("statement_id", "")
    genie_space_id = query.get("genie_space_id", "")
    
    # Extract table name from query text if available
    query_text = query.get("query_text", "") or ""
    
    # ==========================================================================
    # ALWAYS INCLUDE: Query History Details
    # ==========================================================================
    if statement_id:
        queries.append(DiagnosticQuery(
            title="Query Execution Details",
            description="Get full execution details for this specific query from query history",
            category="monitoring",
            sql=f"""-- Full execution details for this query
SELECT 
    statement_id,
    executed_by,
    start_time,
    end_time,
    total_duration_ms / 1000.0 AS duration_sec,
    execution_status,
    error_message,
    rows_produced,
    read_bytes / (1024*1024*1024) AS read_gb,
    compilation_duration_ms,
    execution_duration_ms,
    waiting_for_compute_duration_ms,
    waiting_at_capacity_duration_ms,
    statement_text
FROM {QUERY_HISTORY_TABLE}
WHERE statement_id = '{statement_id}'"""
        ))
    
    # ==========================================================================
    # COMPUTE STARTUP - Warehouse diagnostics
    # ==========================================================================
    if bottleneck == "COMPUTE_STARTUP":
        queries.append(DiagnosticQuery(
            title="Warehouse Cold Start Analysis",
            description="Analyze how often this warehouse experiences cold starts",
            category="performance",
            sql=f"""-- Analyze cold start frequency for the warehouse
SELECT 
    warehouse_id,
    DATE(start_time) AS query_date,
    COUNT(*) AS total_queries,
    SUM(CASE WHEN waiting_for_compute_duration_ms > 10000 THEN 1 ELSE 0 END) AS cold_starts,
    ROUND(100.0 * SUM(CASE WHEN waiting_for_compute_duration_ms > 10000 THEN 1 ELSE 0 END) / COUNT(*), 1) AS cold_start_pct,
    ROUND(AVG(waiting_for_compute_duration_ms) / 1000.0, 1) AS avg_startup_sec
FROM {QUERY_HISTORY_TABLE}
WHERE query_source.genie_space_id = '{genie_space_id}'
    AND start_time >= current_timestamp() - INTERVAL 7 DAY
GROUP BY warehouse_id, DATE(start_time)
ORDER BY query_date DESC"""
        ))
        
        queries.append(DiagnosticQuery(
            title="Warehouse Usage Gaps",
            description="Find gaps in warehouse usage that cause cold starts",
            category="performance",
            sql="""-- Find time gaps between queries (causing cold starts)
WITH query_times AS (
    SELECT 
        start_time,
        LAG(end_time) OVER (ORDER BY start_time) AS prev_end_time
    FROM {QUERY_HISTORY_TABLE}
    WHERE query_source.genie_space_id IS NOT NULL
        AND start_time >= current_timestamp() - INTERVAL 1 DAY
)
SELECT 
    DATE_TRUNC('hour', start_time) AS hour,
    COUNT(*) AS queries,
    ROUND(AVG(TIMESTAMPDIFF(MINUTE, prev_end_time, start_time)), 1) AS avg_gap_minutes,
    MAX(TIMESTAMPDIFF(MINUTE, prev_end_time, start_time)) AS max_gap_minutes
FROM query_times
WHERE prev_end_time IS NOT NULL
GROUP BY DATE_TRUNC('hour', start_time)
ORDER BY hour DESC"""
        ))
    
    # ==========================================================================
    # QUEUE WAIT - Concurrency diagnostics
    # ==========================================================================
    if bottleneck == "QUEUE_WAIT":
        queries.append(DiagnosticQuery(
            title="Peak Concurrency Analysis",
            description="Find peak concurrent query periods causing queue wait",
            category="performance",
            sql=f"""-- Analyze concurrent query load by hour
SELECT 
    DATE_TRUNC('hour', start_time) AS hour,
    COUNT(*) AS total_queries,
    ROUND(AVG(waiting_at_capacity_duration_ms) / 1000.0, 1) AS avg_queue_sec,
    MAX(waiting_at_capacity_duration_ms) / 1000.0 AS max_queue_sec,
    SUM(CASE WHEN waiting_at_capacity_duration_ms > 5000 THEN 1 ELSE 0 END) AS queued_queries
FROM {QUERY_HISTORY_TABLE}
WHERE query_source.genie_space_id = '{genie_space_id}'
    AND start_time >= current_timestamp() - INTERVAL 7 DAY
GROUP BY DATE_TRUNC('hour', start_time)
HAVING COUNT(*) > 5
ORDER BY avg_queue_sec DESC
LIMIT 20"""
        ))
        
        queries.append(DiagnosticQuery(
            title="Concurrent Running Queries",
            description="See what other queries were running at the same time",
            category="monitoring",
            sql=f"""-- Find queries running concurrently with the slow query
SELECT 
    statement_id,
    executed_by,
    start_time,
    end_time,
    total_duration_ms / 1000.0 AS duration_sec,
    LEFT(statement_text, 200) AS query_preview
FROM {QUERY_HISTORY_TABLE}
WHERE start_time >= (
    SELECT start_time - INTERVAL 5 MINUTE
    FROM {QUERY_HISTORY_TABLE} 
    WHERE statement_id = '{statement_id}'
)
AND end_time <= (
    SELECT end_time + INTERVAL 5 MINUTE
    FROM {QUERY_HISTORY_TABLE} 
    WHERE statement_id = '{statement_id}'
)
AND statement_id != '{statement_id}'
ORDER BY total_duration_ms DESC
LIMIT 20"""
        ))
    
    # ==========================================================================
    # COMPILATION - Query complexity diagnostics
    # ==========================================================================
    if bottleneck == "COMPILATION":
        queries.append(DiagnosticQuery(
            title="High Compilation Time Queries",
            description="Find other queries with high compilation times for pattern analysis",
            category="performance",
            sql=f"""-- Find queries with high compilation times
SELECT 
    LEFT(statement_text, 300) AS query_preview,
    COUNT(*) AS occurrences,
    ROUND(AVG(compilation_duration_ms) / 1000.0, 1) AS avg_compile_sec,
    ROUND(AVG(total_duration_ms) / 1000.0, 1) AS avg_total_sec,
    ROUND(100.0 * AVG(compilation_duration_ms) / NULLIF(AVG(total_duration_ms), 0), 0) AS compile_pct
FROM {QUERY_HISTORY_TABLE}
WHERE query_source.genie_space_id = '{genie_space_id}'
    AND start_time >= current_timestamp() - INTERVAL 7 DAY
    AND compilation_duration_ms > 2000
GROUP BY LEFT(statement_text, 300)
ORDER BY avg_compile_sec DESC
LIMIT 10"""
        ))
    
    # ==========================================================================
    # LARGE SCAN - Data volume diagnostics
    # ==========================================================================
    if bottleneck == "LARGE_SCAN":
        queries.append(DiagnosticQuery(
            title="Table Scan Analysis",
            description="Analyze data volumes being scanned by Genie queries",
            category="data",
            sql=f"""-- Analyze scan volumes for this Genie space
SELECT 
    DATE(start_time) AS query_date,
    COUNT(*) AS queries,
    ROUND(SUM(read_bytes) / (1024*1024*1024), 1) AS total_gb_scanned,
    ROUND(AVG(read_bytes) / (1024*1024*1024), 2) AS avg_gb_per_query,
    ROUND(MAX(read_bytes) / (1024*1024*1024), 2) AS max_gb_scanned
FROM {QUERY_HISTORY_TABLE}
WHERE query_source.genie_space_id = '{genie_space_id}'
    AND start_time >= current_timestamp() - INTERVAL 7 DAY
GROUP BY DATE(start_time)
ORDER BY query_date DESC"""
        ))
        
        queries.append(DiagnosticQuery(
            title="Check Table Statistics",
            description="Verify table statistics are up to date (run ANALYZE if stale)",
            category="statistics",
            sql="""-- Check table optimization status
-- Replace 'catalog.schema.table' with your actual table
DESCRIBE DETAIL catalog.schema.your_table;

-- View table history and optimizations
DESCRIBE HISTORY catalog.schema.your_table LIMIT 20;

-- Update statistics (run if stale)
-- ANALYZE TABLE catalog.schema.your_table COMPUTE STATISTICS;"""
        ))
        
        queries.append(DiagnosticQuery(
            title="Check Partition Columns",
            description="View table partitioning to ensure filters use partition columns",
            category="data",
            sql="""-- View table schema and partitioning
-- Replace 'catalog.schema.table' with your actual table
DESCRIBE EXTENDED catalog.schema.your_table;

-- Check if table has Z-ORDER clustering
DESCRIBE DETAIL catalog.schema.your_table;"""
        ))
    
    # ==========================================================================
    # SLOW EXECUTION - Execution diagnostics
    # ==========================================================================
    if bottleneck == "SLOW_EXECUTION":
        queries.append(DiagnosticQuery(
            title="Similar Slow Queries",
            description="Find similar queries to identify optimization patterns",
            category="performance",
            sql=f"""-- Find similar slow queries for pattern analysis
SELECT 
    LEFT(statement_text, 300) AS query_pattern,
    COUNT(*) AS occurrences,
    ROUND(AVG(execution_duration_ms) / 1000.0, 1) AS avg_exec_sec,
    ROUND(MAX(execution_duration_ms) / 1000.0, 1) AS max_exec_sec,
    ROUND(AVG(read_bytes) / (1024*1024), 0) AS avg_mb_read
FROM {QUERY_HISTORY_TABLE}
WHERE query_source.genie_space_id = '{genie_space_id}'
    AND start_time >= current_timestamp() - INTERVAL 7 DAY
    AND execution_duration_ms > 10000
GROUP BY LEFT(statement_text, 300)
ORDER BY occurrences DESC
LIMIT 10"""
        ))
        
        queries.append(DiagnosticQuery(
            title="Query Profile Link",
            description="Open Query Profile in Databricks SQL to see execution plan",
            category="monitoring",
            sql=f"""-- To view the Query Profile:
-- 1. Go to Databricks SQL Editor
-- 2. Click on "Query History" tab
-- 3. Search for statement_id: {statement_id}
-- 4. Click "View Query Profile" to see the execution plan

-- Or run this to get a link to the query in history:
SELECT 
    statement_id,
    CONCAT('https://', current_catalog(), '.cloud.databricks.com/sql/history/', statement_id) AS query_profile_url
FROM {QUERY_HISTORY_TABLE}
WHERE statement_id = '{statement_id}'"""
        ))
    
    # ==========================================================================
    # ALWAYS INCLUDE: General monitoring queries
    # ==========================================================================
    queries.append(DiagnosticQuery(
        title="Genie Space Performance Summary",
        description="Overall performance metrics for this Genie space",
        category="monitoring",
        sql=f"""-- Performance summary for this Genie space
SELECT 
    COUNT(*) AS total_queries,
    ROUND(AVG(total_duration_ms) / 1000.0, 1) AS avg_duration_sec,
    ROUND(PERCENTILE(total_duration_ms, 0.50) / 1000.0, 1) AS p50_sec,
    ROUND(PERCENTILE(total_duration_ms, 0.90) / 1000.0, 1) AS p90_sec,
    ROUND(PERCENTILE(total_duration_ms, 0.95) / 1000.0, 1) AS p95_sec,
    SUM(CASE WHEN total_duration_ms > 10000 THEN 1 ELSE 0 END) AS slow_queries,
    ROUND(100.0 * SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) / COUNT(*), 1) AS success_pct,
    COUNT(DISTINCT executed_by) AS unique_users
FROM {QUERY_HISTORY_TABLE}
WHERE query_source.genie_space_id = '{genie_space_id}'
    AND start_time >= current_timestamp() - INTERVAL 7 DAY"""
    ))
    
    # ==========================================================================
    # ALWAYS INCLUDE: Data Correlation Query
    # ==========================================================================
    queries.append(DiagnosticQuery(
        title="Correlate SQL Queries with Genie API Events",
        description="Join SQL warehouse statement IDs with API request IDs and Genie space IDs to understand the full request lifecycle",
        category="monitoring",
        sql=f"""-- Correlate SQL query history with Genie API audit events
-- This query shows the relationship between:
--   - {QUERY_HISTORY_TABLE}: SQL execution metrics (statement_id, duration, etc.)
--   - {AUDIT_TABLE}: Genie API events (request_id, action_name, etc.)

WITH genie_messages AS (
    -- Get Genie conversation/message events from audit logs
    SELECT 
        request_id AS api_request_id,
        request_params.space_id AS genie_space_id,
        request_params.conversation_id,
        event_time AS message_time,
        action_name,
        user_identity.email AS user_email
    FROM {AUDIT_TABLE}
    WHERE service_name = 'genieV2'
      AND action_name IN ('genieStartConversationMessage', 'genieContinueConversationMessage')
      AND request_params.space_id = '{genie_space_id}'
      AND event_time >= current_timestamp() - INTERVAL 7 DAY
),
sql_queries AS (
    -- Get SQL queries from query history
    SELECT 
        statement_id,
        query_source.genie_space_id,
        executed_by,
        start_time,
        end_time,
        total_duration_ms,
        execution_status,
        LEFT(statement_text, 200) AS query_preview
    FROM {QUERY_HISTORY_TABLE}
    WHERE query_source.genie_space_id = '{genie_space_id}'
      AND start_time >= current_timestamp() - INTERVAL 7 DAY
)
-- Join: Find SQL queries that started within 60 seconds of a Genie message
SELECT 
    m.api_request_id,
    m.conversation_id,
    m.genie_space_id,
    m.user_email,
    m.message_time,
    m.action_name AS genie_action,
    q.statement_id,
    q.start_time AS sql_start_time,
    q.end_time AS sql_end_time,
    ROUND((UNIX_TIMESTAMP(q.start_time) - UNIX_TIMESTAMP(m.message_time)), 1) AS ai_processing_sec,
    ROUND(q.total_duration_ms / 1000.0, 1) AS sql_duration_sec,
    q.execution_status,
    q.query_preview
FROM genie_messages m
LEFT JOIN sql_queries q
    ON m.genie_space_id = q.genie_space_id
    AND q.start_time BETWEEN m.message_time AND m.message_time + INTERVAL 60 SECOND
    AND q.executed_by = m.user_email
ORDER BY m.message_time DESC
LIMIT 100"""
    ))
    
    queries.append(DiagnosticQuery(
        title="Data Source Reference",
        description="Explains the key columns and relationships between system tables",
        category="monitoring",
        sql=f"""-- ============================================================================
-- DATA SOURCE REFERENCE: Correlating Genie API with SQL Execution
-- ============================================================================

-- SYSTEM.QUERY.HISTORY - SQL Execution Metrics
-- =============================================
-- Key columns for Genie correlation:
--   statement_id              : Unique SQL statement identifier
--   query_source.genie_space_id : Genie space that triggered this query
--   executed_by               : User who executed the query
--   start_time / end_time     : Query execution time window
--   total_duration_ms         : End-to-end query duration
--   compilation_duration_ms   : Time to compile/optimize query
--   execution_duration_ms     : Time to execute query
--   waiting_for_compute_duration_ms : Time waiting for warehouse startup
--   waiting_at_capacity_duration_ms : Time waiting in queue

-- SYSTEM.ACCESS.AUDIT - Genie API Events
-- ======================================
-- Key columns for Genie correlation:
--   request_id                : API request identifier
--   request_params.space_id   : Genie space ID
--   request_params.conversation_id : Conversation thread ID
--   event_time                : When the API event occurred
--   action_name               : Type of Genie action:
--     - 'genieStartConversationMessage' : New conversation started
--     - 'genieContinueConversationMessage' : Follow-up message
--     - 'genieExecuteSql' : SQL execution triggered
--   user_identity.email       : User who made the request

-- HOW TO CORRELATE THEM
-- =====================
-- 1. Match by genie_space_id (both tables have this)
-- 2. Match by user (executed_by = user_identity.email)
-- 3. Match by time window (SQL start_time should be shortly after message event_time)
-- 4. The difference between message event_time and SQL start_time = AI processing time

-- Example: Find the AI overhead for this specific query
-- Statement ID: {statement_id}
-- Genie Space ID: {genie_space_id}
SELECT 
    (SELECT MIN(event_time) 
     FROM {AUDIT_TABLE} 
     WHERE service_name = 'genieV2'
       AND action_name LIKE 'genie%Message'
       AND request_params.space_id = '{genie_space_id}'
       AND event_time BETWEEN 
           (SELECT start_time - INTERVAL 60 SECOND FROM {QUERY_HISTORY_TABLE} WHERE statement_id = '{statement_id}')
           AND (SELECT start_time FROM {QUERY_HISTORY_TABLE} WHERE statement_id = '{statement_id}')
    ) AS message_time,
    start_time AS sql_start_time,
    ROUND((UNIX_TIMESTAMP(start_time) - UNIX_TIMESTAMP(
        (SELECT MIN(event_time) 
         FROM {AUDIT_TABLE} 
         WHERE service_name = 'genieV2'
           AND action_name LIKE 'genie%Message'
           AND request_params.space_id = '{genie_space_id}'
           AND event_time BETWEEN start_time - INTERVAL 60 SECOND AND start_time)
    )), 1) AS ai_overhead_sec
FROM {QUERY_HISTORY_TABLE}
WHERE statement_id = '{statement_id}'"""
    ))
    
    return queries


def map_status(status: str) -> str:
    """
    Map Databricks execution status to display status.
    
    Args:
        status: Databricks execution status
        
    Returns:
        Display status string
    """
    status_upper = (status or "").upper()
    
    if status_upper in ("FINISHED", "SUCCEEDED"):
        return "success"
    if status_upper == "FAILED":
        return "failed"
    if status_upper in ("CANCELED", "CANCELLED"):
        return "cancelled"
    
    return "unknown"
