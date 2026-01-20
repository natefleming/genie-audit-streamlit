"""
SQL Queries for Genie Performance Audit

Consolidated from:
- genie_audit_bundle/src/dashboards/genie_performance.lvdash.json
- genie_audit_bundle/tests/test_queries.sql
- genie_performance_audit.sql
"""

# ============================================================================
# GLOBAL SUMMARY QUERIES
# ============================================================================

SUMMARY_STATS_QUERY = """
SELECT
  COUNT(*) AS total_queries,
  COUNT(DISTINCT query_source.genie_space_id) AS genie_spaces,
  COUNT(DISTINCT executed_by) AS unique_users,
  ROUND(AVG(total_duration_ms) / 1000.0, 2) AS avg_duration_sec,
  ROUND(PERCENTILE(total_duration_ms, 0.50) / 1000.0, 2) AS p50_sec,
  ROUND(PERCENTILE(total_duration_ms, 0.90) / 1000.0, 2) AS p90_sec,
  ROUND(PERCENTILE(total_duration_ms, 0.95) / 1000.0, 2) AS p95_sec,
  ROUND(PERCENTILE(total_duration_ms, 0.99) / 1000.0, 2) AS p99_sec,
  SUM(CASE WHEN total_duration_ms >= 10000 THEN 1 ELSE 0 END) AS slow_10s,
  SUM(CASE WHEN total_duration_ms >= 30000 THEN 1 ELSE 0 END) AS slow_30s,
  ROUND(100.0 * SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) / COUNT(*), 1) AS success_rate_pct
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
"""

# ============================================================================
# GENIE SPACES QUERIES
# ============================================================================

GENIE_SPACES_QUERY = """
SELECT
  query_source.genie_space_id AS genie_space_id,
  COUNT(*) AS total_queries,
  COUNT(DISTINCT executed_by) AS unique_users,
  ROUND(AVG(total_duration_ms) / 1000.0, 2) AS avg_duration_sec,
  ROUND(PERCENTILE(total_duration_ms, 0.50) / 1000.0, 2) AS p50_sec,
  ROUND(PERCENTILE(total_duration_ms, 0.90) / 1000.0, 2) AS p90_sec,
  ROUND(PERCENTILE(total_duration_ms, 0.95) / 1000.0, 2) AS p95_sec,
  ROUND(MAX(total_duration_ms) / 1000.0, 2) AS max_sec,
  SUM(CASE WHEN total_duration_ms >= 10000 THEN 1 ELSE 0 END) AS slow_10s,
  SUM(CASE WHEN total_duration_ms >= 30000 THEN 1 ELSE 0 END) AS slow_30s,
  ROUND(100.0 * SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) / COUNT(*), 1) AS success_rate_pct,
  ROUND(SUM(COALESCE(read_bytes, 0)) / 1024.0 / 1024.0 / 1024.0, 2) AS total_gb_read
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
GROUP BY query_source.genie_space_id
ORDER BY total_queries DESC
"""

SPACE_METRICS_QUERY = """
SELECT
  COUNT(*) AS total_queries,
  COUNT(DISTINCT executed_by) AS unique_users,
  ROUND(AVG(total_duration_ms) / 1000.0, 2) AS avg_duration_sec,
  ROUND(PERCENTILE(total_duration_ms, 0.50) / 1000.0, 2) AS p50_sec,
  ROUND(PERCENTILE(total_duration_ms, 0.90) / 1000.0, 2) AS p90_sec,
  ROUND(PERCENTILE(total_duration_ms, 0.95) / 1000.0, 2) AS p95_sec,
  ROUND(PERCENTILE(total_duration_ms, 0.99) / 1000.0, 2) AS p99_sec,
  SUM(CASE WHEN total_duration_ms >= 10000 THEN 1 ELSE 0 END) AS slow_10s,
  SUM(CASE WHEN total_duration_ms >= 30000 THEN 1 ELSE 0 END) AS slow_30s,
  SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) AS successful_queries,
  SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_queries,
  ROUND(100.0 * SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) / COUNT(*), 1) AS success_rate_pct
FROM system.query.history
WHERE query_source.genie_space_id = '{space_id}'
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
"""

# ============================================================================
# BOTTLENECK ANALYSIS
# ============================================================================

BOTTLENECK_DISTRIBUTION_QUERY = """
SELECT
  CASE
    WHEN COALESCE(waiting_for_compute_duration_ms, 0) > total_duration_ms * 0.5 THEN 'Compute Startup'
    WHEN COALESCE(waiting_at_capacity_duration_ms, 0) > total_duration_ms * 0.3 THEN 'Queue Wait'
    WHEN COALESCE(compilation_duration_ms, 0) > total_duration_ms * 0.4 THEN 'Compilation'
    WHEN COALESCE(read_bytes, 0) > 1073741824 THEN 'Large Scan'
    WHEN COALESCE(execution_duration_ms, 0) > 10000 THEN 'Slow Execution'
    ELSE 'Normal'
  END AS bottleneck_type,
  COUNT(*) AS query_count,
  ROUND(SUM(total_duration_ms) / 1000.0 / 60.0, 2) AS total_time_min,
  ROUND(AVG(total_duration_ms) / 1000.0, 2) AS avg_duration_sec
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  {space_filter}
GROUP BY 1
ORDER BY total_time_min DESC
"""

# ============================================================================
# PHASE BREAKDOWN - Time spent in each response phase (per-request correlation)
# ============================================================================

# Per-request breakdown correlating message events with SQL queries
PER_REQUEST_BREAKDOWN_QUERY = """
WITH message_starts AS (
    SELECT 
        event_time as start_time,
        request_params.space_id as space_id
    FROM system.access.audit
    WHERE service_name = 'aibiGenie'
      AND event_date >= current_timestamp() - INTERVAL {hours} HOUR
      AND action_name = 'genieStartConversationMessage'
      {audit_space_filter}
),
queries AS (
    SELECT 
        query_source.genie_space_id as space_id,
        statement_id,
        start_time as query_start,
        end_time as query_end,
        total_duration_ms,
        compilation_duration_ms,
        execution_duration_ms,
        waiting_at_capacity_duration_ms,
        waiting_for_compute_duration_ms
    FROM system.query.history
    WHERE query_source.genie_space_id IS NOT NULL
      AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
      {space_filter}
),
per_request AS (
    SELECT 
        ms.space_id,
        TIMESTAMPDIFF(SECOND, ms.start_time, MIN(q.query_start)) as ai_overhead_sec,
        SUM(q.compilation_duration_ms) / 1000.0 as compilation_sec,
        SUM(q.execution_duration_ms) / 1000.0 as execution_sec,
        SUM(q.waiting_at_capacity_duration_ms) / 1000.0 as queue_sec,
        SUM(q.waiting_for_compute_duration_ms) / 1000.0 as compute_startup_sec
    FROM message_starts ms
    JOIN queries q ON ms.space_id = q.space_id 
        AND q.query_start BETWEEN ms.start_time AND ms.start_time + INTERVAL 2 MINUTE
    GROUP BY ms.space_id, ms.start_time
)
SELECT 
    'AI Overhead' as phase,
    0 as phase_order,
    ROUND(COALESCE(SUM(ai_overhead_sec), 0) / 60.0, 2) as time_min,
    ROUND(COALESCE(AVG(ai_overhead_sec), 0), 2) as avg_sec
FROM per_request

UNION ALL

SELECT 
    'Queue Wait' as phase,
    1 as phase_order,
    ROUND(COALESCE(SUM(queue_sec), 0) / 60.0, 2) as time_min,
    ROUND(COALESCE(AVG(queue_sec), 0), 2) as avg_sec
FROM per_request

UNION ALL

SELECT 
    'Compute Startup' as phase,
    2 as phase_order,
    ROUND(COALESCE(SUM(compute_startup_sec), 0) / 60.0, 2) as time_min,
    ROUND(COALESCE(AVG(compute_startup_sec), 0), 2) as avg_sec
FROM per_request

UNION ALL

SELECT 
    'Compilation' as phase,
    3 as phase_order,
    ROUND(COALESCE(SUM(compilation_sec), 0) / 60.0, 2) as time_min,
    ROUND(COALESCE(AVG(compilation_sec), 0), 2) as avg_sec
FROM per_request

UNION ALL

SELECT 
    'Execution' as phase,
    4 as phase_order,
    ROUND(COALESCE(SUM(execution_sec), 0) / 60.0, 2) as time_min,
    ROUND(COALESCE(AVG(execution_sec), 0), 2) as avg_sec
FROM per_request

ORDER BY phase_order
"""

# Legacy query for backward compatibility (simple phase aggregation)
PHASE_BREAKDOWN_QUERY = """
SELECT 
  'Queue Wait' as phase,
  1 as phase_order,
  ROUND(COALESCE(SUM(waiting_at_capacity_duration_ms), 0) / 1000.0 / 60.0, 2) as time_min,
  ROUND(COALESCE(AVG(waiting_at_capacity_duration_ms), 0) / 1000.0, 2) as avg_sec
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  {space_filter}

UNION ALL

SELECT 
  'Compute Startup' as phase,
  2 as phase_order,
  ROUND(COALESCE(SUM(waiting_for_compute_duration_ms), 0) / 1000.0 / 60.0, 2) as time_min,
  ROUND(COALESCE(AVG(waiting_for_compute_duration_ms), 0) / 1000.0, 2) as avg_sec
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  {space_filter}

UNION ALL

SELECT 
  'Compilation' as phase,
  3 as phase_order,
  ROUND(COALESCE(SUM(compilation_duration_ms), 0) / 1000.0 / 60.0, 2) as time_min,
  ROUND(COALESCE(AVG(compilation_duration_ms), 0) / 1000.0, 2) as avg_sec
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  {space_filter}

UNION ALL

SELECT 
  'Execution' as phase,
  4 as phase_order,
  ROUND(COALESCE(SUM(execution_duration_ms), 0) / 1000.0 / 60.0, 2) as time_min,
  ROUND(COALESCE(AVG(execution_duration_ms), 0) / 1000.0, 2) as avg_sec
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  {space_filter}

ORDER BY phase_order
"""

# ============================================================================
# DURATION DISTRIBUTION
# ============================================================================

DURATION_HISTOGRAM_QUERY = """
SELECT
  CASE
    WHEN total_duration_ms < 1000 THEN '< 1s'
    WHEN total_duration_ms < 5000 THEN '1-5s'
    WHEN total_duration_ms < 10000 THEN '5-10s'
    WHEN total_duration_ms < 30000 THEN '10-30s'
    WHEN total_duration_ms < 60000 THEN '30-60s'
    ELSE '> 60s'
  END AS duration_bucket,
  CASE
    WHEN total_duration_ms < 1000 THEN 1
    WHEN total_duration_ms < 5000 THEN 2
    WHEN total_duration_ms < 10000 THEN 3
    WHEN total_duration_ms < 30000 THEN 4
    WHEN total_duration_ms < 60000 THEN 5
    ELSE 6
  END AS bucket_order,
  COUNT(*) AS query_count
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  {space_filter}
GROUP BY 1, 2
ORDER BY bucket_order
"""

# ============================================================================
# TREND ANALYSIS
# ============================================================================

DAILY_TREND_QUERY = """
SELECT
  DATE(start_time) AS query_date,
  COUNT(*) AS total_queries,
  SUM(CASE WHEN total_duration_ms >= 10000 THEN 1 ELSE 0 END) AS slow_queries,
  ROUND(AVG(total_duration_ms) / 1000.0, 2) AS avg_sec,
  ROUND(PERCENTILE(total_duration_ms, 0.90) / 1000.0, 2) AS p90_sec,
  ROUND(PERCENTILE(total_duration_ms, 0.95) / 1000.0, 2) AS p95_sec,
  ROUND(100.0 * SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) / COUNT(*), 1) AS success_rate
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  {space_filter}
GROUP BY DATE(start_time)
ORDER BY query_date
"""

HOURLY_PATTERN_QUERY = """
SELECT
  HOUR(start_time) AS hour_of_day,
  COUNT(*) AS query_count,
  ROUND(AVG(total_duration_ms) / 1000.0, 2) AS avg_sec,
  ROUND(PERCENTILE(total_duration_ms, 0.90) / 1000.0, 2) AS p90_sec,
  SUM(CASE WHEN total_duration_ms >= 10000 THEN 1 ELSE 0 END) AS slow_count
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  {space_filter}
GROUP BY HOUR(start_time)
ORDER BY hour_of_day
"""

# ============================================================================
# TIME BREAKDOWN BY PHASE
# ============================================================================

TIME_BREAKDOWN_QUERY = """
SELECT
  'Compilation' AS phase,
  ROUND(SUM(COALESCE(compilation_duration_ms, 0)) / 1000.0 / 60.0, 2) AS total_minutes,
  ROUND(AVG(COALESCE(compilation_duration_ms, 0)) / 1000.0, 2) AS avg_seconds
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL 
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  {space_filter}
UNION ALL
SELECT 'Execution', 
  ROUND(SUM(COALESCE(execution_duration_ms, 0)) / 1000.0 / 60.0, 2), 
  ROUND(AVG(COALESCE(execution_duration_ms, 0)) / 1000.0, 2)
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL 
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  {space_filter}
UNION ALL
SELECT 'Wait for Compute', 
  ROUND(SUM(COALESCE(waiting_for_compute_duration_ms, 0)) / 1000.0 / 60.0, 2), 
  ROUND(AVG(COALESCE(waiting_for_compute_duration_ms, 0)) / 1000.0, 2)
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL 
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  {space_filter}
UNION ALL
SELECT 'Queue Wait', 
  ROUND(SUM(COALESCE(waiting_at_capacity_duration_ms, 0)) / 1000.0 / 60.0, 2), 
  ROUND(AVG(COALESCE(waiting_at_capacity_duration_ms, 0)) / 1000.0, 2)
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL 
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  {space_filter}
UNION ALL
SELECT 'Result Fetch', 
  ROUND(SUM(COALESCE(result_fetch_duration_ms, 0)) / 1000.0 / 60.0, 2), 
  ROUND(AVG(COALESCE(result_fetch_duration_ms, 0)) / 1000.0, 2)
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL 
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  {space_filter}
"""

# ============================================================================
# QUERY LISTINGS
# ============================================================================

QUERIES_LIST_QUERY = """
WITH message_events AS (
  SELECT 
    event_time as message_time,
    request_id as api_request_id,
    request_params.space_id as space_id,
    request_params.conversation_id as conversation_id,
    -- message_id not available in standard audit schema, use request_id as fallback
    request_id as message_id,
    user_identity.email as user_email,
    action_name
  FROM system.access.audit
  WHERE service_name = 'aibiGenie'
    AND event_date >= current_timestamp() - INTERVAL {hours} HOUR
    AND action_name IN (
      'genieStartConversationMessage',
      'genieContinueConversationMessage',
      'genieCreateConversationMessage',
      'createConversationMessage',
      'regenerateConversationMessage'
    )
    {audit_space_filter}
),
queries AS (
  SELECT
    statement_id,
    query_source.genie_space_id AS genie_space_id,
    compute.warehouse_id AS warehouse_id,
    executed_by,
    start_time,
    end_time,
    ROUND(total_duration_ms / 1000.0, 2) AS total_sec,
    ROUND(COALESCE(compilation_duration_ms, 0) / 1000.0, 2) AS compile_sec,
    ROUND(COALESCE(execution_duration_ms, 0) / 1000.0, 2) AS execute_sec,
    ROUND(COALESCE(waiting_for_compute_duration_ms, 0) / 1000.0, 2) AS wait_compute_sec,
    ROUND(COALESCE(waiting_at_capacity_duration_ms, 0) / 1000.0, 2) AS queue_sec,
    ROUND(COALESCE(result_fetch_duration_ms, 0) / 1000.0, 2) AS fetch_sec,
    ROUND(COALESCE(read_bytes, 0) / 1024.0 / 1024.0, 2) AS read_mb,
    COALESCE(read_rows, 0) AS read_rows,
    COALESCE(produced_rows, 0) AS produced_rows,
    execution_status,
    total_duration_ms,
    COALESCE(compilation_duration_ms, 0) AS compilation_ms,
    COALESCE(execution_duration_ms, 0) AS execution_ms,
    COALESCE(waiting_for_compute_duration_ms, 0) AS compute_wait_ms,
    COALESCE(waiting_at_capacity_duration_ms, 0) AS queue_wait_ms,
    COALESCE(result_fetch_duration_ms, 0) AS result_fetch_ms,
    COALESCE(read_bytes, 0) AS bytes_scanned,
    CASE
      WHEN total_duration_ms >= 30000 THEN 'CRITICAL'
      WHEN total_duration_ms >= 10000 THEN 'SLOW'
      WHEN total_duration_ms >= 5000 THEN 'MODERATE'
      ELSE 'FAST'
    END AS speed_category,
    CASE
      WHEN COALESCE(waiting_for_compute_duration_ms, 0) > total_duration_ms * 0.5 THEN 'COMPUTE_STARTUP'
      WHEN COALESCE(waiting_at_capacity_duration_ms, 0) > total_duration_ms * 0.3 THEN 'QUEUE_WAIT'
      WHEN COALESCE(compilation_duration_ms, 0) > total_duration_ms * 0.4 THEN 'COMPILATION'
      WHEN COALESCE(read_bytes, 0) > 1073741824 THEN 'LARGE_SCAN'
      WHEN COALESCE(execution_duration_ms, 0) > 10000 THEN 'SLOW_EXECUTION'
      ELSE 'NORMAL'
    END AS bottleneck,
    LEFT(statement_text, 500) AS query_text
  FROM system.query.history
  WHERE query_source.genie_space_id IS NOT NULL
    AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
    {space_filter}
    {status_filter}
),
query_with_message AS (
  SELECT 
    q.*,
    m.message_time,
    m.api_request_id,
    m.conversation_id,
    m.message_id,
    m.action_name as message_action,
    ROW_NUMBER() OVER (PARTITION BY q.statement_id ORDER BY m.message_time DESC) as rn
  FROM queries q
  LEFT JOIN message_events m 
    ON q.genie_space_id = m.space_id
    AND m.user_email = q.executed_by
    AND m.message_time <= q.start_time
    AND m.message_time >= q.start_time - INTERVAL 5 MINUTE
)
SELECT 
  statement_id,
  genie_space_id,
  warehouse_id,
  executed_by,
  start_time,
  total_sec,
  compile_sec,
  execute_sec,
  wait_compute_sec,
  queue_sec,
  fetch_sec,
  read_mb,
  read_rows,
  produced_rows,
  execution_status,
  total_duration_ms,
  compilation_ms,
  execution_ms,
  compute_wait_ms,
  queue_wait_ms,
  result_fetch_ms,
  bytes_scanned,
  speed_category,
  bottleneck,
  query_text,
  api_request_id,
  conversation_id,
  message_id,
  ROUND(COALESCE(TIMESTAMPDIFF(SECOND, message_time, start_time), 0), 1) AS ai_overhead_sec
FROM query_with_message
WHERE rn = 1 OR rn IS NULL
ORDER BY total_duration_ms DESC
LIMIT {limit}
"""

SLOW_QUERIES_QUERY = """
SELECT
  statement_id,
  query_source.genie_space_id AS genie_space_id,
  executed_by,
  start_time,
  ROUND(total_duration_ms / 1000.0, 2) AS total_sec,
  ROUND(COALESCE(compilation_duration_ms, 0) / 1000.0, 2) AS compile_sec,
  ROUND(COALESCE(execution_duration_ms, 0) / 1000.0, 2) AS execute_sec,
  ROUND(COALESCE(waiting_for_compute_duration_ms, 0) / 1000.0, 2) AS wait_compute_sec,
  ROUND(COALESCE(waiting_at_capacity_duration_ms, 0) / 1000.0, 2) AS queue_sec,
  ROUND(COALESCE(read_bytes, 0) / 1024.0 / 1024.0, 2) AS read_mb,
  COALESCE(read_rows, 0) AS read_rows,
  COALESCE(read_files, 0) AS read_files,
  COALESCE(pruned_files, 0) AS pruned_files,
  CASE WHEN (COALESCE(read_files, 0) + COALESCE(pruned_files, 0)) > 0 
    THEN ROUND(100.0 * COALESCE(pruned_files, 0) / (read_files + pruned_files), 1) 
    ELSE 0 
  END AS prune_pct,
  CASE
    WHEN COALESCE(waiting_for_compute_duration_ms, 0) > total_duration_ms * 0.5 THEN 'COMPUTE_STARTUP'
    WHEN COALESCE(waiting_at_capacity_duration_ms, 0) > total_duration_ms * 0.3 THEN 'QUEUE_WAIT'
    WHEN COALESCE(compilation_duration_ms, 0) > total_duration_ms * 0.4 THEN 'COMPILATION'
    WHEN COALESCE(read_bytes, 0) > 1073741824 THEN 'LARGE_SCAN'
    WHEN COALESCE(execution_duration_ms, 0) > 10000 THEN 'SLOW_EXECUTION'
    ELSE 'OTHER'
  END AS bottleneck,
  CASE
    WHEN COALESCE(waiting_for_compute_duration_ms, 0) > total_duration_ms * 0.5 THEN 'Use always-on warehouse or reduce auto-suspend'
    WHEN COALESCE(waiting_at_capacity_duration_ms, 0) > total_duration_ms * 0.3 THEN 'Scale up warehouse size or add more clusters'
    WHEN COALESCE(compilation_duration_ms, 0) > total_duration_ms * 0.4 THEN 'Simplify query or break into smaller parts'
    WHEN COALESCE(read_bytes, 0) > 1073741824 THEN 'Add partition/filter predicates to reduce scan'
    WHEN COALESCE(execution_duration_ms, 0) > 10000 THEN 'Review query plan, add indexes or optimize joins'
    ELSE 'Check query plan for specific issues'
  END AS recommendation,
  execution_status,
  LEFT(statement_text, 500) AS query_text
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  AND total_duration_ms >= 10000
  {space_filter}
ORDER BY total_duration_ms DESC
LIMIT {limit}
"""

FAILED_QUERIES_QUERY = """
SELECT
  statement_id,
  query_source.genie_space_id AS genie_space_id,
  executed_by,
  start_time,
  execution_status,
  error_message,
  LEFT(statement_text, 500) AS query_text
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  AND execution_status IN ('FAILED', 'CANCELED')
  {space_filter}
ORDER BY start_time DESC
LIMIT {limit}
"""

# ============================================================================
# SINGLE QUERY DETAIL
# ============================================================================

QUERY_DETAIL_QUERY = """
SELECT
  statement_id,
  query_source.genie_space_id AS genie_space_id,
  executed_by,
  start_time,
  end_time,
  total_duration_ms,
  COALESCE(compilation_duration_ms, 0) AS compilation_ms,
  COALESCE(execution_duration_ms, 0) AS execution_ms,
  COALESCE(waiting_for_compute_duration_ms, 0) AS compute_wait_ms,
  COALESCE(waiting_at_capacity_duration_ms, 0) AS queue_wait_ms,
  COALESCE(result_fetch_duration_ms, 0) AS result_fetch_ms,
  COALESCE(read_rows, 0) AS rows_scanned,
  COALESCE(produced_rows, 0) AS rows_returned,
  COALESCE(read_bytes, 0) AS bytes_scanned,
  execution_status,
  error_message,
  statement_text AS query_text,
  compute.warehouse_id AS warehouse_id
FROM system.query.history
WHERE statement_id = '{statement_id}'
"""

# ============================================================================
# QUERY CONCURRENCY
# ============================================================================

QUERY_CONCURRENCY_QUERY = """
SELECT 
  -- Genie concurrency: queries in same Genie space overlapping with this query
  (SELECT COUNT(*) 
   FROM system.query.history h2 
   WHERE h2.query_source.genie_space_id = '{genie_space_id}'
     AND h2.start_time <= TIMESTAMP'{start_time}'
     AND h2.end_time >= TIMESTAMP'{start_time}'
     AND h2.statement_id != '{statement_id}'
  ) AS genie_concurrent,
  
  -- Warehouse concurrency: queries on same warehouse overlapping with this query  
  (SELECT COUNT(*) 
   FROM system.query.history h2 
   WHERE h2.compute.warehouse_id = '{warehouse_id}'
     AND h2.start_time <= TIMESTAMP'{start_time}'
     AND h2.end_time >= TIMESTAMP'{start_time}'
     AND h2.statement_id != '{statement_id}'
  ) AS warehouse_concurrent
"""

# ============================================================================
# USER STATISTICS
# ============================================================================

USER_STATS_QUERY = """
SELECT
  executed_by AS user_name,
  COUNT(*) AS query_count,
  ROUND(AVG(total_duration_ms) / 1000.0, 2) AS avg_sec,
  ROUND(PERCENTILE(total_duration_ms, 0.90) / 1000.0, 2) AS p90_sec,
  SUM(CASE WHEN total_duration_ms >= 10000 THEN 1 ELSE 0 END) AS slow_queries,
  ROUND(SUM(COALESCE(read_bytes, 0)) / 1024.0 / 1024.0 / 1024.0, 2) AS total_gb_read,
  ROUND(100.0 * SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) / COUNT(*), 1) AS success_rate
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  {space_filter}
GROUP BY executed_by
ORDER BY query_count DESC
"""

# ============================================================================
# IO HEAVY QUERIES
# ============================================================================

IO_HEAVY_QUERIES_QUERY = """
SELECT
  statement_id,
  query_source.genie_space_id AS genie_space_id,
  executed_by,
  start_time,
  ROUND(total_duration_ms / 1000.0, 2) AS total_sec,
  ROUND(COALESCE(read_bytes, 0) / 1024.0 / 1024.0 / 1024.0, 3) AS read_gb,
  COALESCE(read_rows, 0) AS rows_read,
  COALESCE(read_files, 0) AS files_read,
  COALESCE(pruned_files, 0) AS files_pruned,
  CASE WHEN (COALESCE(read_files, 0) + COALESCE(pruned_files, 0)) > 0 
    THEN ROUND(100.0 * COALESCE(pruned_files, 0) / (read_files + pruned_files), 1) 
    ELSE 0 
  END AS prune_pct,
  CASE
    WHEN (COALESCE(read_files, 0) + COALESCE(pruned_files, 0)) > 0 
      AND 100.0 * COALESCE(pruned_files, 0) / (read_files + pruned_files) < 30 
    THEN 'Poor pruning - add partition filters'
    WHEN COALESCE(read_bytes, 0) > 5368709120 THEN 'Large scan > 5GB - narrow query scope'
    WHEN COALESCE(read_files, 0) > 1000 THEN 'Many small files - run OPTIMIZE'
    ELSE 'OK'
  END AS io_recommendation
FROM system.query.history
WHERE query_source.genie_space_id IS NOT NULL
  AND start_time >= current_timestamp() - INTERVAL {hours} HOUR
  AND COALESCE(read_bytes, 0) > 104857600
  {space_filter}
ORDER BY read_bytes DESC
LIMIT {limit}
"""


def build_space_filter(space_id: str | None) -> str:
    """Build SQL filter for a specific space."""
    if space_id:
        return f"AND query_source.genie_space_id = '{space_id}'"
    return ""


def build_status_filter(status: str | None) -> str:
    """Build SQL filter for execution status."""
    if not status:
        return ""
    if status.lower() == "success":
        return "AND execution_status = 'FINISHED'"
    if status.lower() == "failed":
        return "AND execution_status = 'FAILED'"
    if status.lower() == "cancelled":
        return "AND execution_status IN ('CANCELED', 'CANCELLED')"
    return ""


# ============================================================================
# AI CONVERSATION ACTIVITY QUERIES (from system.access.audit)
# ============================================================================

CONVERSATION_ACTIVITY_QUERY = """
SELECT
  COUNT(*) AS message_count,
  date_trunc('hour', event_time) AS event_hour
FROM system.access.audit
WHERE service_name = 'aibiGenie'
  AND event_date >= now() - INTERVAL {hours} HOUR
  AND action_name IN (
    'genieCreateConversationMessage',
    'createConversationMessage',
    'genieStartConversationMessage',
    'regenerateConversationMessage'
  )
  {space_filter}
GROUP BY event_hour
ORDER BY event_hour
"""

CONVERSATION_DAILY_QUERY = """
SELECT
  COUNT(*) AS message_count,
  DATE(event_time) AS event_date,
  CASE action_name
    WHEN 'genieStartConversationMessage' THEN 'New Conversation'
    WHEN 'genieContinueConversationMessage' THEN 'Follow-up Message'
    WHEN 'genieCreateConversationMessage' THEN 'Message Created'
    WHEN 'createConversationMessage' THEN 'Message Created'
    WHEN 'regenerateConversationMessage' THEN 'Regenerate Response'
    ELSE 'Other'
  END AS message_type
FROM system.access.audit
WHERE service_name = 'aibiGenie'
  AND DATE(event_time) >= DATE(now() - INTERVAL {hours} HOUR)
  AND action_name IN (
    'genieCreateConversationMessage',
    'createConversationMessage',
    'genieStartConversationMessage',
    'genieContinueConversationMessage',
    'regenerateConversationMessage'
  )
  {space_filter}
GROUP BY DATE(event_time), message_type
ORDER BY event_date, message_type
"""

CONVERSATION_PEAK_QUERY = """
WITH messages_per_minute AS (
  SELECT
    COUNT(*) AS message_count,
    date_trunc('minute', event_time) AS event_minute
  FROM system.access.audit
  WHERE service_name = 'aibiGenie'
    AND DATE(event_time) >= DATE(now() - INTERVAL {hours} HOUR)
    AND action_name IN (
      'genieCreateConversationMessage',
      'createConversationMessage',
      'genieStartConversationMessage',
      'genieContinueConversationMessage',
      'regenerateConversationMessage'
    )
    {space_filter}
  GROUP BY event_minute
)
SELECT 
  MAX(message_count) AS peak_messages_per_minute,
  COUNT(*) AS total_minutes_with_activity,
  SUM(message_count) AS total_messages,
  ROUND(AVG(message_count), 2) AS avg_messages_per_minute
FROM messages_per_minute
"""

CONVERSATION_BY_ACTION_QUERY = """
SELECT
  action_name,
  COUNT(*) AS message_count
FROM system.access.audit
WHERE service_name = 'aibiGenie'
  AND event_date >= now() - INTERVAL {hours} HOUR
  AND action_name IN (
    'genieCreateConversationMessage',
    'createConversationMessage',
    'genieStartConversationMessage',
    'regenerateConversationMessage'
  )
  {space_filter}
GROUP BY action_name
ORDER BY message_count DESC
"""


def build_audit_space_filter(space_id: str | None) -> str:
    """Build SQL filter for space ID in audit logs."""
    if not space_id:
        return ""
    return f"AND request_params.space_id = '{space_id}'"


# AI Latency estimation queries - correlate message events with query execution
AI_LATENCY_METRICS_QUERY = """
WITH message_events AS (
  SELECT 
    event_time as message_time,
    request_params.space_id as space_id
  FROM system.access.audit
  WHERE service_name = 'aibiGenie'
    AND event_date >= now() - INTERVAL {hours} HOUR
    AND action_name IN ('genieStartConversationMessage', 'createConversationMessage')
    {space_filter}
),
queries AS (
  SELECT 
    query_source.genie_space_id as space_id,
    start_time as query_start
  FROM system.query.history
  WHERE query_source.genie_space_id IS NOT NULL
    AND start_time >= now() - INTERVAL {hours} HOUR
    {query_space_filter}
)
SELECT 
  COUNT(*) as message_query_pairs,
  ROUND(AVG(TIMESTAMPDIFF(SECOND, m.message_time, q.query_start)), 1) as avg_ai_latency_sec,
  ROUND(PERCENTILE(TIMESTAMPDIFF(SECOND, m.message_time, q.query_start), 0.5), 1) as p50_ai_latency_sec,
  ROUND(PERCENTILE(TIMESTAMPDIFF(SECOND, m.message_time, q.query_start), 0.9), 1) as p90_ai_latency_sec,
  ROUND(MIN(TIMESTAMPDIFF(SECOND, m.message_time, q.query_start)), 1) as min_ai_latency_sec,
  ROUND(MAX(TIMESTAMPDIFF(SECOND, m.message_time, q.query_start)), 1) as max_ai_latency_sec
FROM message_events m
JOIN queries q ON m.space_id = q.space_id
  AND q.query_start BETWEEN m.message_time AND m.message_time + INTERVAL 2 MINUTE
"""


AI_LATENCY_TREND_QUERY = """
WITH message_events AS (
  SELECT 
    event_time as message_time,
    DATE(event_time) as event_date,
    request_params.space_id as space_id
  FROM system.access.audit
  WHERE service_name = 'aibiGenie'
    AND event_date >= now() - INTERVAL {hours} HOUR
    AND action_name IN ('genieStartConversationMessage', 'createConversationMessage')
    {space_filter}
),
queries AS (
  SELECT 
    query_source.genie_space_id as space_id,
    start_time as query_start
  FROM system.query.history
  WHERE query_source.genie_space_id IS NOT NULL
    AND start_time >= now() - INTERVAL {hours} HOUR
    {query_space_filter}
)
SELECT 
  m.event_date,
  COUNT(*) as message_count,
  ROUND(AVG(TIMESTAMPDIFF(SECOND, m.message_time, q.query_start)), 1) as avg_ai_latency_sec,
  ROUND(PERCENTILE(TIMESTAMPDIFF(SECOND, m.message_time, q.query_start), 0.5), 1) as p50_ai_latency_sec,
  ROUND(PERCENTILE(TIMESTAMPDIFF(SECOND, m.message_time, q.query_start), 0.9), 1) as p90_ai_latency_sec
FROM message_events m
JOIN queries q ON m.space_id = q.space_id
  AND q.query_start BETWEEN m.message_time AND m.message_time + INTERVAL 2 MINUTE
GROUP BY m.event_date
ORDER BY m.event_date
"""


def build_query_space_filter(space_id: str | None) -> str:
    """Build SQL filter for space ID in query history."""
    if not space_id:
        return ""
    return f"AND query_source.genie_space_id = '{space_id}'"
