"""
PDF Report Generator for Genie Performance Audit

Generates comprehensive PDF reports with actionable insights for engineering teams.
"""

from datetime import datetime
from io import BytesIO
from typing import Optional

import pandas as pd
from fpdf import FPDF

from services.analytics import get_query_optimizations, get_diagnostic_queries, get_bottleneck_recommendation
from queries.sql import QUERY_HISTORY_TABLE, AUDIT_TABLE


class GenieAuditReport(FPDF):
    """Custom PDF class for Genie Audit reports."""
    
    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=15)
        
    def header(self):
        """Page header."""
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(100, 100, 100)
        self.cell(0, 10, "Genie Performance Audit Report", align="R")
        self.ln(5)
        self.set_draw_color(200, 200, 200)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(5)
        
    def footer(self):
        """Page footer."""
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}} | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}", align="C")
        
    def section_title(self, title: str):
        """Add a section title."""
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(30, 30, 30)
        self.ln(5)
        self.cell(0, 10, title, ln=True)
        self.set_draw_color(59, 130, 246)  # Blue line
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 60, self.get_y())
        self.ln(5)
        
    def subsection_title(self, title: str):
        """Add a subsection title."""
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(60, 60, 60)
        self.ln(3)
        self.cell(0, 8, title, ln=True)
        self.ln(2)
        
    def body_text(self, text: str):
        """Add body text."""
        self.set_font("Helvetica", "", 10)
        self.set_text_color(50, 50, 50)
        self.multi_cell(0, 5, text)
        self.ln(2)
        
    def metric_row(self, label: str, value: str, description: str = ""):
        """Add a metric row."""
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(80, 80, 80)
        self.cell(60, 6, label + ":")
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.cell(40, 6, str(value))
        if description:
            self.set_font("Helvetica", "I", 9)
            self.set_text_color(120, 120, 120)
            self.cell(0, 6, description)
        self.ln(6)
        
    def warning_box(self, text: str):
        """Add a warning/recommendation box."""
        self.set_fill_color(255, 243, 205)  # Light yellow
        self.set_draw_color(255, 193, 7)    # Yellow border
        self.set_text_color(133, 100, 4)
        self.set_font("Helvetica", "", 9)
        self.set_line_width(0.3)
        
        # Calculate height needed
        self.multi_cell(0, 5, text, border=1, fill=True)
        self.ln(3)
        
    def info_box(self, text: str):
        """Add an info box."""
        self.set_fill_color(219, 234, 254)  # Light blue
        self.set_draw_color(59, 130, 246)   # Blue border
        self.set_text_color(30, 64, 175)
        self.set_font("Helvetica", "", 9)
        self.set_line_width(0.3)
        
        self.multi_cell(0, 5, text, border=1, fill=True)
        self.ln(3)
        
    def code_block(self, code: str, max_lines: int = 0):
        """Add a code block. Set max_lines > 0 to truncate."""
        self.set_fill_color(245, 245, 245)
        self.set_text_color(50, 50, 50)
        self.set_font("Courier", "", 8)
        
        # Only truncate if max_lines is set and exceeded
        lines = code.split('\n')
        if max_lines > 0 and len(lines) > max_lines:
            truncated = '\n'.join(lines[:max_lines])
            truncated += f'\n... ({len(lines) - max_lines} more lines)'
        else:
            truncated = code
            
        self.multi_cell(0, 4, truncated, border=1, fill=True)
        self.ln(3)


def _format_hours_to_period(hours: float) -> str:
    """Convert hours to human-readable period."""
    if hours < 1:
        return f"{int(hours * 60)} minutes"
    elif hours < 24:
        return f"{int(hours)} hours"
    else:
        days = int(hours / 24)
        return f"{days} days"


def _get_top_bottleneck(queries_df: pd.DataFrame) -> tuple[str, int]:
    """Get the most common bottleneck type and count."""
    if queries_df.empty or "bottleneck" not in queries_df.columns:
        return "N/A", 0
    
    bottleneck_counts = queries_df["bottleneck"].value_counts()
    if bottleneck_counts.empty:
        return "N/A", 0
    
    top = bottleneck_counts.idxmax()
    count = bottleneck_counts.max()
    return top, count


def generate_pdf_report(
    room_name: str,
    room_id: str,
    hours: float,
    metrics: dict,
    queries_df: pd.DataFrame,
    phase_df: pd.DataFrame,
    conversation_peak: Optional[dict] = None,
) -> bytes:
    """
    Generate a comprehensive PDF report for engineering teams.
    
    Args:
        room_name: Display name of the Genie room
        room_id: Genie space ID
        hours: Time period in hours
        metrics: Room metrics dictionary
        queries_df: DataFrame with query data
        phase_df: DataFrame with phase breakdown data
        conversation_peak: Optional conversation metrics
        
    Returns:
        PDF file as bytes
    """
    pdf = GenieAuditReport()
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # =========================================================================
    # TITLE PAGE / EXECUTIVE SUMMARY
    # =========================================================================
    pdf.set_font("Helvetica", "B", 24)
    pdf.set_text_color(30, 30, 30)
    pdf.ln(20)
    pdf.cell(0, 15, "Genie Performance Audit", ln=True, align="C")
    
    pdf.set_font("Helvetica", "", 14)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 10, f"Room: {room_name}", ln=True, align="C")
    pdf.ln(10)
    
    # Report metadata
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 6, f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", ln=True, align="C")
    pdf.cell(0, 6, f"Time Period: {_format_hours_to_period(hours)}", ln=True, align="C")
    pdf.cell(0, 6, f"Space ID: {room_id}", ln=True, align="C")
    pdf.ln(15)
    
    # Executive Summary Box
    total_queries = int(float(metrics.get("total_queries", 0) or 0))
    success_rate = float(metrics.get("success_rate_pct", 0) or 0)
    avg_duration = float(metrics.get("avg_duration_sec", 0) or 0)
    slow_queries = int(float(metrics.get("slow_10s", 0) or 0))
    unique_users = int(float(metrics.get("unique_users", 0) or 0))
    top_bottleneck, bottleneck_count = _get_top_bottleneck(queries_df)
    
    summary_text = (
        f"EXECUTIVE SUMMARY\n\n"
        f"Total Queries: {total_queries:,}\n"
        f"Success Rate: {success_rate:.1f}%\n"
        f"Avg Duration: {avg_duration:.2f}s\n"
        f"Slow Queries (>10s): {slow_queries}\n"
        f"Unique Users: {unique_users}\n"
        f"Top Bottleneck: {top_bottleneck} ({bottleneck_count} queries)"
    )
    pdf.info_box(summary_text)
    
    # =========================================================================
    # SECTION 1: PERFORMANCE METRICS
    # =========================================================================
    pdf.add_page()
    pdf.section_title("1. Performance Metrics")
    
    pdf.subsection_title("Query Volume & Latency")
    pdf.metric_row("Total Queries", f"{total_queries:,}")
    pdf.metric_row("Unique Users", str(metrics.get("unique_users", "N/A")))
    pdf.metric_row("Success Rate", f"{success_rate:.1f}%")
    pdf.metric_row("Average Duration", f"{avg_duration:.2f}s")
    pdf.metric_row("P50 Latency", f"{float(metrics.get('p50_sec', 0) or 0):.2f}s")
    pdf.metric_row("P90 Latency", f"{float(metrics.get('p90_sec', 0) or 0):.2f}s")
    pdf.metric_row("P95 Latency", f"{float(metrics.get('p95_sec', 0) or 0):.2f}s")
    pdf.ln(5)
    
    pdf.subsection_title("Slow Query Breakdown")
    pdf.metric_row("Queries > 10 seconds", str(slow_queries))
    pdf.metric_row("Queries > 30 seconds", str(metrics.get("slow_30s", 0)))
    pdf.metric_row("Failed Queries", str(metrics.get("failed_queries", 0)))
    
    # =========================================================================
    # SECTION 2: RESPONSE TIME BREAKDOWN
    # =========================================================================
    pdf.add_page()
    pdf.section_title("2. Response Time Breakdown")
    
    pdf.body_text(
        "This section shows where time is spent from when a user asks a question "
        "to when they receive an answer. Understanding this breakdown helps identify "
        "optimization opportunities."
    )
    pdf.ln(5)
    
    if not phase_df.empty:
        total_time = phase_df["time_min"].sum()
        
        pdf.subsection_title("Phase Distribution")
        for _, row in phase_df.iterrows():
            phase = row.get("phase", "Unknown")
            time_min = float(row.get("time_min", 0))
            pct = float(row.get("pct", 0))
            avg_sec = float(row.get("avg_sec", 0))
            
            pdf.metric_row(
                phase, 
                f"{time_min:.1f} min ({pct:.0f}%)",
                f"Avg: {avg_sec:.2f}s per query"
            )
        
        pdf.ln(5)
        pdf.metric_row("Total Time Analyzed", f"{total_time:.1f} minutes")
        
        # AI vs SQL split
        ai_time = phase_df[phase_df["phase"] == "AI Overhead"]["time_min"].sum()
        sql_time = total_time - ai_time
        ai_pct = (ai_time / total_time * 100) if total_time > 0 else 0
        
        pdf.ln(5)
        pdf.subsection_title("AI vs SQL Execution")
        pdf.metric_row("AI Processing", f"{ai_time:.1f} min ({ai_pct:.0f}%)")
        pdf.metric_row("SQL Execution", f"{sql_time:.1f} min ({100-ai_pct:.0f}%)")
    else:
        pdf.body_text("No phase breakdown data available for this time period.")
    
    # =========================================================================
    # SECTION 3: BOTTLENECK ANALYSIS
    # =========================================================================
    pdf.add_page()
    pdf.section_title("3. Bottleneck Analysis")
    
    pdf.body_text(
        "Queries are classified by their primary bottleneck to help prioritize "
        "optimization efforts. Focus on categories with the highest time impact."
    )
    pdf.ln(5)
    
    if not queries_df.empty and "bottleneck" in queries_df.columns:
        # Ensure total_sec is numeric for aggregation
        agg_df = queries_df.copy()
        agg_df["total_sec"] = pd.to_numeric(agg_df["total_sec"], errors="coerce").fillna(0)
        
        bottleneck_stats = agg_df.groupby("bottleneck").agg({
            "statement_id": "count",
            "total_sec": ["sum", "mean"]
        }).reset_index()
        
        bottleneck_stats.columns = ["bottleneck", "count", "total_time", "avg_time"]
        bottleneck_stats = bottleneck_stats.sort_values("total_time", ascending=False)
        
        pdf.subsection_title("Bottleneck Distribution (by time impact)")
        
        for _, row in bottleneck_stats.iterrows():
            bn = row["bottleneck"]
            count = int(row["count"])
            total_time = float(row["total_time"])
            avg_time = float(row["avg_time"])
            
            pdf.metric_row(
                bn,
                f"{count} queries",
                f"Total: {total_time:.0f}s, Avg: {avg_time:.1f}s"
            )
        
        pdf.ln(5)
        
        # Recommendations per bottleneck
        pdf.subsection_title("Bottleneck Recommendations")
        for bn in bottleneck_stats["bottleneck"].unique():
            if bn != "NORMAL":
                rec = get_bottleneck_recommendation(bn)
                pdf.set_font("Helvetica", "B", 9)
                pdf.cell(0, 5, f"{bn}:", ln=True)
                pdf.set_font("Helvetica", "", 9)
                pdf.multi_cell(0, 4, f"  {rec}")
                pdf.ln(2)
    else:
        pdf.body_text("No bottleneck data available.")
    
    # =========================================================================
    # SECTION 4: TOP SLOW QUERIES
    # =========================================================================
    pdf.add_page()
    pdf.section_title("4. Top 20 Slowest Queries")
    
    pdf.body_text(
        "These are the slowest queries in the analyzed period. Each query includes "
        "timing breakdown and specific recommendations."
    )
    pdf.ln(5)
    
    if not queries_df.empty:
        # Ensure total_sec is numeric for sorting
        sort_df = queries_df.copy()
        if "total_sec" in sort_df.columns:
            sort_df["total_sec"] = pd.to_numeric(sort_df["total_sec"], errors="coerce").fillna(0)
            top_queries = sort_df.nlargest(20, "total_sec")
        else:
            top_queries = sort_df.head(20)
        
        for idx, (_, query) in enumerate(top_queries.iterrows(), 1):
            statement_id = query.get("statement_id", "N/A")
            total_sec = float(query.get("total_sec", 0) or 0)
            bottleneck = query.get("bottleneck", "NORMAL")
            ai_overhead = float(query.get("ai_overhead_sec", 0) or 0)
            compile_sec = float(query.get("compile_sec", 0) or 0)
            execute_sec = float(query.get("execute_sec", 0) or 0)
            queue_sec = float(query.get("queue_sec", 0) or 0)
            
            pdf.subsection_title(f"Query #{idx} - {total_sec:.1f}s ({bottleneck})")
            pdf.set_font("Courier", "", 8)
            pdf.set_text_color(100, 100, 100)
            pdf.cell(0, 4, f"Statement ID: {statement_id}", ln=True)
            pdf.ln(2)
            
            pdf.set_font("Helvetica", "", 9)
            pdf.set_text_color(50, 50, 50)
            pdf.cell(47, 5, f"AI: {ai_overhead:.2f}s")
            pdf.cell(47, 5, f"Compile: {compile_sec:.2f}s")
            pdf.cell(47, 5, f"Execute: {execute_sec:.2f}s")
            pdf.cell(47, 5, f"Queue: {queue_sec:.2f}s")
            pdf.ln(6)
            
            # Query text (full, not truncated)
            query_text = query.get("query_text", "")
            if query_text:
                pdf.set_font("Courier", "", 7)
                pdf.set_text_color(80, 80, 80)
                pdf.multi_cell(0, 3, query_text)
                pdf.ln(2)
            
            # Recommendation
            rec = get_bottleneck_recommendation(bottleneck)
            pdf.set_font("Helvetica", "I", 8)
            pdf.set_text_color(59, 130, 246)
            pdf.multi_cell(0, 4, f"Recommendation: {rec}")
            pdf.ln(3)
            
            # Add page break every 5 queries
            if idx % 5 == 0 and idx < 20:
                pdf.add_page()
    else:
        pdf.body_text("No query data available.")
    
    # =========================================================================
    # SECTION 5: DIAGNOSTIC QUERIES
    # =========================================================================
    pdf.add_page()
    pdf.section_title("5. Diagnostic SQL Queries")
    
    pdf.body_text(
        "Copy and paste these queries into Databricks SQL Editor to investigate "
        "performance issues further."
    )
    pdf.ln(5)
    
    # Get diagnostic queries for the top bottleneck
    if not queries_df.empty:
        sample_query = queries_df.iloc[0].to_dict() if len(queries_df) > 0 else {}
        sample_query["genie_space_id"] = room_id
        
        diag_queries = get_diagnostic_queries(sample_query)
        
        for diag in diag_queries:  # Include all diagnostic queries
            pdf.subsection_title(diag.title)
            pdf.set_font("Helvetica", "I", 9)
            pdf.set_text_color(100, 100, 100)
            pdf.multi_cell(0, 4, diag.description)
            pdf.ln(2)
            pdf.code_block(diag.sql)
            pdf.ln(3)
    
    # =========================================================================
    # SECTION 6: ACTION ITEMS
    # =========================================================================
    pdf.add_page()
    pdf.section_title("6. Recommended Action Items")
    
    pdf.body_text(
        "Based on the analysis, here are prioritized action items for your engineering team:"
    )
    pdf.ln(5)
    
    # Generate action items based on metrics
    action_items = []
    
    if slow_queries > 10:
        action_items.append(
            "HIGH PRIORITY: Review and optimize the slow queries identified in Section 4. "
            "Focus on queries with >10s execution time."
        )
    
    if success_rate < 95:
        action_items.append(
            f"MEDIUM PRIORITY: Investigate failed queries. Current success rate is {success_rate:.1f}%, "
            "target should be >99%."
        )
    
    if not phase_df.empty:
        ai_pct = 0
        if phase_df["time_min"].sum() > 0:
            ai_time = phase_df[phase_df["phase"] == "AI Overhead"]["time_min"].sum()
            ai_pct = (ai_time / phase_df["time_min"].sum()) * 100
        
        if ai_pct > 30:
            action_items.append(
                f"MEDIUM PRIORITY: AI processing accounts for {ai_pct:.0f}% of response time. "
                "Consider improving Genie instructions and reducing table count in the space."
            )
    
    if top_bottleneck == "COMPUTE_STARTUP":
        action_items.append(
            "HIGH PRIORITY: Compute startup is the top bottleneck. "
            "Switch to Serverless SQL Warehouse or increase auto-suspend timeout."
        )
    elif top_bottleneck == "QUEUE_WAIT":
        action_items.append(
            "HIGH PRIORITY: Queue wait is the top bottleneck. "
            "Scale up warehouse size or enable auto-scaling with more clusters."
        )
    elif top_bottleneck == "LARGE_SCAN":
        action_items.append(
            "MEDIUM PRIORITY: Large data scans are impacting performance. "
            "Add partition filters and ensure Genie instructions mention partition columns."
        )
    
    action_items.append(
        "ONGOING: Schedule regular runs of ANALYZE TABLE and OPTIMIZE on frequently queried tables."
    )
    
    for i, item in enumerate(action_items, 1):
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(10, 6, f"{i}.")
        pdf.set_font("Helvetica", "", 10)
        pdf.multi_cell(0, 5, item)
        pdf.ln(3)
    
    # =========================================================================
    # OUTPUT
    # =========================================================================
    output = BytesIO()
    pdf.output(output)
    return output.getvalue()


def generate_query_pdf_report(
    query: dict,
    room_name: str,
    room_id: str,
    phase_df: pd.DataFrame,
    genie_concurrent: int = 0,
    warehouse_concurrent: int = 0,
    user_prompt: Optional[str] = None,
) -> bytes:
    """
    Generate a detailed PDF report for a single query with actionable insights.
    
    Args:
        query: Dictionary with query data
        room_name: Display name of the Genie room
        room_id: Genie space ID
        phase_df: DataFrame with phase breakdown data for this query
        genie_concurrent: Number of concurrent Genie queries at submission
        warehouse_concurrent: Number of concurrent warehouse queries at submission
        user_prompt: The original user question/prompt that generated this query
        
    Returns:
        PDF file as bytes
    """
    pdf = GenieAuditReport()
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # Extract query data
    statement_id = query.get("statement_id", "N/A")
    api_request_id = query.get("api_request_id", "") or "N/A"
    conversation_id = query.get("conversation_id", "") or "N/A"
    total_sec = float(query.get("total_sec", 0) or 0)
    compile_sec = float(query.get("compile_sec", 0) or 0)
    execute_sec = float(query.get("execute_sec", 0) or 0)
    queue_sec = float(query.get("queue_sec", 0) or 0)
    wait_compute_sec = float(query.get("wait_compute_sec", 0) or 0)
    ai_overhead_sec = float(query.get("ai_overhead_sec", 0) or 0)
    read_rows = int(float(query.get("read_rows", 0) or 0))
    read_mb = float(query.get("read_mb", 0) or 0)
    bottleneck = query.get("bottleneck", "NORMAL")
    execution_status = query.get("execution_status", "UNKNOWN")
    query_text = query.get("query_text", "")
    executed_by = query.get("executed_by", "Unknown")
    start_time = query.get("start_time", "")
    
    # =========================================================================
    # TITLE PAGE
    # =========================================================================
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(30, 30, 30)
    pdf.ln(15)
    pdf.cell(0, 12, "Query Performance Analysis", ln=True, align="C")
    
    pdf.set_font("Helvetica", "", 12)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 8, f"Genie Room: {room_name}", ln=True, align="C")
    pdf.ln(8)
    
    # Report metadata
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 6, f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", ln=True, align="C")
    pdf.cell(0, 6, f"Statement ID: {statement_id}", ln=True, align="C")
    pdf.ln(10)
    
    # Summary box - include new metrics
    bottleneck_label = bottleneck.replace("_", " ").title()
    total_response_time = ai_overhead_sec + total_sec  # Full end-to-end time
    summary_text = (
        f"QUERY SUMMARY\n\n"
        f"Total Response Time: {total_response_time:.2f}s\n"
        f"  - AI Processing: {ai_overhead_sec:.2f}s\n"
        f"  - SQL Execution: {total_sec:.2f}s\n"
        f"Primary Bottleneck: {bottleneck_label}\n"
        f"Status: {execution_status}\n"
        f"User: {executed_by}\n"
        f"Executed: {start_time}"
    )
    pdf.info_box(summary_text)
    
    # Add user prompt on title page if available
    if user_prompt:
        pdf.ln(5)
        pdf.set_font("Helvetica", "B", 11)
        pdf.set_text_color(60, 60, 60)
        pdf.cell(0, 8, "User Question:", ln=True)
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(30, 30, 30)
        # Truncate prompt for title page, full version in details
        prompt_preview = user_prompt[:200] + "..." if len(user_prompt) > 200 else user_prompt
        pdf.multi_cell(0, 6, prompt_preview)
    
    # =========================================================================
    # SECTION 1: QUERY IDENTIFIERS
    # =========================================================================
    pdf.add_page()
    pdf.section_title("1. Query Identifiers")
    
    pdf.metric_row("Statement ID", statement_id)
    pdf.metric_row("API Request ID", api_request_id)
    pdf.metric_row("Conversation ID", conversation_id)
    pdf.metric_row("Genie Space ID", room_id)
    pdf.metric_row("Executed By", executed_by)
    pdf.metric_row("Start Time", str(start_time))
    pdf.metric_row("Status", execution_status)
    
    # User prompt section (if available)
    if user_prompt:
        pdf.ln(5)
        pdf.subsection_title("User Question")
        pdf.body_text("The natural language question that generated this SQL query:")
        pdf.ln(2)
        pdf.info_box(user_prompt)
    
    # =========================================================================
    # SECTION 2: TIMING BREAKDOWN
    # =========================================================================
    pdf.ln(5)
    pdf.section_title("2. Timing Breakdown")
    
    pdf.body_text(
        "This section shows exactly where time was spent during query execution. "
        "Use this information to identify optimization opportunities."
    )
    pdf.ln(3)
    
    pdf.subsection_title("Response Time Phases")
    total_response_time = ai_overhead_sec + total_sec
    pdf.metric_row("TOTAL RESPONSE TIME", f"{total_response_time:.2f}s", "Complete end-to-end time from question to answer")
    pdf.ln(2)
    pdf.metric_row("AI Processing (Genie)", f"{ai_overhead_sec:.2f}s", "Time from message to SQL generation")
    pdf.metric_row("Queue Wait", f"{queue_sec:.2f}s", "Waiting for warehouse capacity")
    pdf.metric_row("Compute Startup", f"{wait_compute_sec:.2f}s", "Waiting for compute resources")
    pdf.metric_row("Compilation", f"{compile_sec:.2f}s", "Query parsing and optimization")
    pdf.metric_row("Execution", f"{execute_sec:.2f}s", "Actual data processing")
    pdf.metric_row("SQL Duration (subtotal)", f"{total_sec:.2f}s", "Total time in SQL warehouse")
    
    pdf.ln(5)
    pdf.subsection_title("Data Metrics")
    pdf.metric_row("Rows Scanned", f"{read_rows:,}")
    pdf.metric_row("Data Read", f"{read_mb:.2f} MB")
    
    # =========================================================================
    # SECTION 3: CONCURRENCY ANALYSIS
    # =========================================================================
    pdf.ln(5)
    pdf.section_title("3. Concurrency at Submission Time")
    
    pdf.body_text(
        "These metrics show the system load when this query was submitted. "
        "High concurrency can contribute to queue wait times."
    )
    pdf.ln(3)
    
    pdf.metric_row("Concurrent Genie Queries", str(genie_concurrent), "Other queries in this Genie space")
    pdf.metric_row("Concurrent Warehouse Queries", str(warehouse_concurrent), "Other queries on the same warehouse")
    
    if warehouse_concurrent > 15:
        pdf.warning_box(
            f"HIGH CONCURRENCY: {warehouse_concurrent} other queries were running on the warehouse "
            "when this query was submitted. This likely contributed to queue wait time."
        )
    
    # =========================================================================
    # SECTION 4: BOTTLENECK ANALYSIS
    # =========================================================================
    pdf.add_page()
    pdf.section_title("4. Bottleneck Analysis")
    
    pdf.subsection_title("Identified Bottleneck")
    bottleneck_color = {
        "COMPUTE_STARTUP": "#f59e0b",
        "QUEUE_WAIT": "#ef4444",
        "COMPILATION": "#8b5cf6",
        "LARGE_SCAN": "#3b82f6",
        "SLOW_EXECUTION": "#ec4899",
        "NORMAL": "#22c55e",
    }.get(bottleneck, "#888888")
    
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 10, f"Primary Bottleneck: {bottleneck_label}", ln=True)
    pdf.ln(3)
    
    # Recommendation
    rec = get_bottleneck_recommendation(bottleneck)
    pdf.subsection_title("Recommendation")
    pdf.body_text(rec)
    
    # Detailed analysis based on bottleneck type
    pdf.ln(5)
    pdf.subsection_title("Detailed Analysis")
    
    if bottleneck == "COMPUTE_STARTUP":
        pdf.body_text(
            f"This query spent {wait_compute_sec:.1f}s waiting for compute resources to start. "
            "This typically occurs when the SQL Warehouse was in a suspended state.\n\n"
            "ACTIONS:\n"
            "1. Switch to Serverless SQL Warehouse for instant startup\n"
            "2. Increase auto-suspend timeout (e.g., from 10 to 30 minutes)\n"
            "3. Consider using an always-on warehouse for frequently used Genie spaces"
        )
    elif bottleneck == "QUEUE_WAIT":
        pdf.body_text(
            f"This query spent {queue_sec:.1f}s waiting in queue for capacity. "
            f"There were {warehouse_concurrent} other queries running on the warehouse.\n\n"
            "ACTIONS:\n"
            "1. Scale up the warehouse size (e.g., Small to Medium)\n"
            "2. Enable auto-scaling with min/max clusters\n"
            "3. Consider dedicated warehouses for high-priority Genie spaces"
        )
    elif bottleneck == "COMPILATION":
        pdf.body_text(
            f"This query spent {compile_sec:.1f}s in compilation, which is unusually high. "
            "This can happen with complex queries or when table metadata needs to be refreshed.\n\n"
            "ACTIONS:\n"
            "1. Run ANALYZE TABLE on frequently queried tables\n"
            "2. Simplify complex JOINs or break into smaller queries\n"
            "3. Check if Genie is generating overly complex SQL"
        )
    elif bottleneck == "LARGE_SCAN":
        pdf.body_text(
            f"This query scanned {read_mb:.1f} MB of data ({read_rows:,} rows). "
            "Large scans can be reduced with better filtering.\n\n"
            "ACTIONS:\n"
            "1. Add partition columns to Genie space instructions\n"
            "2. Ensure tables are partitioned by commonly filtered columns (e.g., date)\n"
            "3. Run OPTIMIZE and ZORDER on frequently queried tables"
        )
    elif bottleneck == "SLOW_EXECUTION":
        pdf.body_text(
            f"The actual execution took {execute_sec:.1f}s. "
            "This may indicate inefficient query patterns or missing optimizations.\n\n"
            "ACTIONS:\n"
            "1. Review the query plan for full table scans\n"
            "2. Add appropriate indexes or clustering keys\n"
            "3. Consider materializing frequently accessed aggregations"
        )
    else:
        pdf.body_text(
            "This query performed within normal parameters. No specific bottleneck was identified."
        )
    
    # =========================================================================
    # SECTION 5: SQL QUERY
    # =========================================================================
    pdf.add_page()
    pdf.section_title("5. SQL Query")
    
    pdf.body_text("The following SQL was generated by Genie and executed:")
    pdf.ln(3)
    
    if query_text:
        pdf.code_block(query_text)  # Full query text without truncation
    else:
        pdf.body_text("(Query text not available)")
    
    # =========================================================================
    # SECTION 6: DIAGNOSTIC QUERIES
    # =========================================================================
    pdf.add_page()
    pdf.section_title("6. Follow-Up Diagnostic Queries")
    
    pdf.body_text(
        "Copy and paste these queries into Databricks SQL Editor to investigate further."
    )
    pdf.ln(3)
    
    # Query history lookup
    pdf.subsection_title("Query Details from History")
    pdf.code_block(f"""SELECT *
FROM {QUERY_HISTORY_TABLE}
WHERE statement_id = '{statement_id}'""")
    
    # Query profile
    pdf.subsection_title("Query Profile (if available)")
    pdf.code_block(f"""-- View query profile in Databricks SQL
-- Navigate to: SQL Editor > Query History
-- Search for Statement ID: {statement_id}
-- Click to view execution plan and metrics""")
    
    # Related queries
    pdf.subsection_title("Related Queries in Time Window")
    pdf.code_block(f"""SELECT 
  statement_id,
  executed_by,
  total_duration_ms / 1000.0 as duration_sec,
  execution_status,
  LEFT(statement_text, 100) as query_preview
FROM {QUERY_HISTORY_TABLE}
WHERE query_source.genie_space_id = '{room_id}'
  AND start_time BETWEEN 
    TIMESTAMP'{start_time}' - INTERVAL 5 MINUTE
    AND TIMESTAMP'{start_time}' + INTERVAL 5 MINUTE
ORDER BY start_time""")
    
    # Correlation query
    pdf.add_page()
    pdf.subsection_title("Correlate SQL with Genie API Events")
    pdf.body_text(
        "This query joins SQL execution data with Genie API events to show the "
        "full request lifecycle and calculate AI processing overhead."
    )
    pdf.ln(2)
    pdf.code_block(f"""-- Correlate this SQL query with Genie API audit events
-- Shows: API request ID, message time, AI processing time, SQL execution

WITH genie_message AS (
  SELECT 
    request_id AS api_request_id,
    request_params.conversation_id,
    event_time AS message_time,
    action_name
  FROM {AUDIT_TABLE}
  WHERE service_name = 'genieV2'
    AND action_name LIKE 'genie%Message'
    AND request_params.space_id = '{room_id}'
    AND event_time BETWEEN 
        TIMESTAMP'{start_time}' - INTERVAL 60 SECOND
        AND TIMESTAMP'{start_time}'
  ORDER BY event_time DESC
  LIMIT 1
),
sql_query AS (
  SELECT 
    statement_id,
    start_time,
    end_time,
    total_duration_ms,
    execution_status
  FROM {QUERY_HISTORY_TABLE}
  WHERE statement_id = '{statement_id}'
)
SELECT 
  m.api_request_id,
  m.conversation_id,
  m.message_time,
  m.action_name AS genie_action,
  q.statement_id,
  q.start_time AS sql_start,
  ROUND(UNIX_TIMESTAMP(q.start_time) - 
        UNIX_TIMESTAMP(m.message_time), 1) AS ai_overhead_sec,
  ROUND(q.total_duration_ms / 1000.0, 1) AS sql_duration_sec,
  q.execution_status
FROM sql_query q
CROSS JOIN genie_message m""")
    
    # =========================================================================
    # SECTION 7: ACTION ITEMS
    # =========================================================================
    pdf.add_page()
    pdf.section_title("7. Recommended Actions")
    
    # Generate prioritized action items based on the query
    action_items = []
    
    if bottleneck != "NORMAL":
        action_items.append(f"HIGH PRIORITY: Address {bottleneck_label} bottleneck - {rec}")
    
    if warehouse_concurrent > 10:
        action_items.append(
            f"MEDIUM PRIORITY: High warehouse concurrency ({warehouse_concurrent} queries). "
            "Consider scaling the warehouse or spreading load."
        )
    
    if read_mb > 100:
        action_items.append(
            f"MEDIUM PRIORITY: Query scanned {read_mb:.0f} MB. Review partition usage and filters."
        )
    
    if total_sec > 30:
        action_items.append(
            "HIGH PRIORITY: Query exceeded 30 second threshold. User experience is impacted."
        )
    
    if ai_overhead_sec > 5:
        action_items.append(
            f"LOW PRIORITY: AI processing took {ai_overhead_sec:.1f}s. "
            "Consider simplifying Genie space instructions."
        )
    
    if not action_items:
        action_items.append("No critical issues identified. Query performed within acceptable parameters.")
    
    for i, item in enumerate(action_items, 1):
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(10, 6, f"{i}.")
        pdf.set_font("Helvetica", "", 10)
        pdf.multi_cell(0, 5, item)
        pdf.ln(3)
    
    # =========================================================================
    # OUTPUT
    # =========================================================================
    output = BytesIO()
    pdf.output(output)
    return output.getvalue()
