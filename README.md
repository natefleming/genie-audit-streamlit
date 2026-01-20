# Genie Performance Audit

A Streamlit application for analyzing Databricks Genie query performance, identifying problematic queries, and providing optimization recommendations.

## Features

- **Genie Room Overview**: Browse all Genie rooms with tile-based navigation showing query counts, average duration, and health indicators
- **Room Insights**: Detailed performance analysis including:
  - Key metrics (total queries, avg duration, slow queries, success rate)
  - Daily trend charts (volume, slow queries, P90 latency)
  - Duration distribution histogram
  - Bottleneck type analysis
  - Time breakdown by query phase
  - Hourly query volume patterns
  - Interactive query table with drill-down
- **Query Detail**: Deep-dive into individual queries with:
  - Execution timeline visualization
  - Full SQL text with copy functionality
  - Performance metrics (rows scanned, data read, selectivity)
  - Automated optimization recommendations

## Prerequisites

- Python 3.10+
- Databricks workspace with access to `system.query.history`
- Databricks CLI configured (for deployment)

## Local Development

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set environment variables:
   ```bash
   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
   export DATABRICKS_TOKEN="your-token"
   export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
   ```

3. Run locally:
   ```bash
   streamlit run app.py
   ```

## Deployment to Databricks Apps

Deploy using Databricks Asset Bundles:

```bash
# Validate the bundle
databricks bundle validate

# Deploy to Databricks
databricks bundle deploy

# Run the app
databricks bundle run genie-audit
```

## Project Structure

```
genie-audit-streamlit/
├── app.py                     # Main Streamlit application
├── app.yaml                   # Databricks Apps configuration
├── databricks.yml             # Databricks Asset Bundle config
├── requirements.txt           # Python dependencies
├── .streamlit/
│   └── config.toml            # Streamlit configuration
├── pages/
│   ├── 1_Room_Insights.py     # Room-specific analysis page
│   └── 2_Query_Detail.py      # Individual query detail page
├── services/
│   ├── databricks_client.py   # WorkspaceClient wrapper
│   └── analytics.py           # Query analysis and recommendations
├── components/
│   ├── charts.py              # Plotly chart components
│   ├── metrics.py             # Metric card components
│   └── tiles.py               # Room tile grid component
├── queries/
│   └── sql.py                 # Consolidated SQL queries
└── utils/
    └── formatters.py          # Duration, number formatting utilities
```

## SQL Queries

All queries are consolidated from:
- `genie_audit_bundle/src/dashboards/genie_performance.lvdash.json`
- `genie_audit_bundle/tests/test_queries.sql`
- `genie_performance_audit.sql`

Queries analyze:
- Query performance metrics (duration, P50/P90/P95/P99)
- Bottleneck classification (compute startup, queue wait, compilation, large scan, slow execution)
- Time breakdown by phase
- Success/failure rates
- User activity patterns

## Configuration

### app.yaml
Controls Databricks Apps runtime configuration:
- Streamlit server settings
- Warehouse ID for SQL execution

### databricks.yml
Controls deployment:
- Bundle name and sync paths
- User API scopes for authentication
- App permissions

## License

Internal use only - Databricks/Albertsons
