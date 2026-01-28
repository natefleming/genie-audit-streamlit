# Genie Performance Audit - Deployment & Permissions Guide

## Overview

This guide provides step-by-step instructions for granting the necessary permissions to deploy and run the Genie Performance Audit application in your Databricks workspace.

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Account Admin or Workspace Admin privileges (for granting permissions)
- At least one Genie room with query activity
- Access to a SQL Warehouse

---

## Step 1: Grant System Table Access

The application requires SELECT access to two system tables. Run these SQL commands as an Account Admin or Metastore Admin:

### Option A: Grant to Individual User

```sql
-- Grant access to query execution history
GRANT SELECT ON system.query.history TO `user@example.com`;

-- Grant access to audit logs (for AI overhead calculation)
GRANT SELECT ON system.access.audit TO `user@example.com`;
```

### Option B: Grant to a Group (Recommended)

```sql
-- Create a group for Genie Audit users (if not exists)
-- This is done in Account Console > User Management > Groups

-- Grant access to the group
GRANT SELECT ON system.query.history TO `genie-audit-users`;
GRANT SELECT ON system.access.audit TO `genie-audit-users`;
```

### Verify Access

Test that you can query the system tables:

```sql
-- Test query.history access
SELECT COUNT(*) FROM system.query.history 
WHERE start_time >= current_timestamp() - INTERVAL 1 DAY;

-- Test audit access  
SELECT COUNT(*) FROM system.access.audit
WHERE event_time >= current_timestamp() - INTERVAL 1 DAY
  AND action_name LIKE '%genie%';
```

---

## Step 2: Configure SQL Warehouse Access

The application executes SQL queries via a SQL Warehouse. Ensure users have access:

### Grant Warehouse Access

1. Navigate to **SQL Warehouses** in the Databricks workspace
2. Click on your target warehouse
3. Go to **Permissions** tab
4. Add users or groups with **Can Use** permission

### Identify Warehouse ID

You'll need the warehouse ID for deployment:

1. Go to **SQL Warehouses**
2. Click on your warehouse
3. Copy the **ID** from the URL or warehouse details panel
   - Format: `148ccb90800933a1` (16-character hex string)

---

## Step 3: Verify Genie Room Access

Users can only analyze Genie rooms they have access to:

1. Navigate to **Genie** in the workspace sidebar
2. Verify you can see the rooms you want to analyze
3. If you don't see a room, request access from the room owner

---

## Step 4: Install Databricks CLI (For Deployment)

### macOS/Linux

```bash
# Using Homebrew
brew install databricks/tap/databricks

# Or using pip
pip install databricks-cli
```

### Windows

```powershell
# Using pip
pip install databricks-cli
```

### Configure Authentication

```bash
# Configure the CLI with your workspace
databricks configure --host https://your-workspace.cloud.databricks.com

# Verify connection
databricks current-user me
```

---

## Step 5: Deploy the Application

### Clone the Repository

```bash
git clone <repository-url>
cd genie-audit-streamlit
```

### Update Configuration

Edit `app.yaml` to set your warehouse ID:

```yaml
env:
  - name: DATABRICKS_WAREHOUSE_ID
    value: YOUR_WAREHOUSE_ID_HERE
  - name: SYSTEM_CATALOG
    value: system  # Default: "system". Change if using a custom catalog.
```

### Deploy with Databricks Asset Bundles

```bash
# Validate the bundle configuration
databricks bundle validate

# Deploy to your workspace
databricks bundle deploy

# Check deployment status
databricks bundle run genie-audit
```

### Verify Deployment

1. Navigate to **Apps** in the Databricks workspace
2. Find **genie-audit** in the list
3. Click to open the application

---

## Step 6: Grant Application Access to Other Users

After deployment, grant other users access to the app:

### Via Databricks UI

1. Go to **Apps** > **genie-audit**
2. Click **Permissions**
3. Add users or groups with **Can Use** permission

### Via databricks.yml (During Deployment)

The `databricks.yml` file includes default permissions:

```yaml
resources:
  apps:
    genie-audit:
      permissions:
        - level: CAN_USE
          group_name: users  # All workspace users
```

Modify to restrict access:

```yaml
permissions:
  - level: CAN_USE
    group_name: data-analysts
  - level: CAN_MANAGE
    user_name: admin@example.com
```

---

## Step 7: Local Development (Optional)

For local testing before deployment:

### Install Dependencies

```bash
# Using uv (recommended)
uv sync

# Or using pip
pip install -r requirements.txt
```

### Set Environment Variables

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
export SYSTEM_CATALOG="system"  # Optional: defaults to "system"
```

### Run Locally

```bash
uv run streamlit run app.py
```

---

## Permissions Summary

| Permission | How to Grant | Required For |
|------------|--------------|--------------|
| `system.query.history` SELECT | SQL GRANT statement | Query metrics and timing |
| `system.access.audit` SELECT | SQL GRANT statement | AI overhead calculation |
| SQL Warehouse CAN_USE | Warehouse Permissions UI | Executing queries |
| Genie Room Access | Room sharing settings | Viewing room data |
| App CAN_USE | App Permissions UI | Running the application |

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABRICKS_HOST` | Yes (local only) | - | Workspace URL (e.g., `https://your-workspace.cloud.databricks.com`) |
| `DATABRICKS_TOKEN` | Yes (local only) | - | Personal access token (not needed in Databricks Apps) |
| `DATABRICKS_WAREHOUSE_ID` | Yes | - | SQL warehouse ID for executing queries |
| `SYSTEM_CATALOG` | No | `system` | Catalog containing system tables. Change if using a custom catalog location. |
| `STREAMLIT_THEME_BASE` | No | `dark` | Streamlit theme (`dark` or `light`) |

---

## API Scopes Required

The application uses on-behalf-of-user authentication with these OAuth scopes:

| Scope | Purpose |
|-------|---------|
| `sql` | Execute SQL queries against system tables |
| `dashboards.genie` | Access Genie API (list rooms, conversations, messages) |
| `catalog.catalogs:read` | Read catalog metadata |
| `catalog.schemas:read` | Read schema metadata |
| `catalog.tables:read` | Read table metadata |

These are configured automatically in `databricks.yml` under `user_api_scopes`.

---

## Troubleshooting

### "Permission denied" on system tables

Verify grants were applied:

```sql
SHOW GRANTS ON system.query.history;
SHOW GRANTS ON system.access.audit;
```

### "Cannot access warehouse"

Check warehouse permissions in the SQL Warehouses UI.

### "No Genie rooms found"

- Ensure you have access to at least one Genie room
- Verify the `dashboards.genie` scope is included in the app configuration

### Slow initial load

The application queries up to 30 days of data. For faster loads:
- Use a larger SQL warehouse
- Reduce the time filter in the application UI

### "Failed to connect to Databricks"

- Verify `DATABRICKS_HOST` is set correctly
- For local development, ensure `DATABRICKS_TOKEN` is valid
- For Databricks Apps, authentication is automatic

---

## Support

For issues or questions, contact your Databricks administrator or the application maintainer.
