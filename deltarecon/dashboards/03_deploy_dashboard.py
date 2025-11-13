# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Validation Dashboard
# MAGIC This notebook deploys the validation_daily_report dashboard to the Databricks workspace.

# COMMAND ----------

import json
import requests
import sys
import os
from typing import Optional

# Add parent directory to path to import constants
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Load catalog and schema from constants.py

# COMMAND ----------

# Import constants
from config.constants import VALIDATION_OPS_CATALOG, VALIDATION_SCHEMA

# Extract catalog and schema names
# VALIDATION_SCHEMA is in format "catalog.schema", we need to split it
VALIDATION_CATALOG_NAME = VALIDATION_OPS_CATALOG  
VALIDATION_SCHEMA_NAME = VALIDATION_SCHEMA.split(".")[-1] 

DASHBOARD_NAME = "Validation Daily Report"

print(f"Configuration loaded from constants.py:")
print(f"  Catalog: {VALIDATION_CATALOG_NAME}")
print(f"  Schema: {VALIDATION_SCHEMA_NAME}")
print(f"  Full path: {VALIDATION_CATALOG_NAME}.{VALIDATION_SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Dashboard Definition

# COMMAND ----------

# Read the dashboard JSON file (in the same directory as this notebook)
dashboard_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "validation_daily_report.lvdash.json")

with open(dashboard_file_path, "r") as f:
    dashboard_definition = json.load(f)

print(f"✓ Loaded dashboard definition from: {dashboard_file_path}")
print(f"  Datasets: {len(dashboard_definition.get('datasets', []))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Replace Template Variables in Queries

# COMMAND ----------

def replace_template_variables(dashboard_def: dict, catalog: str, schema: str) -> dict:
    """
    Replace template variables in the dashboard definition
    """
    dashboard_str = json.dumps(dashboard_def)
    
    # Replace template variables
    dashboard_str = dashboard_str.replace("<<validation_catalog_name>>", catalog)
    dashboard_str = dashboard_str.replace("<<validation_schema_name>>", schema)
    
    return json.loads(dashboard_str)

# Replace template variables
dashboard_with_values = replace_template_variables(
    dashboard_definition, 
    VALIDATION_CATALOG_NAME, 
    VALIDATION_SCHEMA_NAME
)

print(f"✓ Replaced template variables:")
print(f"  - <<validation_catalog_name>> → {VALIDATION_CATALOG_NAME}")
print(f"  - <<validation_schema_name>> → {VALIDATION_SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Dashboard using Databricks API

# COMMAND ----------

# Get workspace context
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_url = ctx.apiUrl().get()
api_token = ctx.apiToken().get()

print(f"Workspace URL: {workspace_url}")

# COMMAND ----------

def create_lakeview_dashboard(
    workspace_url: str,
    token: str,
    dashboard_name: str,
    serialized_dashboard: dict,
    parent_path: Optional[str] = None
) -> dict:
    """
    Create a Lakeview dashboard using Databricks API
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Create dashboard payload
    payload = {
        "display_name": dashboard_name,
        "serialized_dashboard": json.dumps(serialized_dashboard)
    }
    
    if parent_path:
        payload["parent_path"] = parent_path
    
    # Create dashboard
    response = requests.post(
        f"{workspace_url}/api/2.0/lakeview/dashboards",
        headers=headers,
        json=payload
    )
    
    if response.status_code in [200, 201]:
        return response.json()
    else:
        raise Exception(f"Failed to create dashboard: {response.status_code} - {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or Update Dashboard

# COMMAND ----------

try:
    # Create the dashboard (without parent_path so it appears in Dashboards section)
    result = create_lakeview_dashboard(
        workspace_url=workspace_url,
        token=api_token,
        dashboard_name=DASHBOARD_NAME,
        serialized_dashboard=dashboard_with_values,
        parent_path=None
    )
    
    dashboard_id = result.get("dashboard_id") or result.get("id")
    dashboard_path = result.get("path")
    
    print("✓ Dashboard created successfully!")
    print(f"  Dashboard ID: {dashboard_id}")
    if dashboard_path:
        print(f"  Dashboard Path: {dashboard_path}")
    print(f"\n🔗 Access your dashboard at:")
    print(f"  {workspace_url}/sql/dashboardsv3/{dashboard_id}")
    print(f"\nThe dashboard will appear in the Dashboards section of your Databricks workspace.")
    
except Exception as e:
    print(f" Error creating dashboard: {str(e)}")
    print("\nTrying alternative approach...")
    
    # Alternative: Try using the older DBSQL API
    try:
        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "name": DASHBOARD_NAME,
            "tags": ["validation", "automated"],
        }
        
        response = requests.post(
            f"{workspace_url}/api/2.0/preview/sql/dashboards",
            headers=headers,
            json=payload
        )
        
        if response.status_code in [200, 201]:
            dashboard = response.json()
            print(f"✓ Dashboard created successfully using DBSQL API!")
            print(f"  Dashboard ID: {dashboard.get('id')}")
            print(f"\n Note: You'll need to manually import the dashboard definition")
            print(f"     or use the Databricks UI to configure it.")
        else:
            print(f" Also failed with DBSQL API: {response.status_code} - {response.text}")
            
    except Exception as e2:
        print(f" Alternative approach also failed: {str(e2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Dashboard Tables Exist

# COMMAND ----------

# Check if the validation_summary table exists
validation_table_path = f"{VALIDATION_CATALOG_NAME}.{VALIDATION_SCHEMA_NAME}.validation_summary"

try:
    result = spark.sql(f"""
        SELECT COUNT(*) as row_count 
        FROM {validation_table_path}
    """).collect()
    
    row_count = result[0]['row_count']
    print(f"✓ validation_summary table exists with {row_count:,} rows")
    print(f"  Table: {validation_table_path}")
    
    # Show sample data
    print("\n Sample validation data:")
    display(spark.sql(f"""
        SELECT 
            tgt_table,
            iteration_name,
            CASE 
                WHEN row_count_match_status != 'FAILED'
                 AND schema_match_status != 'FAILED'
                 AND col_name_compare_status != 'FAILED'
                 AND data_type_compare_status != 'FAILED'
                 AND data_reconciliation_status != 'FAILED'
                THEN 'PASSED'
                ELSE 'FAILED'
            END AS overall_status
        FROM {validation_table_path}
        ORDER BY iteration_name DESC
        LIMIT 10
    """))
    
except Exception as e:
    print(f"  Warning: Could not access validation_summary table")
    print(f"   Error: {str(e)}")
    print(f"\n   Make sure the table exists at: {validation_table_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Import Instructions
# MAGIC 
# MAGIC If the API approach doesn't work, you can manually import the dashboard:
# MAGIC 
# MAGIC 1. Go to **Dashboards** section in your Databricks workspace
# MAGIC 2. Click **Create** → **Import Dashboard**
# MAGIC 3. Upload the file: `validation_daily_report.lvdash.json` (from the dashboards directory)
# MAGIC 4. When prompted for template variables, enter:
# MAGIC    - `validation_catalog_name`: cat_ril_nayeem_03
# MAGIC    - `validation_schema_name`: validation_v2
# MAGIC 5. Click **Import**
# MAGIC 
# MAGIC The dashboard will appear in the **Dashboards** section and will be accessible to all users with appropriate permissions.

