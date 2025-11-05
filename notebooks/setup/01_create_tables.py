# Databricks notebook source
"""
Create Lightweight Validation Framework Metadata Tables

Creates:
- validation_mapping (configuration table)
- validation_log (run audit table)
- validation_summary (metrics table)

Run this ONCE during initial setup.
"""

# COMMAND ----------

# DBTITLE 1,Load Constants
from deltarecon.config import constants
import importlib

# Reload constants to ensure we get the latest values
importlib.reload(constants)

# COMMAND ----------

print(f"Framework Version: {constants.FRAMEWORK_VERSION}")
print(f"Creating tables in catalog: {constants.VALIDATION_OPS_CATALOG}")
print(f"Schema: {constants.VALIDATION_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Create validation_mapping Table
validation_mapping_ddl = f"""
CREATE TABLE IF NOT EXISTS {constants.VALIDATION_MAPPING_TABLE} (
  table_group STRING COMMENT 'Table group (matches ingestion_config group_name)',
  workflow_name STRING COMMENT 'Workflow name',
  table_family STRING COMMENT 'Table family',
  src_table STRING COMMENT 'Source table name (fully qualified)',
  tgt_table STRING COMMENT 'Target table name (fully qualified)',
  tgt_primary_keys STRING COMMENT 'Pipe-separated primary key columns',
  mismatch_exclude_fields STRING COMMENT 'Comma-separated fields to exclude from comparison',
  validation_is_active BOOLEAN COMMENT 'Whether validation is active for this table'
)
USING DELTA
COMMENT 'Validation mapping configuration - synced from ingestion_config'
"""

spark.sql(validation_mapping_ddl)
print(f"Created/verified: {constants.VALIDATION_MAPPING_TABLE}")

# COMMAND ----------

# DBTITLE 1,Create validation_log Table
validation_log_ddl = f"""
CREATE TABLE IF NOT EXISTS {constants.VALIDATION_LOG_TABLE} (
  batch_load_id STRING COMMENT 'Batch ID validated in this run',
  src_table STRING COMMENT 'Source table name',
  tgt_table STRING COMMENT 'Target table name',
  validation_run_status STRING COMMENT 'Run status: IN_PROGRESS, SUCCESS, FAILED',
  validation_run_start_time TIMESTAMP COMMENT 'Start timestamp',
  validation_run_end_time TIMESTAMP COMMENT 'End timestamp',
  exception STRING COMMENT 'Exception message if failed',
  iteration_name STRING COMMENT 'Iteration name (e.g., daily_20240101_120000)',
  workflow_name STRING COMMENT 'Workflow name',
  table_family STRING COMMENT 'Table family'
)
USING DELTA
COMMENT 'Validation run audit log - one record per batch'
"""

spark.sql(validation_log_ddl)
print(f"Created/verified: {constants.VALIDATION_LOG_TABLE}")

# COMMAND ----------

# DBTITLE 1,Create validation_summary Table
validation_summary_ddl = f"""
CREATE TABLE IF NOT EXISTS {constants.VALIDATION_SUMMARY_TABLE} (
  batch_load_id STRING COMMENT 'Batch ID validated',
  src_table STRING COMMENT 'Source table name',
  tgt_table STRING COMMENT 'Target table name',
  row_count_match_status STRING COMMENT 'Row count validation status: PASSED, FAILED, SKIPPED',
  schema_match_status STRING COMMENT 'Schema validation status: PASSED, FAILED, SKIPPED',
  primary_key_compliance_status STRING COMMENT 'PK validation status: PASSED, FAILED, SKIPPED',
  col_name_compare_status STRING COMMENT 'Column name comparison status: PASSED, FAILED, SKIPPED',
  data_type_compare_status STRING COMMENT 'Data type comparison status: PASSED, FAILED, SKIPPED',
  data_reconciliation_status STRING COMMENT 'Data reconciliation status: PASSED, FAILED, SKIPPED',
  overall_status STRING COMMENT 'Overall validation status: SUCCESS, FAILED',
  metrics STRUCT<
    src_records: BIGINT,
    tgt_records: BIGINT,
    src_extras: BIGINT,
    tgt_extras: BIGINT,
    mismatches: STRING,
    matches: BIGINT
  > COMMENT 'Validation metrics',
  iteration_name STRING COMMENT 'Iteration name',
  workflow_name STRING COMMENT 'Workflow name',
  table_family STRING COMMENT 'Table family'
)
USING DELTA
COMMENT 'Validation summary with metrics and check statuses - one record per batch'
"""

spark.sql(validation_summary_ddl)
print(f"Created/verified: {constants.VALIDATION_SUMMARY_TABLE}")

# COMMAND ----------

# DBTITLE 1,Grant Permissions to SPN
# Check if SPN_ID is configured
if not constants.SPN_ID:
    print("WARNING: SPN_ID not configured in constants.py")
    print("Please set constants.SPN_ID to grant permissions to the Service Principal")
    print("Skipping permission grants...")
else:
    print(f"Granting permissions to SPN: {constants.SPN_ID}")
    
    # Grant permissions to the SPN for all validation tables
    tables = [
        constants.VALIDATION_MAPPING_TABLE,
        constants.VALIDATION_LOG_TABLE,
        constants.VALIDATION_SUMMARY_TABLE
    ]
    
    for table in tables:
        try:
            # Grant SELECT and MODIFY permissions to allow read/write access
            spark.sql(f"GRANT SELECT, MODIFY ON TABLE {table} TO `{constants.SPN_ID}`")
            print(f"✓ Granted SELECT, MODIFY permissions on {table} to SPN {constants.SPN_ID}")
        except Exception as e:
            print(f" Warning: Could not grant permissions on {table}: {str(e)}")
            print(f"  You may need to grant these permissions manually or ensure you have the right privileges")
    
    # Also grant permissions on the schema itself
    try:
        spark.sql(f"GRANT USAGE ON SCHEMA {constants.VALIDATION_SCHEMA} TO `{constants.SPN_ID}`")
        print(f"✓ Granted USAGE permission on schema {constants.VALIDATION_SCHEMA} to SPN {constants.SPN_ID}")
    except Exception as e:
        print(f"Warning: Could not grant USAGE on schema: {str(e)}")
    
    # Grant permissions on catalog (if needed)
    try:
        spark.sql(f"GRANT USAGE ON CATALOG {constants.VALIDATION_OPS_CATALOG} TO `{constants.SPN_ID}`")
        print(f"✓ Granted USAGE permission on catalog {constants.VALIDATION_OPS_CATALOG} to SPN {constants.SPN_ID}")
    except Exception as e:
        print(f"⚠ Warning: Could not grant USAGE on catalog: {str(e)}")
    
    print("\n Permission grants completed!")

# COMMAND ----------

# DBTITLE 1,Summary
print("All tables created successfully!")
print(f"\n1. {constants.VALIDATION_MAPPING_TABLE}")
print(f"2. {constants.VALIDATION_LOG_TABLE}")
print(f"3. {constants.VALIDATION_SUMMARY_TABLE}")

if constants.SPN_ID:
    print("\n Permissions granted to SPN")
else:
    print("\n SPN permissions not granted - configure constants.SPN_ID")

print("\nNext step: Run 02_setup_validation_mapping.py to populate validation_mapping")

