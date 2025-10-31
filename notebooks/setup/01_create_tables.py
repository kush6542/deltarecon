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
  batch_load_id ARRAY<STRING> COMMENT 'Array of batch IDs validated in this run',
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
COMMENT 'Validation run audit log'
"""

spark.sql(validation_log_ddl)
print(f"Created/verified: {constants.VALIDATION_LOG_TABLE}")

# COMMAND ----------

# DBTITLE 1,Create validation_summary Table
validation_summary_ddl = f"""
CREATE TABLE IF NOT EXISTS {constants.VALIDATION_SUMMARY_TABLE} (
  batch_load_id ARRAY<STRING> COMMENT 'Array of batch IDs validated',
  src_table STRING COMMENT 'Source table name',
  tgt_table STRING COMMENT 'Target table name',
  row_count_match_status STRING COMMENT 'Row count validation status: PASSED, FAILED, SKIPPED',
  schema_match_status STRING COMMENT 'Schema validation status: PASSED, FAILED, SKIPPED',
  primary_key_compliance_status STRING COMMENT 'PK validation status: PASSED, FAILED, SKIPPED',
  col_name_compare_status STRING COMMENT 'Column name comparison status: PASSED, FAILED, SKIPPED',
  data_type_compare_status STRING COMMENT 'Data type comparison status: PASSED, FAILED, SKIPPED',
  overall_status STRING COMMENT 'Overall validation status: SUCCESS, FAILED',
  metrics STRUCT<
    src_records: BIGINT,
    tgt_records: BIGINT,
    src_extras: BIGINT,
    tgt_extras: BIGINT,
    mismatches: BIGINT,
    matches: BIGINT
  > COMMENT 'Validation metrics',
  iteration_name STRING COMMENT 'Iteration name',
  workflow_name STRING COMMENT 'Workflow name',
  table_family STRING COMMENT 'Table family'
)
USING DELTA
COMMENT 'Validation summary with metrics and check statuses'
"""

spark.sql(validation_summary_ddl)
print(f"Created/verified: {constants.VALIDATION_SUMMARY_TABLE}")

# COMMAND ----------

# DBTITLE 1,Summary
print("All tables created successfully!")
print(f"\n1. {constants.VALIDATION_MAPPING_TABLE}")
print(f"2. {constants.VALIDATION_LOG_TABLE}")
print(f"3. {constants.VALIDATION_SUMMARY_TABLE}")
print("\nNext step: Run 02_setup_validation_mapping.py to populate validation_mapping")

