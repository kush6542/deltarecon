# Databricks notebook source
"""
Setup Validation Mapping

Syncs validation_mapping table from ingestion_config.
Run this whenever ingestion_config changes to update validation configs.

MERGE logic ensures idempotency.
"""

# COMMAND ----------

# DBTITLE 1,Load Constants
from deltarecon.config import constants

# COMMAND ----------

print(f"Framework Version: {constants.FRAMEWORK_VERSION}")
print(f"Syncing validation_mapping from ingestion_config...")

# COMMAND ----------

# DBTITLE 1,Sync validation_mapping from ingestion_config
merge_query = f"""
MERGE INTO {constants.VALIDATION_MAPPING_TABLE} AS target
USING (
  SELECT
    group_name as table_group,
    concat('validation_', group_name) as workflow_name,
    concat_ws('_', target_catalog, target_schema, target_table) as table_family,
    concat_ws('.', 'source_system', source_schema, source_table) as src_table,
    concat_ws('.', target_catalog, target_schema, target_table) as tgt_table,
    -- Combine primary_key and partition_column into tgt_primary_keys (pipe-separated)
    CASE 
      WHEN (primary_key IS NOT NULL AND primary_key <> '') 
           AND (partition_column IS NOT NULL AND partition_column <> '') THEN
        replace(concat_ws('|', primary_key, partition_column), ',', '|')
      WHEN (primary_key IS NOT NULL AND primary_key <> '') THEN
        replace(primary_key, ',', '|')
      ELSE NULL
    END as tgt_primary_keys,
    '_aud_batch_load_id' as mismatch_exclude_fields,
    CASE WHEN is_active = 'Y' THEN true ELSE false END as validation_is_active
  FROM {constants.INGESTION_CONFIG_TABLE}
) AS source
ON target.tgt_table = source.tgt_table
WHEN MATCHED THEN UPDATE SET
  target.table_group = source.table_group,
  target.workflow_name = source.workflow_name,
  target.table_family = source.table_family,
  target.src_table = source.src_table,
  target.tgt_primary_keys = source.tgt_primary_keys,
  target.mismatch_exclude_fields = source.mismatch_exclude_fields,
  target.validation_is_active = source.validation_is_active
WHEN NOT MATCHED THEN INSERT (
  table_group,
  workflow_name,
  table_family,
  src_table,
  tgt_table,
  tgt_primary_keys,
  mismatch_exclude_fields,
  validation_is_active
) VALUES (
  source.table_group,
  source.workflow_name,
  source.table_family,
  source.src_table,
  source.tgt_table,
  source.tgt_primary_keys,
  source.mismatch_exclude_fields,
  source.validation_is_active
)
"""

spark.sql(merge_query)
print("Validation mapping synced successfully")

# COMMAND ----------

# DBTITLE 1,Display Results
print("Validation Mapping Summary")

summary_df = spark.sql(f"""
  SELECT 
    table_group,
    COUNT(*) as table_count,
    SUM(CASE WHEN validation_is_active THEN 1 ELSE 0 END) as active_count
  FROM {constants.VALIDATION_MAPPING_TABLE}
  GROUP BY table_group
  ORDER BY table_group
""")

display(summary_df)

print("\nSample records:")
sample_df = spark.sql(f"""
  SELECT *
  FROM {constants.VALIDATION_MAPPING_TABLE}
  LIMIT 5
""")

display(sample_df)

print("\nNext step: Configure jobs in 03_configure_jobs.py")

