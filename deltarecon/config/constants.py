"""
Configuration constants for lightweight validation framework

These constants define:
- Ingestion metadata table references (READ FROM)
- Validation metadata table references (WRITE TO)
- Framework configuration
"""

# ============================================================================
# INGESTION METADATA TABLES (READ FROM - Source of Truth)
# ============================================================================

INGESTION_OPS_CATALOG = "ts42_demo"
INGESTION_OPS_SCHEMA = "ts42_demo.migration_operations"

INGESTION_METADATA_TABLE = f"{INGESTION_OPS_SCHEMA}.serving_ingestion_metadata"
INGESTION_AUDIT_TABLE = f"{INGESTION_OPS_SCHEMA}.serving_ingestion_audit"
INGESTION_CONFIG_TABLE = f"{INGESTION_OPS_SCHEMA}.serving_ingestion_config"
INGESTION_SRC_TABLE_PARTITION_MAPPING = f"{INGESTION_OPS_SCHEMA}.source_table_partition_mapping"

# ============================================================================
# VALIDATION METADATA TABLES (WRITE TO - Validation Results)
# ============================================================================
VALIDATION_OPS_CATALOG = "cat_ril_nayeem_03"
VALIDATION_SCHEMA = f"{VALIDATION_OPS_CATALOG}.validation_v2"

VALIDATION_MAPPING_TABLE = f"{VALIDATION_SCHEMA}.validation_mapping"
VALIDATION_LOG_TABLE = f"{VALIDATION_SCHEMA}.validation_log"
VALIDATION_SUMMARY_TABLE = f"{VALIDATION_SCHEMA}.validation_summary"

# ============================================================================
# FRAMEWORK CONFIGURATION
# ============================================================================

# Processing configuration
PARALLELISM = 5  # Number of tables to process in parallel per job
SPOT_CHECK_SAMPLE_SIZE = 100  # Number of rows for random spot check
FRAMEWORK_VERSION = "v1.0.0"

# Retry configuration (for future use)
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 10

# Timeout configuration
QUERY_TIMEOUT_SECONDS = 300  # 5 minutes timeout for queries

# Validation run user (for audit trail)
VALIDATION_USER_NAME = "lightweight_deltarecon"
VALIDATION_USER_EMAIL = "kushagra.parashar@databricks.com"

# Job naming configuration
JOB_NAME_PREFIX = "validation_job"  # Prefix for job names. Final format: {JOB_NAME_PREFIX}_{table_group}

