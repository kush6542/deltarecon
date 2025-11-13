# Databricks notebook source
# MAGIC %md
# MAGIC # Manual Validation Override
# MAGIC 
# MAGIC ## Purpose
# MAGIC This notebook allows manual override of validation status for specific batches. Use this when:
# MAGIC - Transient failures that are acceptable and verified manually
# MAGIC - Known data quality issues that have been approved by stakeholders
# MAGIC - Emergency scenarios requiring bypass of failed validations
# MAGIC - Batches that need to be excluded from validation
# MAGIC 
# MAGIC ## ⚠️ IMPORTANT WARNINGS
# MAGIC - **USE WITH CAUTION**: This bypasses automated validation checks
# MAGIC - **AUDIT TRAIL**: All manual overrides are logged with reason and user
# MAGIC - **APPROVAL REQUIRED**: Ensure proper approval before marking validations as SUCCESS
# MAGIC - **VERIFICATION**: Manually verify data quality before using this tool
# MAGIC 
# MAGIC ## How It Works
# MAGIC The notebook inserts a SUCCESS record into the `validation_log` table, which prevents
# MAGIC the batch from being picked up in subsequent validation runs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input Parameters

# COMMAND ----------

dbutils.widgets.text("target_table", "", "1. Target Table (catalog.schema.table)")
dbutils.widgets.text("batch_load_id", "", "2. Batch Load ID")
dbutils.widgets.dropdown("override_status", "SUCCESS", ["SUCCESS", "SKIPPED"], "3. Override Status")
dbutils.widgets.text("reason", "", "4. Reason for Override (Required)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "5. Dry Run (Preview)")

# Get parameters
target_table = dbutils.widgets.get("target_table").strip()
batch_load_id = dbutils.widgets.get("batch_load_id").strip()
override_status = dbutils.widgets.get("override_status").strip()
reason = dbutils.widgets.get("reason").strip()
dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"

# Display parameters
print("=" * 80)
print("VALIDATION OVERRIDE PARAMETERS")
print("=" * 80)
print(f"Target Table      : {target_table}")
print(f"Batch Load ID     : {batch_load_id}")
print(f"Override Status   : {override_status}")
print(f"Reason            : {reason}")
print(f"Dry Run           : {dry_run}")
print(f"Executed By       : {spark.sql('SELECT current_user()').first()[0]}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation of Inputs

# COMMAND ----------

# Input validation
errors = []

if not target_table:
    errors.append("❌ Target table is required")
elif len(target_table.split('.')) != 3:
    errors.append("❌ Target table must be in format: catalog.schema.table")

if not batch_load_id:
    errors.append("❌ Batch load ID is required")

if not reason:
    errors.append("❌ Reason is required - this is critical for audit trail")
elif len(reason) < 10:
    errors.append("❌ Reason must be at least 10 characters (provide detailed explanation)")

if override_status not in ['SUCCESS', 'SKIPPED']:
    errors.append("❌ Override status must be SUCCESS or SKIPPED")

# Display errors if any
if errors:
    print("\n🚨 INPUT VALIDATION FAILED:\n")
    for error in errors:
        print(error)
    print("\n⚠️  Please correct the errors and try again.")
    dbutils.notebook.exit("FAILED: Input validation errors")
else:
    print("✅ Input validation passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Framework Configuration

# COMMAND ----------

# Import framework configuration
import sys
sys.path.append("/Workspace/Shared/serving_gold_ingestion/dbx_validation_framework")

from deltarecon.config import constants
from datetime import datetime

print(f"Validation Log Table: {constants.VALIDATION_LOG_TABLE}")
print(f"Validation Summary Table: {constants.VALIDATION_SUMMARY_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Verify Batch Exists in Ingestion Audit

# COMMAND ----------

print("=" * 80)
print("STEP 1: VERIFY BATCH EXISTS IN INGESTION AUDIT")
print("=" * 80)

# Check if batch exists in ingestion audit
check_batch_query = f"""
    SELECT 
        batch_load_id,
        target_table_name,
        group_name,
        status,
        start_time,
        end_time
    FROM {constants.INGESTION_AUDIT_TABLE}
    WHERE batch_load_id = '{batch_load_id}'
        AND target_table_name = '{target_table}'
"""

batch_info_df = spark.sql(check_batch_query)
batch_exists = batch_info_df.count() > 0

if not batch_exists:
    print(f"\n❌ ERROR: Batch '{batch_load_id}' not found for table '{target_table}'")
    print(f"\nPlease verify:")
    print(f"  1. Batch load ID is correct")
    print(f"  2. Target table name is correct (use full name: catalog.schema.table)")
    print(f"  3. Batch exists in ingestion audit table")
    dbutils.notebook.exit(f"FAILED: Batch not found")
else:
    print(f"✅ Batch found in ingestion audit table")
    print("\nBatch Information:")
    batch_info_df.show(truncate=False)
    
    # Store batch info for later use
    batch_record = batch_info_df.first()
    group_name = batch_record.group_name
    ingestion_status = batch_record.status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Check Current Validation Status

# COMMAND ----------

print("=" * 80)
print("STEP 2: CHECK CURRENT VALIDATION STATUS")
print("=" * 80)

# Check if validation already exists for this batch
check_validation_query = f"""
    SELECT 
        batch_load_id,
        validation_run_status,
        validation_run_start_time,
        validation_run_end_time,
        iteration_name
    FROM {constants.VALIDATION_LOG_TABLE}
    WHERE batch_load_id = '{batch_load_id}'
    ORDER BY validation_run_start_time DESC
    LIMIT 5
"""

existing_validations_df = spark.sql(check_validation_query)
validation_count = existing_validations_df.count()

if validation_count > 0:
    print(f"⚠️  Found {validation_count} existing validation record(s) for this batch:")
    existing_validations_df.show(truncate=False)
    
    latest_validation = existing_validations_df.first()
    latest_status = latest_validation.validation_run_status
    
    if latest_status == 'SUCCESS':
        print(f"\n⚠️  WARNING: Latest validation status is already SUCCESS")
        print(f"   This batch will already be skipped in future runs.")
        print(f"   Do you still want to add another SUCCESS record?")
    elif latest_status == 'SKIPPED':
        print(f"\n⚠️  WARNING: Latest validation status is SKIPPED")
        print(f"   This batch is already marked to be skipped.")
else:
    print(f"ℹ️  No existing validation records found for this batch")
    print(f"   This is a new validation override.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Check Validation Summary

# COMMAND ----------

print("=" * 80)
print("STEP 3: CHECK VALIDATION SUMMARY")
print("=" * 80)

# Check validation summary (if exists)
check_summary_query = f"""
    SELECT 
        batch_load_id,
        tgt_table,
        overall_status,
        row_count_match_status,
        schema_match_status,
        data_reconciliation_status,
        src_records,
        tgt_records,
        iteration_name
    FROM {constants.VALIDATION_SUMMARY_TABLE}
    WHERE batch_load_id = '{batch_load_id}'
        AND tgt_table = '{target_table}'
    ORDER BY iteration_name DESC
    LIMIT 5
"""

existing_summary_df = spark.sql(check_summary_query)
summary_count = existing_summary_df.count()

if summary_count > 0:
    print(f"ℹ️  Found {summary_count} validation summary record(s):")
    existing_summary_df.show(truncate=False)
else:
    print(f"ℹ️  No validation summary records found for this batch")
    print(f"   (This is normal if validation never ran)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Preview Override Operation

# COMMAND ----------

print("=" * 80)
print("STEP 4: PREVIEW OVERRIDE OPERATION")
print("=" * 80)

# Get current user
current_user = spark.sql('SELECT current_user()').first()[0]

# Generate iteration name for this manual override
override_iteration = f"manual_override_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{current_user.split('@')[0]}"

# Preview the record that will be inserted
print("\n📝 The following record will be inserted into validation_log:")
print("-" * 80)
print(f"batch_load_id              : {batch_load_id}")
print(f"validation_run_status      : {override_status}")
print(f"validation_run_start_time  : {datetime.now()}")
print(f"validation_run_end_time    : {datetime.now()}")
print(f"iteration_name             : {override_iteration}")
print(f"workflow_name              : manual_override")
print(f"table_family               : {group_name}")
print(f"source_table               : <will be looked up>")
print(f"target_table               : {target_table}")
print(f"override_reason            : {reason}")
print(f"override_by_user           : {current_user}")
print("-" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Execute Override (or Dry Run)

# COMMAND ----------

print("=" * 80)
print("STEP 5: EXECUTE OVERRIDE")
print("=" * 80)

if dry_run:
    print("🔍 DRY RUN MODE - No changes will be made")
    print("\nTo execute the override:")
    print("  1. Review all information above")
    print("  2. Ensure you have proper approval")
    print("  3. Set 'Dry Run' parameter to 'false'")
    print("  4. Re-run this notebook")
    print("\n✅ Dry run completed successfully")
else:
    print("⚠️  EXECUTING OVERRIDE - This will modify the validation_log table")
    
    try:
        # Get source table from ingestion config
        source_lookup_query = f"""
            SELECT source_table
            FROM {constants.INGESTION_CONFIG_TABLE}
            WHERE concat_ws('.', target_catalog, target_schema, target_table) = '{target_table}'
                AND group_name = '{group_name}'
            LIMIT 1
        """
        source_table_result = spark.sql(source_lookup_query).first()
        source_table = source_table_result.source_table if source_table_result else "UNKNOWN"
        
        # Insert override record into validation_log
        insert_query = f"""
            INSERT INTO {constants.VALIDATION_LOG_TABLE}
            VALUES (
                '{override_iteration}',
                'manual_override',
                '{group_name}',
                '{source_table}',
                '{target_table}',
                '{batch_load_id}',
                '{override_status}',
                TIMESTAMP '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}',
                TIMESTAMP '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}',
                'MANUAL OVERRIDE - Reason: {reason} | User: {current_user}'
            )
        """
        
        spark.sql(insert_query)
        
        print("✅ Override record successfully inserted into validation_log")
        print(f"\nIteration Name: {override_iteration}")
        print(f"Status: {override_status}")
        print(f"User: {current_user}")
        print(f"Timestamp: {datetime.now()}")
        
        # Verify the insertion
        verify_query = f"""
            SELECT *
            FROM {constants.VALIDATION_LOG_TABLE}
            WHERE iteration_name = '{override_iteration}'
        """
        spark.sql(verify_query).show(truncate=False)
        
        print("\n" + "=" * 80)
        print("✅ OVERRIDE COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print(f"\nBatch '{batch_load_id}' for table '{target_table}' has been marked as {override_status}")
        print(f"This batch will be skipped in future validation runs.")
        print(f"\nAudit Information:")
        print(f"  - Reason: {reason}")
        print(f"  - Executed by: {current_user}")
        print(f"  - Timestamp: {datetime.now()}")
        print(f"  - Iteration: {override_iteration}")
        
    except Exception as e:
        print(f"\n❌ ERROR: Failed to insert override record")
        print(f"Error: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Override Effect

# COMMAND ----------

if not dry_run:
    print("=" * 80)
    print("STEP 6: VERIFY OVERRIDE EFFECT")
    print("=" * 80)
    
    # Verify that this batch will now be skipped
    verify_skip_query = f"""
        SELECT 
            batch_load_id,
            validation_run_status,
            validation_run_start_time,
            iteration_name,
            row_number() OVER(PARTITION BY batch_load_id ORDER BY validation_run_start_time DESC) as rnk
        FROM {constants.VALIDATION_LOG_TABLE}
        WHERE batch_load_id = '{batch_load_id}'
        ORDER BY validation_run_start_time DESC
    """
    
    verification_df = spark.sql(verify_skip_query)
    latest_record = verification_df.filter("rnk = 1").first()
    
    if latest_record and latest_record.validation_run_status in ['SUCCESS', 'SKIPPED']:
        print(f"✅ VERIFICATION PASSED")
        print(f"\nLatest validation status for batch '{batch_load_id}': {latest_record.validation_run_status}")
        print(f"This batch WILL BE SKIPPED in future validation runs.")
    else:
        print(f"⚠️  WARNING: Latest status is not SUCCESS/SKIPPED")
        print(f"Please review the validation_log table manually.")
    
    print("\nAll validation records for this batch:")
    verification_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples
# MAGIC 
# MAGIC ### Example 1: Mark Failed Validation as Success (After Manual Verification)
# MAGIC ```
# MAGIC Target Table: jio_home_prod.gold.customer_orders
# MAGIC Batch Load ID: 20241113_092145_abc123
# MAGIC Override Status: SUCCESS
# MAGIC Reason: Data reconciliation failed due to timezone mismatch. Manually verified data is correct. Approved by Data Quality team (Ticket: DQ-1234)
# MAGIC Dry Run: false
# MAGIC ```
# MAGIC 
# MAGIC ### Example 2: Skip Validation for Known Issue
# MAGIC ```
# MAGIC Target Table: jio_infra_prod.silver.network_events
# MAGIC Batch Load ID: 20241113_080000_def456
# MAGIC Override Status: SKIPPED
# MAGIC Reason: Source system outage caused incomplete data load. Business approved skipping this batch. Will be reprocessed in next cycle. (Ticket: INC-5678)
# MAGIC Dry Run: false
# MAGIC ```
# MAGIC 
# MAGIC ### Example 3: Preview Before Execution (Dry Run)
# MAGIC ```
# MAGIC Target Table: jio_mobility_prod.bronze.location_data
# MAGIC Batch Load ID: 20241113_103000_ghi789
# MAGIC Override Status: SUCCESS
# MAGIC Reason: Validation timeout due to large batch size. Data quality verified through alternative checks. Approved by Platform team.
# MAGIC Dry Run: true  (Set to false after review)
# MAGIC ```
# MAGIC 
# MAGIC ## Important Notes
# MAGIC 
# MAGIC 1. **Always use Dry Run first** - Review the preview before executing
# MAGIC 2. **Provide detailed reasons** - Include ticket numbers, approvals, and justification
# MAGIC 3. **Audit trail is critical** - All overrides are logged with user and timestamp
# MAGIC 4. **Verify manually first** - Ensure data quality before marking as SUCCESS
# MAGIC 5. **Get approval** - Follow your organization's approval process
# MAGIC 6. **Document externally** - Create tickets/documentation for manual overrides
# MAGIC 
# MAGIC ## Troubleshooting
# MAGIC 
# MAGIC ### Batch Not Found
# MAGIC - Verify batch_load_id is correct (check ingestion audit table)
# MAGIC - Ensure target table uses full name (catalog.schema.table)
# MAGIC - Check that batch completed ingestion successfully
# MAGIC 
# MAGIC ### Override Not Working
# MAGIC - Verify the record was inserted into validation_log
# MAGIC - Check that status is SUCCESS or SKIPPED
# MAGIC - Ensure timestamp is recent (latest record is used)
# MAGIC 
# MAGIC ## Support
# MAGIC 
# MAGIC For issues or questions, contact:
# MAGIC - Data Platform Team
# MAGIC - Databricks Support

# COMMAND ----------



