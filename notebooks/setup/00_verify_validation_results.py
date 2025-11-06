# Databricks notebook source
"""
Verification Notebook for End-to-End Testing Results

This notebook verifies that the validation framework executed successfully
and all results are as expected.

Run this AFTER:
1. 00_end_to_end_testing_setup.py
2. 02_setup_validation_mapping.py
3. deltarecon.runner (validation execution)
"""

# COMMAND ----------

# DBTITLE 1,Configuration
TEST_GROUP_NAME = "test_e2e_group"
TEST_CATALOG = "ts42_demo"
TEST_SCHEMA = "test_e2e_validation"

VALIDATION_LOG_TABLE = "cat_ril_nayeem_03.validation_v2.validation_log"
VALIDATION_SUMMARY_TABLE = "cat_ril_nayeem_03.validation_v2.validation_summary"
VALIDATION_MAPPING_TABLE = "cat_ril_nayeem_03.validation_v2.validation_mapping"
INGESTION_CONFIG_TABLE = "ts42_demo.migration_operations.serving_ingestion_config"
INGESTION_METADATA_TABLE = "ts42_demo.migration_operations.serving_ingestion_metadata"

print(f"Verifying results for test group: {TEST_GROUP_NAME}")

# COMMAND ----------

# DBTITLE 1,Check 1: Validation Mapping Exists

print("=" * 80)
print("CHECK 1: Validation Mapping Configuration")
print("=" * 80)

mapping_df = spark.sql(f"""
    SELECT 
        table_group,
        workflow_name,
        src_table,
        tgt_table,
        tgt_primary_keys,
        validation_is_active
    FROM {VALIDATION_MAPPING_TABLE}
    WHERE table_group = '{TEST_GROUP_NAME}'
    ORDER BY tgt_table
""")

mapping_count = mapping_df.count()
print(f"\n✓ Found {mapping_count} validation mappings")
print(f"Expected: 6 mappings\n")

if mapping_count == 6:
    print("✅ PASS: All 6 validation mappings exist")
else:
    print(f"❌ FAIL: Expected 6 mappings, found {mapping_count}")

mapping_df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Check 2: Validation Execution Status

print("\n" + "=" * 80)
print("CHECK 2: Validation Execution Status")
print("=" * 80)

log_df = spark.sql(f"""
    SELECT 
        batch_load_id,
        tgt_table,
        validation_run_status,
        validation_run_start_time,
        validation_run_end_time,
        exception,
        workflow_name
    FROM {VALIDATION_LOG_TABLE}
    WHERE table_family LIKE '%{TEST_SCHEMA}%'
    ORDER BY validation_run_start_time DESC, tgt_table
""")

log_count = log_df.count()
print(f"\n✓ Found {log_count} validation log entries")

# Check for failed runs
failed_runs = log_df.filter("validation_run_status != 'SUCCESS'")
failed_count = failed_runs.count()

if failed_count > 0:
    print(f"\n❌ FAIL: {failed_count} validations failed")
    print("\nFailed Validations:")
    failed_runs.select("tgt_table", "validation_run_status", "exception").show(truncate=False)
else:
    print("\n✅ PASS: All validations completed successfully")

# Show recent runs
print("\nRecent Validation Runs:")
log_df.limit(10).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Check 3: Validation Summary Results

print("\n" + "=" * 80)
print("CHECK 3: Validation Summary - Detailed Results")
print("=" * 80)

summary_df = spark.sql(f"""
    SELECT 
        batch_load_id,
        tgt_table,
        row_count_match_status,
        schema_match_status,
        primary_key_compliance_status,
        col_name_compare_status,
        data_type_compare_status,
        data_reconciliation_status,
        overall_status,
        metrics.src_records,
        metrics.tgt_records,
        metrics.src_extras,
        metrics.tgt_extras,
        metrics.mismatches,
        metrics.matches
    FROM {VALIDATION_SUMMARY_TABLE}
    WHERE table_family LIKE '%{TEST_SCHEMA}%'
    ORDER BY tgt_table
""")

summary_count = summary_df.count()
print(f"\n✓ Found {summary_count} validation summary entries")

# Check each validation type
validation_checks = [
    ("row_count_match_status", "Row Count"),
    ("schema_match_status", "Schema"),
    ("primary_key_compliance_status", "Primary Key"),
    ("col_name_compare_status", "Column Name"),
    ("data_type_compare_status", "Data Type"),
    ("data_reconciliation_status", "Data Reconciliation"),
    ("overall_status", "Overall")
]

print("\nValidation Check Results:")
print("-" * 80)

all_passed = True
for col, name in validation_checks:
    failed = summary_df.filter(f"{col} NOT IN ('PASSED', 'SUCCESS', 'SKIPPED')").count()
    passed = summary_df.filter(f"{col} IN ('PASSED', 'SUCCESS')").count()
    skipped = summary_df.filter(f"{col} = 'SKIPPED'").count()
    
    status = "✅ PASS" if failed == 0 else "❌ FAIL"
    print(f"{status} {name:25} - Passed: {passed}, Skipped: {skipped}, Failed: {failed}")
    
    if failed > 0:
        all_passed = False

if all_passed:
    print("\n✅ PASS: All validation checks passed")
else:
    print("\n❌ FAIL: Some validation checks failed")

print("\nDetailed Summary:")
summary_df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Check 4: Row Count Verification

print("\n" + "=" * 80)
print("CHECK 4: Row Count Verification")
print("=" * 80)

print("\nExpected Row Counts:")
expected_counts = {
    f"{TEST_CATALOG}.{TEST_SCHEMA}.orders": 6,
    f"{TEST_CATALOG}.{TEST_SCHEMA}.inventory": 5,
    f"{TEST_CATALOG}.{TEST_SCHEMA}.customers": 5,
    f"{TEST_CATALOG}.{TEST_SCHEMA}.transactions": 5,
    f"{TEST_CATALOG}.{TEST_SCHEMA}.employees": 5,
    f"{TEST_CATALOG}.{TEST_SCHEMA}.sales_metrics": 5
}

print(f"{'Table':<60} {'Expected':<12} {'Actual':<12} {'Match':<10}")
print("-" * 94)

all_match = True
for table, expected in expected_counts.items():
    try:
        actual = spark.table(table).count()
        match = "✅ Yes" if actual == expected else "❌ No"
        print(f"{table:<60} {expected:<12} {actual:<12} {match:<10}")
        if actual != expected:
            all_match = False
    except Exception as e:
        print(f"{table:<60} {expected:<12} {'ERROR':<12} {'❌ No':<10}")
        print(f"  Error: {str(e)}")
        all_match = False

if all_match:
    print("\n✅ PASS: All row counts match expected values")
else:
    print("\n❌ FAIL: Some row counts don't match")

# COMMAND ----------

# DBTITLE 1,Check 5: Source File Format Verification

print("\n" + "=" * 80)
print("CHECK 5: Source File Format Configuration")
print("=" * 80)

config_df = spark.sql(f"""
    SELECT 
        config_id,
        target_table,
        source_file_format,
        source_file_options,
        primary_key,
        partition_column
    FROM {INGESTION_CONFIG_TABLE}
    WHERE group_name = '{TEST_GROUP_NAME}'
    ORDER BY config_id
""")

print("\nConfigured Source Formats:")
config_df.show(truncate=False)

# Verify format distribution
format_dist = config_df.groupBy("source_file_format").count().orderBy("source_file_format")
print("\nFormat Distribution:")
format_dist.show()

expected_formats = {"orc": 2, "text": 2, "csv": 2}
actual_formats = {row.source_file_format: row["count"] for row in format_dist.collect()}

format_match = True
for fmt, expected_count in expected_formats.items():
    actual_count = actual_formats.get(fmt, 0)
    status = "✅" if actual_count == expected_count else "❌"
    print(f"{status} {fmt.upper():6} format: Expected {expected_count}, Found {actual_count}")
    if actual_count != expected_count:
        format_match = False

if format_match:
    print("\n✅ PASS: Source format distribution is correct")
else:
    print("\n❌ FAIL: Source format distribution doesn't match expected")

# COMMAND ----------

# DBTITLE 1,Check 6: Data Reconciliation Metrics

print("\n" + "=" * 80)
print("CHECK 6: Data Reconciliation Metrics")
print("=" * 80)

metrics_df = spark.sql(f"""
    SELECT 
        tgt_table,
        metrics.src_records as source_records,
        metrics.tgt_records as target_records,
        metrics.src_extras as records_only_in_source,
        metrics.tgt_extras as records_only_in_target,
        metrics.mismatches as data_mismatches,
        metrics.matches as matching_records,
        data_reconciliation_status
    FROM {VALIDATION_SUMMARY_TABLE}
    WHERE table_family LIKE '%{TEST_SCHEMA}%'
    ORDER BY tgt_table
""")

print("\nData Reconciliation Metrics:")
metrics_df.show(truncate=False)

# Check for perfect matches
perfect_match_count = metrics_df.filter(
    "source_records = target_records AND "
    "records_only_in_source = 0 AND "
    "records_only_in_target = 0"
).count()

total_tables = metrics_df.count()

if perfect_match_count == total_tables:
    print(f"\n✅ PASS: All {total_tables} tables have perfect data reconciliation")
else:
    print(f"\n⚠️  WARNING: {total_tables - perfect_match_count} tables have data differences")
    
    # Show tables with issues
    issues_df = metrics_df.filter(
        "source_records != target_records OR "
        "records_only_in_source != 0 OR "
        "records_only_in_target != 0"
    )
    print("\nTables with Data Differences:")
    issues_df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Check 7: Ingestion Metadata Tracking

print("\n" + "=" * 80)
print("CHECK 7: Ingestion Metadata File Tracking")
print("=" * 80)

metadata_df = spark.sql(f"""
    SELECT 
        table_name,
        batch_load_id,
        source_file_path,
        file_size,
        is_processed,
        insert_ts
    FROM {INGESTION_METADATA_TABLE}
    WHERE table_name LIKE '{TEST_CATALOG}.{TEST_SCHEMA}.%'
    ORDER BY table_name
""")

metadata_count = metadata_df.count()
print(f"\n✓ Found {metadata_count} ingestion metadata entries")
print(f"Expected: 6 entries\n")

if metadata_count == 6:
    print("✅ PASS: All source files tracked in ingestion metadata")
else:
    print(f"❌ FAIL: Expected 6 metadata entries, found {metadata_count}")

print("\nIngestion Metadata Details:")
metadata_df.show(truncate=False)

# Check all files are marked as processed
unprocessed = metadata_df.filter("is_processed != 'Y'").count()
if unprocessed > 0:
    print(f"\n⚠️  WARNING: {unprocessed} files not marked as processed")
else:
    print("\n✅ All files marked as processed")

# COMMAND ----------

# DBTITLE 1,Check 8: Source File Options Parsing

print("\n" + "=" * 80)
print("CHECK 8: Source File Options Parsing")
print("=" * 80)

import json

options_df = spark.sql(f"""
    SELECT 
        config_id,
        target_table,
        source_file_format,
        source_file_options
    FROM {INGESTION_CONFIG_TABLE}
    WHERE group_name = '{TEST_GROUP_NAME}'
        AND source_file_options IS NOT NULL
    ORDER BY config_id
""")

print("\nTables with Source File Options:")
print(f"Found: {options_df.count()} tables with options\n")

for row in options_df.collect():
    print(f"Table: {row.target_table}")
    print(f"Format: {row.source_file_format}")
    
    if row.source_file_options:
        try:
            options = json.loads(row.source_file_options)
            print(f"Options: {json.dumps(options, indent=2)}")
            print("✅ Valid JSON")
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON: {e}")
    else:
        print("No options configured")
    
    print("-" * 80)

# COMMAND ----------

# DBTITLE 1,Overall Test Summary

print("\n" + "=" * 80)
print("OVERALL TEST SUMMARY")
print("=" * 80)

# Collect all check results
checks = []

# Check 1: Mapping exists
checks.append(("Validation Mappings", mapping_count == 6))

# Check 2: No failed runs
checks.append(("Validation Execution", failed_count == 0))

# Check 3: All validations passed
checks.append(("Validation Checks", all_passed))

# Check 4: Row counts match
checks.append(("Row Count Verification", all_match))

# Check 5: Format distribution
checks.append(("Source Format Config", format_match))

# Check 6: Data reconciliation
checks.append(("Data Reconciliation", perfect_match_count == total_tables))

# Check 7: Metadata tracking
checks.append(("Ingestion Metadata", metadata_count == 6 and unprocessed == 0))

# Print summary
print("\nTest Results:")
print("-" * 80)

passed_count = 0
total_checks = len(checks)

for check_name, passed in checks:
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"{status} {check_name}")
    if passed:
        passed_count += 1

print("-" * 80)
print(f"\nTotal: {passed_count}/{total_checks} checks passed")

if passed_count == total_checks:
    print("\n🎉 SUCCESS: All end-to-end tests passed!")
    print("\nThe validation framework is working correctly with:")
    print("  ✓ ORC source files")
    print("  ✓ TEXT source files (tab and pipe delimited)")
    print("  ✓ CSV source files (with headers)")
    print("  ✓ Partitioned and non-partitioned tables")
    print("  ✓ Multiple data types (INT, STRING, DOUBLE, DATE, TIMESTAMP)")
    print("  ✓ Complete metadata tracking")
    print("  ✓ Data reconciliation validation")
else:
    print(f"\n⚠️  WARNING: {total_checks - passed_count} checks failed")
    print("\nPlease review the failed checks above and:")
    print("  1. Ensure 00_end_to_end_testing_setup.py ran successfully")
    print("  2. Verify 02_setup_validation_mapping.py created mappings")
    print("  3. Check that the validation runner executed without errors")
    print("  4. Review validation_log for any exceptions")

# COMMAND ----------

# DBTITLE 1,Sample Queries for Manual Verification

print("\n" + "=" * 80)
print("SAMPLE QUERIES FOR MANUAL VERIFICATION")
print("=" * 80)

queries = {
    "View All Test Tables": f"""
        SELECT table_catalog, table_schema, table_name, table_type
        FROM system.information_schema.tables
        WHERE table_schema = '{TEST_SCHEMA}'
        ORDER BY table_name
    """,
    
    "Latest Validation Results": f"""
        SELECT 
            vs.tgt_table,
            vs.overall_status,
            vs.row_count_match_status,
            vs.schema_match_status,
            vs.primary_key_compliance_status,
            vs.data_reconciliation_status,
            vs.metrics.src_records,
            vs.metrics.tgt_records
        FROM {VALIDATION_SUMMARY_TABLE} vs
        WHERE vs.table_family LIKE '%{TEST_SCHEMA}%'
        ORDER BY vs.tgt_table
    """,
    
    "Validation Execution Timeline": f"""
        SELECT 
            tgt_table,
            validation_run_start_time,
            validation_run_end_time,
            TIMESTAMPDIFF(SECOND, validation_run_start_time, validation_run_end_time) as duration_seconds,
            validation_run_status
        FROM {VALIDATION_LOG_TABLE}
        WHERE table_family LIKE '%{TEST_SCHEMA}%'
        ORDER BY validation_run_start_time DESC
    """,
    
    "Source File Configuration": f"""
        SELECT 
            config_id,
            target_table,
            source_file_format,
            source_file_path,
            primary_key,
            partition_column
        FROM {INGESTION_CONFIG_TABLE}
        WHERE group_name = '{TEST_GROUP_NAME}'
        ORDER BY target_table
    """
}

for query_name, query in queries.items():
    print(f"\n{query_name}:")
    print("-" * 80)
    print(query)
    print()

print("Copy and run these queries in a SQL notebook for detailed investigation.")

# COMMAND ----------

print("\n" + "=" * 80)
print("✅ VERIFICATION COMPLETE")
print("=" * 80)

