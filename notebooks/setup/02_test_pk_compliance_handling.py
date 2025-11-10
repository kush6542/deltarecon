# Databricks notebook source
"""
PK Compliance Handling E2E Test

This notebook tests the new PK compliance handling feature where:
- PK failures are recorded in validation_summary
- PK failures do NOT affect overall_status
- Batches with only PK failures are NOT re-validated

Test Scenarios:
1. Table with PASSING PK check (no duplicates)
2. Table with FAILING PK check (has duplicates)
3. Verify overall_status = SUCCESS even when PK fails
4. Verify batch is NOT re-picked in next run
"""

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime, timedelta
import json

# COMMAND ----------

# DBTITLE 1,Configuration
# Test configuration
TEST_CATALOG = "ts42_demo"
TEST_SCHEMA = "test_pk_compliance"
TEST_GROUP_NAME = "test_pk_compliance_group"
BATCH_LOAD_ID_PASS = f"BATCH_PK_PASS_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
BATCH_LOAD_ID_FAIL = f"BATCH_PK_FAIL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# DBFS paths for source files
DBFS_BASE_PATH = "/FileStore/delta_recon_pk_test"

# Metadata tables
INGESTION_CONFIG_TABLE = "ts42_demo.migration_operations.serving_ingestion_config"
INGESTION_METADATA_TABLE = "ts42_demo.migration_operations.serving_ingestion_metadata"
INGESTION_AUDIT_TABLE = "ts42_demo.migration_operations.serving_ingestion_audit"

print(f"Test Catalog: {TEST_CATALOG}")
print(f"Test Schema: {TEST_SCHEMA}")
print(f"Test Group: {TEST_GROUP_NAME}")
print(f"Batch Load ID (PASS): {BATCH_LOAD_ID_PASS}")
print(f"Batch Load ID (FAIL): {BATCH_LOAD_ID_FAIL}")
print(f"DBFS Base Path: {DBFS_BASE_PATH}")

# COMMAND ----------

# DBTITLE 1,Cleanup Existing Test Data
print("=" * 80)
print("CLEANING UP EXISTING TEST DATA")
print("=" * 80)

CLEAN_ALL = True  # Set to False to skip cleanup

if CLEAN_ALL:
    print("\n1. Dropping test schema (if exists)...")
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {TEST_CATALOG}.{TEST_SCHEMA} CASCADE")
        print(f"   ✓ Dropped schema: {TEST_CATALOG}.{TEST_SCHEMA}")
    except Exception as e:
        print(f"   ⚠ Could not drop schema: {e}")
    
    print("\n2. Removing DBFS files (if exist)...")
    try:
        dbutils.fs.rm(DBFS_BASE_PATH, recurse=True)
        print(f"   ✓ Removed DBFS path: {DBFS_BASE_PATH}")
    except Exception as e:
        print(f"   ⚠ Path not found or already clean: {e}")
    
    print("\n3. Cleaning metadata tables...")
    
    # Clean ingestion_config
    try:
        spark.sql(f"""
            DELETE FROM {INGESTION_CONFIG_TABLE}
            WHERE group_name = '{TEST_GROUP_NAME}'
        """)
        print(f"   ✓ Cleaned {INGESTION_CONFIG_TABLE}")
    except Exception as e:
        print(f"   ⚠ Could not clean ingestion_config: {e}")
    
    # Clean ingestion_metadata
    try:
        spark.sql(f"""
            DELETE FROM {INGESTION_METADATA_TABLE}
            WHERE table_name LIKE '{TEST_CATALOG}.{TEST_SCHEMA}.%'
        """)
        print(f"   ✓ Cleaned {INGESTION_METADATA_TABLE}")
    except Exception as e:
        print(f"   ⚠ Could not clean ingestion_metadata: {e}")
    
    # Clean ingestion_audit
    try:
        spark.sql(f"""
            DELETE FROM {INGESTION_AUDIT_TABLE}
            WHERE group_name = '{TEST_GROUP_NAME}'
        """)
        print(f"   ✓ Cleaned {INGESTION_AUDIT_TABLE}")
    except Exception as e:
        print(f"   ⚠ Could not clean ingestion_audit: {e}")
    
    print("\n✓ Cleanup completed!")
    print("=" * 80)
else:
    print("\n⚠ Cleanup skipped (CLEAN_ALL = False)")
    print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Create Test Schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {TEST_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_CATALOG}.{TEST_SCHEMA}")
print(f"✓ Created schema: {TEST_CATALOG}.{TEST_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Define Sample Data - Table 1: Products with NO Duplicates (PASS)

products_pass_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True)
])

# CLEAN DATA: No duplicate product_ids
products_pass_data = [
    (1001, "Laptop Pro", "Electronics", 1299.99, 50),
    (1002, "Wireless Mouse", "Accessories", 29.99, 200),
    (1003, "USB-C Cable", "Accessories", 15.99, 500),
    (1004, "Monitor 27inch", "Electronics", 399.99, 75),
    (1005, "Keyboard Mechanical", "Accessories", 89.99, 150),
]

print("✓ Products (PASS) - No duplicate PKs defined")

# COMMAND ----------

# DBTITLE 1,Define Sample Data - Table 2: Orders with Duplicates (FAIL)

orders_fail_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

# DIRTY DATA: Contains duplicate order_ids (2001 appears 2x, 2003 appears 2x)
# This will cause PK validation to FAIL
orders_fail_data = [
    (2001, 101, datetime(2024, 11, 1).date(), 250.50, "COMPLETED"),
    (2002, 102, datetime(2024, 11, 1).date(), 175.25, "PENDING"),
    (2001, 103, datetime(2024, 11, 2).date(), 999.99, "COMPLETED"),  # DUPLICATE order_id=2001
    (2003, 104, datetime(2024, 11, 2).date(), 89.99, "CANCELLED"),
    (2003, 105, datetime(2024, 11, 3).date(), 320.75, "COMPLETED"),  # DUPLICATE order_id=2003
    (2004, 106, datetime(2024, 11, 3).date(), 125.00, "PENDING"),
]

print("✓ Orders (FAIL) - Contains duplicate PKs (2001 x2, 2003 x2)")

# COMMAND ----------

# DBTITLE 1,Create Source Files - Products (PASS)

products_pass_df = spark.createDataFrame(products_pass_data, products_pass_schema)
products_pass_path = f"{DBFS_BASE_PATH}/orc/products_pass/{BATCH_LOAD_ID_PASS}"
products_pass_df.write.mode("overwrite").format("orc").save(products_pass_path)
print(f"✓ Created ORC file: {products_pass_path}")
print(f"  Rows: {products_pass_df.count()}")
print(f"  Distinct PKs: {products_pass_df.select('product_id').distinct().count()}")

# COMMAND ----------

# DBTITLE 1,Create Source Files - Orders (FAIL)

orders_fail_df = spark.createDataFrame(orders_fail_data, orders_fail_schema)
orders_fail_path = f"{DBFS_BASE_PATH}/orc/orders_fail/{BATCH_LOAD_ID_FAIL}"
orders_fail_df.write.mode("overwrite").format("orc").save(orders_fail_path)
print(f"✓ Created ORC file: {orders_fail_path}")
print(f"  Rows: {orders_fail_df.count()}")
print(f"  Distinct PKs: {orders_fail_df.select('order_id').distinct().count()}")
print(f"  ⚠ WARNING: This file has DUPLICATE primary keys!")

# COMMAND ----------

# DBTITLE 1,Create Target Delta Tables

# Table 1: Products (PASS) - with audit column
products_pass_target = f"{TEST_CATALOG}.{TEST_SCHEMA}.products_pass"
products_pass_df_with_audit = products_pass_df.withColumn("_aud_batch_load_id", lit(BATCH_LOAD_ID_PASS))
products_pass_df_with_audit.write.mode("overwrite").format("delta").saveAsTable(products_pass_target)
print(f"✓ Created target table: {products_pass_target}")

# Table 2: Orders (FAIL) - with audit column and duplicates
orders_fail_target = f"{TEST_CATALOG}.{TEST_SCHEMA}.orders_fail"
orders_fail_df_with_audit = orders_fail_df.withColumn("_aud_batch_load_id", lit(BATCH_LOAD_ID_FAIL))
orders_fail_df_with_audit.write.mode("overwrite").format("delta").saveAsTable(orders_fail_target)
print(f"✓ Created target table: {orders_fail_target}")
print(f"  ⚠ WARNING: This table has DUPLICATE primary keys!")

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_config

def create_config_entry(config_id, table_name, source_file_path, batch_id, primary_key):
    return {
        "config_id": config_id,
        "group_name": TEST_GROUP_NAME,
        "source_schema": "source_system",
        "source_table": table_name,
        "source_file_path": source_file_path,
        "target_catalog": TEST_CATALOG,
        "target_schema": TEST_SCHEMA,
        "target_table": table_name,
        "source_file_format": "orc",
        "source_file_options": None,
        "load_type": "incremental",
        "write_mode": "append",
        "primary_key": primary_key,
        "partition_column": None,
        "partitioning_strategy": None,
        "frequency": "daily",
        "table_size_gb": "1",
        "is_active": "Y",
        "deduplicate": "N",
        "clean_column_names": "N",
        "insert_ts": datetime.now(),
        "last_update_ts": datetime.now()
    }

config_entries = [
    # Test 1: Products with clean PKs (should PASS)
    create_config_entry(
        config_id="TEST_PK_001",
        table_name="products_pass",
        source_file_path=products_pass_path,
        batch_id=BATCH_LOAD_ID_PASS,
        primary_key="product_id"
    ),
    
    # Test 2: Orders with duplicate PKs (PK check will FAIL, but overall should be SUCCESS)
    create_config_entry(
        config_id="TEST_PK_002",
        table_name="orders_fail",
        source_file_path=orders_fail_path,
        batch_id=BATCH_LOAD_ID_FAIL,
        primary_key="order_id"
    ),
]

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

ingestion_config_schema = StructType([
    StructField('config_id', StringType(), True),
    StructField('group_name', StringType(), True),
    StructField('source_schema', StringType(), True),
    StructField('source_table', StringType(), True),
    StructField('source_file_path', StringType(), True),
    StructField('target_catalog', StringType(), True),
    StructField('target_schema', StringType(), True),
    StructField('target_table', StringType(), True),
    StructField('source_file_format', StringType(), True),
    StructField('source_file_options', StringType(), True),
    StructField('load_type', StringType(), True),
    StructField('write_mode', StringType(), True),
    StructField('primary_key', StringType(), True),
    StructField('partition_column', StringType(), True),
    StructField('partitioning_strategy', StringType(), True),
    StructField('frequency', StringType(), True),
    StructField('table_size_gb', StringType(), True),
    StructField('is_active', StringType(), True),
    StructField('deduplicate', StringType(), True),
    StructField('clean_column_names', StringType(), True),
    StructField('insert_ts', TimestampType(), True),
    StructField('last_update_ts', TimestampType(), True),
])

config_df = spark.createDataFrame(config_entries, schema=ingestion_config_schema)
config_df.write.mode("append").saveAsTable(INGESTION_CONFIG_TABLE)
print(f"✓ Inserted {len(config_entries)} entries into {INGESTION_CONFIG_TABLE}")

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_metadata

import os

def get_all_data_files(path):
    """Get ALL data files from a directory"""
    try:
        file_info = dbutils.fs.ls(path)
        data_files = []
        for f in file_info:
            if not f.name.startswith('_') and f.size > 0:
                data_files.append((f.path, f.modificationTime, f.size))
        return data_files
    except Exception as e:
        print(f"Warning: Could not list files in {path}: {e}")
        return []

# Create metadata entries
metadata_entries = []

file_paths = [
    (products_pass_path, "products_pass", BATCH_LOAD_ID_PASS),
    (orders_fail_path, "orders_fail", BATCH_LOAD_ID_FAIL),
]

for dir_path, table_name, batch_id in file_paths:
    all_files = get_all_data_files(dir_path)
    print(f"  {table_name}: Found {len(all_files)} file(s)")
    
    for source_file, mod_time, file_size in all_files:
        metadata_entries.append({
            "table_name": f"{TEST_CATALOG}.{TEST_SCHEMA}.{table_name}",
            "batch_load_id": batch_id,
            "source_file_path": source_file,
            "file_modification_time": datetime.fromtimestamp(mod_time / 1000),
            "file_size": file_size,
            "is_processed": "Y",
            "insert_ts": datetime.now(),
            "last_update_ts": datetime.now()
        })

metadata_df = spark.createDataFrame(metadata_entries)
metadata_df.write.mode("append").saveAsTable(INGESTION_METADATA_TABLE)
print(f"✓ Inserted {len(metadata_entries)} file entries into {INGESTION_METADATA_TABLE}")

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_audit

from pyspark.sql.types import IntegerType, LongType

ingestion_audit_schema = StructType([
    StructField('run_id', StringType(), True),
    StructField('config_id', StringType(), True),
    StructField('batch_load_id', StringType(), True),
    StructField('group_name', StringType(), True),
    StructField('target_table_name', StringType(), True),
    StructField('operation_type', StringType(), True),
    StructField('load_type', StringType(), True),
    StructField('status', StringType(), True),
    StructField('start_ts', TimestampType(), True),
    StructField('end_ts', TimestampType(), True),
    StructField('log_message', StringType(), True),
    StructField('microbatch_id', IntegerType(), True),
    StructField('row_count', LongType(), True),
])

audit_entries = [
    {
        "run_id": f"RUN_{BATCH_LOAD_ID_PASS}",
        "config_id": "TEST_PK_001",
        "batch_load_id": BATCH_LOAD_ID_PASS,
        "group_name": TEST_GROUP_NAME,
        "target_table_name": f"{TEST_CATALOG}.{TEST_SCHEMA}.products_pass",
        "operation_type": "LOAD",
        "load_type": "BATCH",
        "status": "COMPLETED",
        "start_ts": datetime.now() - timedelta(minutes=5),
        "end_ts": datetime.now(),
        "log_message": "Test data loaded successfully (clean PKs)",
        "microbatch_id": None,
        "row_count": 5
    },
    {
        "run_id": f"RUN_{BATCH_LOAD_ID_FAIL}",
        "config_id": "TEST_PK_002",
        "batch_load_id": BATCH_LOAD_ID_FAIL,
        "group_name": TEST_GROUP_NAME,
        "target_table_name": f"{TEST_CATALOG}.{TEST_SCHEMA}.orders_fail",
        "operation_type": "LOAD",
        "load_type": "BATCH",
        "status": "COMPLETED",
        "start_ts": datetime.now() - timedelta(minutes=5),
        "end_ts": datetime.now(),
        "log_message": "Test data loaded successfully (HAS DUPLICATE PKs!)",
        "microbatch_id": None,
        "row_count": 6
    },
]

audit_df = spark.createDataFrame(audit_entries, schema=ingestion_audit_schema)
audit_df.write.mode("append").saveAsTable(INGESTION_AUDIT_TABLE)
print(f"✓ Inserted {len(audit_entries)} entries into {INGESTION_AUDIT_TABLE}")

# COMMAND ----------

# DBTITLE 1,Verify PK Duplicates in Test Data

print("\n" + "=" * 80)
print("VERIFICATION: Checking for PK Duplicates in Test Data")
print("=" * 80)

# Check products_pass (should have NO duplicates)
print("\n1. products_pass (Expected: NO duplicates)")
products_dup_check = spark.sql(f"""
    SELECT 
        product_id,
        COUNT(*) as duplicate_count
    FROM {TEST_CATALOG}.{TEST_SCHEMA}.products_pass
    GROUP BY product_id
    HAVING COUNT(*) > 1
""")

dup_count = products_dup_check.count()
if dup_count == 0:
    print("   ✓ NO duplicate product_ids found (PK check will PASS)")
else:
    print(f"   ⚠ Found {dup_count} duplicate product_ids:")
    products_dup_check.show()

# Check orders_fail (SHOULD have duplicates)
print("\n2. orders_fail (Expected: HAS duplicates)")
orders_dup_check = spark.sql(f"""
    SELECT 
        order_id,
        COUNT(*) as duplicate_count
    FROM {TEST_CATALOG}.{TEST_SCHEMA}.orders_fail
    GROUP BY order_id
    HAVING COUNT(*) > 1
    ORDER BY duplicate_count DESC
""")

dup_count = orders_dup_check.count()
if dup_count > 0:
    print(f"   ✓ Found {dup_count} duplicate order_ids (PK check will FAIL):")
    orders_dup_check.show()
else:
    print("   ❌ NO duplicates found - Test data is WRONG!")

print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Summary and Next Steps

print("\n" + "=" * 80)
print("✅ PK COMPLIANCE TEST SETUP COMPLETE")
print("=" * 80)

print(f"""
Test Environment Summary:
------------------------
• Test Group: {TEST_GROUP_NAME}
• Test Schema: {TEST_CATALOG}.{TEST_SCHEMA}

Created Test Cases:
------------------
✓ Test Case 1: products_pass (Batch: {BATCH_LOAD_ID_PASS})
  - PK: product_id
  - Expected: PK validation PASSES
  - Expected: overall_status = SUCCESS
  - Expected: primary_key_compliance_status = PASSED

✓ Test Case 2: orders_fail (Batch: {BATCH_LOAD_ID_FAIL})
  - PK: order_id
  - Expected: PK validation FAILS (duplicate order_ids: 2001, 2003)
  - Expected: overall_status = SUCCESS (PK failures don't affect overall status!)
  - Expected: primary_key_compliance_status = FAILED
  - Expected: Batch will NOT be re-validated in next run

Next Steps:
-----------
1. Run validation mapping setup:
   notebooks/setup/02_setup_validation_mapping.py
   
2. Run validation framework:
   python deltarecon/runner.py --table-group {TEST_GROUP_NAME}

3. CRITICAL: Check validation results to verify new PK handling:

   -- Check validation_summary for BOTH batches:
   SELECT 
     batch_load_id,
     tgt_table,
     overall_status,                      -- Should be SUCCESS for BOTH
     primary_key_compliance_status,       -- PASSED for products, FAILED for orders
     row_count_match_status,
     schema_match_status
   FROM ts42_demo.validation_v2.validation_summary
   WHERE table_family LIKE '%{TEST_SCHEMA}%'
   ORDER BY tgt_table;

   -- Check validation_log:
   SELECT 
     batch_load_id,
     tgt_table,
     validation_run_status,               -- Should be SUCCESS for BOTH
     validation_run_start_time
   FROM ts42_demo.validation_v2.validation_log
   WHERE table_family LIKE '%{TEST_SCHEMA}%'
   ORDER BY tgt_table;

4. Re-run validation framework AGAIN (same command):
   python deltarecon/runner.py --table-group {TEST_GROUP_NAME}
   
   Expected: NO batches should be picked up (because both have overall_status=SUCCESS)

5. Check logs for warning message:
   Look for: "PK Compliance Check FAILED - recorded in summary but not affecting overall status"

Expected Results:
----------------
✓ products_pass batch:
  - overall_status = SUCCESS
  - primary_key_compliance_status = PASSED
  - Batch NOT re-validated

✓ orders_fail batch (THE KEY TEST!):
  - overall_status = SUCCESS (despite PK failure!)
  - primary_key_compliance_status = FAILED
  - Batch NOT re-validated
  - Warning logged but validation continues

This tests the new feature: PK failures are recorded but don't block validation completion!
""")

print("\n✅ Ready for PK compliance validation testing!")

# COMMAND ----------

# DBTITLE 1,Show Metadata Entries

print("\n" + "=" * 80)
print("INGESTION CONFIG ENTRIES")
print("=" * 80)

config_check = spark.sql(f"""
    SELECT config_id, target_table, primary_key, write_mode
    FROM {INGESTION_CONFIG_TABLE}
    WHERE group_name = '{TEST_GROUP_NAME}'
    ORDER BY config_id
""")
config_check.show(truncate=False)

print("\n" + "=" * 80)
print("INGESTION METADATA ENTRIES")
print("=" * 80)

metadata_check = spark.sql(f"""
    SELECT table_name, batch_load_id, is_processed
    FROM {INGESTION_METADATA_TABLE}
    WHERE table_name LIKE '{TEST_CATALOG}.{TEST_SCHEMA}.%'
    ORDER BY table_name
""")
metadata_check.show(truncate=False)

print("\n" + "=" * 80)
print("INGESTION AUDIT ENTRIES")
print("=" * 80)

audit_check = spark.sql(f"""
    SELECT target_table_name, status, row_count, log_message
    FROM {INGESTION_AUDIT_TABLE}
    WHERE group_name = '{TEST_GROUP_NAME}'
    ORDER BY target_table_name
""")
audit_check.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Display Test Data

print("\n" + "=" * 80)
print("TEST DATA PREVIEW")
print("=" * 80)

print("\n1. products_pass (Clean PKs - Should PASS):")
print("-" * 80)
spark.table(f"{TEST_CATALOG}.{TEST_SCHEMA}.products_pass").orderBy("product_id").show(truncate=False)

print("\n2. orders_fail (Duplicate PKs - Will FAIL PK check):")
print("-" * 80)
spark.table(f"{TEST_CATALOG}.{TEST_SCHEMA}.orders_fail").orderBy("order_id").show(truncate=False)

print("\nNOTE: order_id 2001 and 2003 appear twice - PK validation will detect this!")

# COMMAND ----------

