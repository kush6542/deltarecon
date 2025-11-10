# Databricks notebook source
"""
Test Notebook for Write Mode Fix - Self-Contained Test Environment

This notebook creates its own test tables and data to test the write mode fixes
made to source_target_loader.py. No access to production tables required!

WHAT WAS FIXED:
--------------
1. OVERWRITE mode: Now loads ALL target data (entire table) instead of filtering by batch_id
2. PARTITION_OVERWRITE/APPEND/MERGE: Now uses SQL WHERE clause for performance
3. Configuration validation: Prevents partition_overwrite without partition columns

TEST SCENARIOS CREATED:
----------------------
1. OVERWRITE mode table: Multiple batches ingested, but only latest exists
2. PARTITION_OVERWRITE mode table: Multiple batches with different partitions
3. APPEND mode table: Multiple batches that all coexist
4. Invalid config: partition_overwrite without partitions

Author: DeltaRecon Framework
Date: 2024-11-10
"""

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import lit, current_timestamp
from datetime import datetime, timedelta
import time

from deltarecon.core.source_target_loader import SourceTargetLoader
from deltarecon.models.table_config import TableConfig
from deltarecon.utils.exceptions import ConfigurationError, DataLoadError

print("✓ Libraries imported successfully")

# COMMAND ----------

# DBTITLE 1,Configuration
print("="*80)
print("TEST CONFIGURATION")
print("="*80)

# Test configuration
TEST_CATALOG = "ts42_demo"
TEST_SCHEMA = "test_write_mode_fix"
TEST_GROUP = "test_write_mode"

# Generate unique batch IDs
BATCH_1 = f"BATCH_1_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
BATCH_2 = f"BATCH_2_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
BATCH_3 = f"BATCH_3_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# DBFS paths for source files
DBFS_BASE_PATH = "/FileStore/write_mode_test"

# Metadata tables
INGESTION_CONFIG_TABLE = "ts42_demo.migration_operations.serving_ingestion_config"
INGESTION_METADATA_TABLE = "ts42_demo.migration_operations.serving_ingestion_metadata"
INGESTION_AUDIT_TABLE = "ts42_demo.migration_operations.serving_ingestion_audit"

print(f"\nTest Catalog: {TEST_CATALOG}")
print(f"Test Schema: {TEST_SCHEMA}")
print(f"Test Group: {TEST_GROUP}")
print(f"Batch IDs: {BATCH_1}, {BATCH_2}, {BATCH_3}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Cleanup Previous Test Data
print("\n" + "="*80)
print("CLEANING UP PREVIOUS TEST DATA")
print("="*80)

CLEAN_ALL = True

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
    try:
        spark.sql(f"DELETE FROM {INGESTION_CONFIG_TABLE} WHERE group_name = '{TEST_GROUP}'")
        print(f"   ✓ Cleaned {INGESTION_CONFIG_TABLE}")
    except Exception as e:
        print(f"   ⚠ Could not clean config: {e}")
    
    try:
        spark.sql(f"DELETE FROM {INGESTION_METADATA_TABLE} WHERE table_name LIKE '{TEST_CATALOG}.{TEST_SCHEMA}.%'")
        print(f"   ✓ Cleaned {INGESTION_METADATA_TABLE}")
    except Exception as e:
        print(f"   ⚠ Could not clean metadata: {e}")
    
    try:
        spark.sql(f"DELETE FROM {INGESTION_AUDIT_TABLE} WHERE group_name = '{TEST_GROUP}'")
        print(f"   ✓ Cleaned {INGESTION_AUDIT_TABLE}")
    except Exception as e:
        print(f"   ⚠ Could not clean audit: {e}")
    
    print("\n✓ Cleanup completed!")
else:
    print("\n⚠ Cleanup skipped!")

print("="*80)

# COMMAND ----------

# DBTITLE 1,Create Test Schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {TEST_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_CATALOG}.{TEST_SCHEMA}")
print(f"✓ Created schema: {TEST_CATALOG}.{TEST_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Setup Test Data - Sample Schema and Data
# Define sample data schema
orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_amount", DoubleType(), True),
    StructField("order_status", StringType(), True),
    StructField("partition_date", StringType(), True)
])

# Sample data for different batches
batch1_data = [
    (1001, 5001, 100.00, "COMPLETED", "2024-11-01"),
    (1002, 5002, 200.00, "PENDING", "2024-11-01"),
    (1003, 5003, 150.00, "COMPLETED", "2024-11-02"),
]

batch2_data = [
    (2001, 6001, 300.00, "COMPLETED", "2024-11-01"),  # Same partition as batch1
    (2002, 6002, 250.00, "PENDING", "2024-11-03"),    # New partition
]

batch3_data = [
    (3001, 7001, 400.00, "COMPLETED", "2024-11-04"),
    (3002, 7002, 350.00, "PENDING", "2024-11-04"),
]

print("✓ Sample data defined")

# COMMAND ----------

# DBTITLE 1,Scenario 1: OVERWRITE Mode Table
print("\n" + "="*80)
print("CREATING SCENARIO 1: OVERWRITE MODE")
print("="*80)

overwrite_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.orders_overwrite"

# Batch 1: Initial load
print(f"\nBatch 1: Creating table with {len(batch1_data)} rows, batch_id={BATCH_1}")
df1 = spark.createDataFrame(batch1_data, orders_schema)
df1_with_audit = df1.withColumn("_aud_batch_load_id", lit(BATCH_1))
df1_with_audit.write.mode("overwrite").format("delta").saveAsTable(overwrite_table)
print(f"  ✓ Table has {spark.table(overwrite_table).count()} rows with batch_id={BATCH_1}")

# Save batch1 source files
batch1_path = f"{DBFS_BASE_PATH}/overwrite/batch1"
df1.write.mode("overwrite").format("orc").save(batch1_path)

# Batch 2: Overwrite entire table (Batch 1 data is GONE!)
print(f"\nBatch 2: OVERWRITING table with {len(batch2_data)} rows, batch_id={BATCH_2}")
df2 = spark.createDataFrame(batch2_data, orders_schema)
df2_with_audit = df2.withColumn("_aud_batch_load_id", lit(BATCH_2))
df2_with_audit.write.mode("overwrite").format("delta").saveAsTable(overwrite_table)
print(f"  ✓ Table now has {spark.table(overwrite_table).count()} rows with batch_id={BATCH_2}")
print(f"  ⚠ Batch 1 data is COMPLETELY GONE (that's what overwrite does!)")

# Save batch2 source files
batch2_path = f"{DBFS_BASE_PATH}/overwrite/batch2"
df2.write.mode("overwrite").format("orc").save(batch2_path)

# Verify only batch2 exists
actual_batches = [row['_aud_batch_load_id'] for row in 
                 spark.table(overwrite_table).select("_aud_batch_load_id").distinct().collect()]
print(f"\nFinal state: Table contains batch_ids: {actual_batches}")
assert actual_batches == [BATCH_2], "Table should only have BATCH_2!"

print(f"\n✓ OVERWRITE scenario created: {overwrite_table}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Scenario 2: PARTITION_OVERWRITE Mode Table
print("\n" + "="*80)
print("CREATING SCENARIO 2: PARTITION_OVERWRITE MODE")
print("="*80)

partition_overwrite_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.orders_partition_overwrite"

# Batch 1: Initial load with partitions 2024-11-01 and 2024-11-02
print(f"\nBatch 1: Creating partitioned table with {len(batch1_data)} rows, batch_id={BATCH_1}")
df1_po = spark.createDataFrame(batch1_data, orders_schema)
df1_po_with_audit = df1_po.withColumn("_aud_batch_load_id", lit(BATCH_1))
df1_po_with_audit.write.mode("overwrite").format("delta") \
    .partitionBy("partition_date").saveAsTable(partition_overwrite_table)

partitions_b1 = [row['partition_date'] for row in 
                 spark.table(partition_overwrite_table).select("partition_date").distinct().collect()]
print(f"  ✓ Created partitions: {sorted(partitions_b1)}, all with batch_id={BATCH_1}")

# Save batch1 source files
batch1_po_path = f"{DBFS_BASE_PATH}/partition_overwrite/batch1"
df1_po.write.mode("overwrite").format("orc").save(batch1_po_path)

# Batch 2: Overwrite ONLY partition_date=2024-11-01, add new partition 2024-11-03
print(f"\nBatch 2: Overwriting partition 2024-11-01 + adding 2024-11-03, batch_id={BATCH_2}")
df2_po = spark.createDataFrame(batch2_data, orders_schema)
df2_po_with_audit = df2_po.withColumn("_aud_batch_load_id", lit(BATCH_2))

# Use dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df2_po_with_audit.write.mode("overwrite").format("delta") \
    .partitionBy("partition_date").saveAsTable(partition_overwrite_table)

# Reset to default after partition overwrite test
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

# Save batch2 source files
batch2_po_path = f"{DBFS_BASE_PATH}/partition_overwrite/batch2"
df2_po.write.mode("overwrite").format("orc").save(batch2_po_path)

# Verify partition states
result = spark.sql(f"""
    SELECT partition_date, _aud_batch_load_id, COUNT(*) as row_count
    FROM {partition_overwrite_table}
    GROUP BY partition_date, _aud_batch_load_id
    ORDER BY partition_date
""").collect()

print(f"\nFinal state:")
for row in result:
    print(f"  partition_date={row['partition_date']}: batch_id={row['_aud_batch_load_id']}, rows={row['row_count']}")

print(f"\n  Expected:")
print(f"    2024-11-01: BATCH_2 (overwritten)")
print(f"    2024-11-02: BATCH_1 (untouched)")
print(f"    2024-11-03: BATCH_2 (new)")

print(f"\n✓ PARTITION_OVERWRITE scenario created: {partition_overwrite_table}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Scenario 3: APPEND Mode Table
print("\n" + "="*80)
print("CREATING SCENARIO 3: APPEND MODE")
print("="*80)

append_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.orders_append"

# Batch 1: Initial load
print(f"\nBatch 1: Creating table with {len(batch1_data)} rows, batch_id={BATCH_1}")
df1_app = spark.createDataFrame(batch1_data, orders_schema)
df1_app_with_audit = df1_app.withColumn("_aud_batch_load_id", lit(BATCH_1))
df1_app_with_audit.write.mode("overwrite").format("delta") \
    .partitionBy("partition_date").saveAsTable(append_table)

# Save batch1 source files
batch1_app_path = f"{DBFS_BASE_PATH}/append/batch1"
df1_app.write.mode("overwrite").format("orc").save(batch1_app_path)

# Batch 2: Append more data
print(f"\nBatch 2: APPENDING {len(batch2_data)} rows, batch_id={BATCH_2}")
df2_app = spark.createDataFrame(batch2_data, orders_schema)
df2_app_with_audit = df2_app.withColumn("_aud_batch_load_id", lit(BATCH_2))
df2_app_with_audit.write.mode("append").format("delta") \
    .partitionBy("partition_date").saveAsTable(append_table)

# Save batch2 source files
batch2_app_path = f"{DBFS_BASE_PATH}/append/batch2"
df2_app.write.mode("overwrite").format("orc").save(batch2_app_path)

# Batch 3: Append even more data
print(f"\nBatch 3: APPENDING {len(batch3_data)} rows, batch_id={BATCH_3}")
df3_app = spark.createDataFrame(batch3_data, orders_schema)
df3_app_with_audit = df3_app.withColumn("_aud_batch_load_id", lit(BATCH_3))
df3_app_with_audit.write.mode("append").format("delta") \
    .partitionBy("partition_date").saveAsTable(append_table)

# Save batch3 source files
batch3_app_path = f"{DBFS_BASE_PATH}/append/batch3"
df3_app.write.mode("overwrite").format("orc").save(batch3_app_path)

# Verify all batches coexist
result = spark.sql(f"""
    SELECT _aud_batch_load_id, COUNT(*) as row_count
    FROM {append_table}
    GROUP BY _aud_batch_load_id
    ORDER BY _aud_batch_load_id
""").collect()

print(f"\nFinal state - All batches coexist:")
for row in result:
    print(f"  batch_id={row['_aud_batch_load_id']}: {row['row_count']} rows")

total_rows = spark.table(append_table).count()
expected_rows = len(batch1_data) + len(batch2_data) + len(batch3_data)
print(f"\nTotal rows: {total_rows} (expected: {expected_rows})")
assert total_rows == expected_rows, f"Row count mismatch!"

print(f"\n✓ APPEND scenario created: {append_table}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Insert Metadata for Test Tables
print("\n" + "="*80)
print("INSERTING METADATA FOR TEST TABLES")
print("="*80)

from pyspark.sql.types import TimestampType, LongType

# Helper function to get file metadata
def get_file_metadata(path):
    try:
        files = dbutils.fs.ls(path)
        data_files = [(f.path, datetime.fromtimestamp(f.modificationTime / 1000), f.size) 
                      for f in files if not f.name.startswith('_') and f.size > 0]
        return data_files
    except:
        return []

# 1. Insert into serving_ingestion_config
config_entries = []

# Config for OVERWRITE table
config_entries.append({
    "config_id": "TEST_WM_001",
    "group_name": TEST_GROUP,
    "source_schema": "test_source",
    "source_table": "orders_overwrite",
    "source_file_path": f"{DBFS_BASE_PATH}/overwrite",
    "target_catalog": TEST_CATALOG,
    "target_schema": TEST_SCHEMA,
    "target_table": "orders_overwrite",
    "source_file_format": "orc",
    "source_file_options": None,
    "load_type": "hive_full",
    "write_mode": "overwrite",
    "primary_key": "order_id",
    "partition_column": None,
    "partitioning_strategy": None,
    "frequency": "daily",
    "table_size_gb": "1",
    "is_active": "Y",
    "deduplicate": "N",
    "clean_column_names": "N",
    "insert_ts": datetime.now(),
    "last_update_ts": datetime.now()
})

# Config for PARTITION_OVERWRITE table
config_entries.append({
    "config_id": "TEST_WM_002",
    "group_name": TEST_GROUP,
    "source_schema": "test_source",
    "source_table": "orders_partition_overwrite",
    "source_file_path": f"{DBFS_BASE_PATH}/partition_overwrite",
    "target_catalog": TEST_CATALOG,
    "target_schema": TEST_SCHEMA,
    "target_table": "orders_partition_overwrite",
    "source_file_format": "orc",
    "source_file_options": None,
    "load_type": "incremental",
    "write_mode": "partition_overwrite",
    "primary_key": "order_id",
    "partition_column": "partition_date",
    "partitioning_strategy": "PARTITIONED_BY",
    "frequency": "daily",
    "table_size_gb": "1",
    "is_active": "Y",
    "deduplicate": "N",
    "clean_column_names": "N",
    "insert_ts": datetime.now(),
    "last_update_ts": datetime.now()
})

# Config for APPEND table
config_entries.append({
    "config_id": "TEST_WM_003",
    "group_name": TEST_GROUP,
    "source_schema": "test_source",
    "source_table": "orders_append",
    "source_file_path": f"{DBFS_BASE_PATH}/append",
    "target_catalog": TEST_CATALOG,
    "target_schema": TEST_SCHEMA,
    "target_table": "orders_append",
    "source_file_format": "orc",
    "source_file_options": None,
    "load_type": "incremental",
    "write_mode": "append",
    "primary_key": "order_id",
    "partition_column": "partition_date",
    "partitioning_strategy": "PARTITIONED_BY",
    "frequency": "daily",
    "table_size_gb": "1",
    "is_active": "Y",
    "deduplicate": "N",
    "clean_column_names": "N",
    "insert_ts": datetime.now(),
    "last_update_ts": datetime.now()
})

# Define schema and insert
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

# 2. Insert into serving_ingestion_metadata
metadata_entries = []

# Metadata for overwrite table (batch2 only - batch1 was overwritten)
for file_path, mod_time, size in get_file_metadata(batch2_path):
    metadata_entries.append({
        "table_name": overwrite_table,
        "batch_load_id": BATCH_2,
        "source_file_path": file_path,
        "file_modification_time": mod_time,
        "file_size": size,
        "is_processed": "Y",
        "insert_ts": datetime.now(),
        "last_update_ts": datetime.now()
    })

# Metadata for partition_overwrite table (both batches exist)
for file_path, mod_time, size in get_file_metadata(batch1_po_path):
    metadata_entries.append({
        "table_name": partition_overwrite_table,
        "batch_load_id": BATCH_1,
        "source_file_path": file_path,
        "file_modification_time": mod_time,
        "file_size": size,
        "is_processed": "Y",
        "insert_ts": datetime.now(),
        "last_update_ts": datetime.now()
    })

for file_path, mod_time, size in get_file_metadata(batch2_po_path):
    metadata_entries.append({
        "table_name": partition_overwrite_table,
        "batch_load_id": BATCH_2,
        "source_file_path": file_path,
        "file_modification_time": mod_time,
        "file_size": size,
        "is_processed": "Y",
        "insert_ts": datetime.now(),
        "last_update_ts": datetime.now()
    })

# Metadata for append table (all 3 batches exist)
for file_path, mod_time, size in get_file_metadata(batch1_app_path):
    metadata_entries.append({
        "table_name": append_table,
        "batch_load_id": BATCH_1,
        "source_file_path": file_path,
        "file_modification_time": mod_time,
        "file_size": size,
        "is_processed": "Y",
        "insert_ts": datetime.now(),
        "last_update_ts": datetime.now()
    })

for file_path, mod_time, size in get_file_metadata(batch2_app_path):
    metadata_entries.append({
        "table_name": append_table,
        "batch_load_id": BATCH_2,
        "source_file_path": file_path,
        "file_modification_time": mod_time,
        "file_size": size,
        "is_processed": "Y",
        "insert_ts": datetime.now(),
        "last_update_ts": datetime.now()
    })

for file_path, mod_time, size in get_file_metadata(batch3_app_path):
    metadata_entries.append({
        "table_name": append_table,
        "batch_load_id": BATCH_3,
        "source_file_path": file_path,
        "file_modification_time": mod_time,
        "file_size": size,
        "is_processed": "Y",
        "insert_ts": datetime.now(),
        "last_update_ts": datetime.now()
    })

metadata_df = spark.createDataFrame(metadata_entries)
metadata_df.write.mode("append").saveAsTable(INGESTION_METADATA_TABLE)
print(f"✓ Inserted {len(metadata_entries)} entries into {INGESTION_METADATA_TABLE}")

# 3. Insert into serving_ingestion_audit
audit_schema = StructType([
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

audit_entries = []

# Audit for overwrite (only batch2)
audit_entries.append({
    "run_id": f"RUN_{BATCH_2}",
    "config_id": "TEST_WM_001",
    "batch_load_id": BATCH_2,
    "group_name": TEST_GROUP,
    "target_table_name": overwrite_table,
    "operation_type": "LOAD",
    "load_type": "BATCH",
    "status": "COMPLETED",
    "start_ts": datetime.now() - timedelta(minutes=5),
    "end_ts": datetime.now(),
    "log_message": "Test data loaded",
    "microbatch_id": None,
    "row_count": len(batch2_data)
})

# Audit for partition_overwrite (both batches)
audit_entries.append({
    "run_id": f"RUN_{BATCH_1}",
    "config_id": "TEST_WM_002",
    "batch_load_id": BATCH_1,
    "group_name": TEST_GROUP,
    "target_table_name": partition_overwrite_table,
    "operation_type": "LOAD",
    "load_type": "BATCH",
    "status": "COMPLETED",
    "start_ts": datetime.now() - timedelta(minutes=10),
    "end_ts": datetime.now() - timedelta(minutes=5),
    "log_message": "Test data loaded",
    "microbatch_id": None,
    "row_count": len(batch1_data)
})

audit_entries.append({
    "run_id": f"RUN_{BATCH_2}",
    "config_id": "TEST_WM_002",
    "batch_load_id": BATCH_2,
    "group_name": TEST_GROUP,
    "target_table_name": partition_overwrite_table,
    "operation_type": "LOAD",
    "load_type": "BATCH",
    "status": "COMPLETED",
    "start_ts": datetime.now() - timedelta(minutes=5),
    "end_ts": datetime.now(),
    "log_message": "Test data loaded",
    "microbatch_id": None,
    "row_count": len(batch2_data)
})

# Audit for append (all 3 batches)
audit_entries.append({
    "run_id": f"RUN_{BATCH_1}",
    "config_id": "TEST_WM_003",
    "batch_load_id": BATCH_1,
    "group_name": TEST_GROUP,
    "target_table_name": append_table,
    "operation_type": "LOAD",
    "load_type": "BATCH",
    "status": "COMPLETED",
    "start_ts": datetime.now() - timedelta(minutes=15),
    "end_ts": datetime.now() - timedelta(minutes=10),
    "log_message": "Test data loaded",
    "microbatch_id": None,
    "row_count": len(batch1_data)
})

audit_entries.append({
    "run_id": f"RUN_{BATCH_2}",
    "config_id": "TEST_WM_003",
    "batch_load_id": BATCH_2,
    "group_name": TEST_GROUP,
    "target_table_name": append_table,
    "operation_type": "LOAD",
    "load_type": "BATCH",
    "status": "COMPLETED",
    "start_ts": datetime.now() - timedelta(minutes=10),
    "end_ts": datetime.now() - timedelta(minutes=5),
    "log_message": "Test data loaded",
    "microbatch_id": None,
    "row_count": len(batch2_data)
})

audit_entries.append({
    "run_id": f"RUN_{BATCH_3}",
    "config_id": "TEST_WM_003",
    "batch_load_id": BATCH_3,
    "group_name": TEST_GROUP,
    "target_table_name": append_table,
    "operation_type": "LOAD",
    "load_type": "BATCH",
    "status": "COMPLETED",
    "start_ts": datetime.now() - timedelta(minutes=5),
    "end_ts": datetime.now(),
    "log_message": "Test data loaded",
    "microbatch_id": None,
    "row_count": len(batch3_data)
})

audit_df = spark.createDataFrame(audit_entries, schema=audit_schema)
audit_df.write.mode("append").saveAsTable(INGESTION_AUDIT_TABLE)
print(f"✓ Inserted {len(audit_entries)} entries into {INGESTION_AUDIT_TABLE}")

print("\n✓ All metadata inserted successfully")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Test 1: Configuration Validation
print("\n" + "="*80)
print("TEST 1: Configuration Validation")
print("="*80)

print("\nTesting: partition_overwrite without partition_columns should FAIL")

try:
    invalid_config = TableConfig(
        table_group="test",
        table_family="test",
        table_name="test_table",
        source_table="test_source",
        write_mode="partition_overwrite",
        source_file_format="orc",
        partition_columns=[],  # EMPTY - should fail!
        primary_keys=["id"]
    )
    print("  ❌ TEST FAILED: Invalid config was accepted!")
    test1_passed = False
except ConfigurationError as e:
    print(f"  ✓ Correctly rejected: {str(e)}")
    test1_passed = True
except Exception as e:
    print(f"  ❌ Unexpected error: {e}")
    test1_passed = False

print("\n" + "="*80)
if test1_passed:
    print("✅ TEST 1 PASSED")
else:
    print("❌ TEST 1 FAILED")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Test 2: OVERWRITE Mode - Load ALL Data
print("\n" + "="*80)
print("TEST 2: OVERWRITE Mode - Load ALL Target Data")
print("="*80)

print(f"\nTable: {overwrite_table}")
print(f"Scenario: Batch1 was overwritten by Batch2")
print(f"Expected: Load entire table (all BATCH_2 data)")

test2_passed = False

try:
    # Create config
    config = TableConfig(
        table_group=TEST_GROUP,
        table_family=TEST_GROUP,
        table_name=overwrite_table,
        source_table="test_source.orders_overwrite",
        write_mode="overwrite",
        source_file_format="orc",
        partition_columns=[],
        primary_keys=["order_id"]
    )
    
    # Load target data
    loader = SourceTargetLoader(spark)
    
    print(f"\nCalling _load_target_delta() with batch_id={BATCH_2}")
    target_df = loader._load_target_delta(config, BATCH_2)
    
    actual_count = target_df.count()
    expected_count = len(batch2_data)
    
    print(f"\nResults:")
    print(f"  Rows loaded: {actual_count}")
    print(f"  Expected: {expected_count}")
    
    # Verify correct count
    if actual_count == expected_count:
        print(f"  ✓ Correct row count")
        
        # Verify only BATCH_2 exists
        actual_batches = [row['_aud_batch_load_id'] for row in 
                        target_df.select("_aud_batch_load_id").distinct().collect()]
        
        if actual_batches == [BATCH_2]:
            print(f"  ✓ Only BATCH_2 present (entire table loaded)")
            test2_passed = True
        else:
            print(f"  ❌ Unexpected batch_ids: {actual_batches}")
    else:
        print(f"  ❌ Row count mismatch")
        
except Exception as e:
    print(f"  ❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
if test2_passed:
    print("✅ TEST 2 PASSED: OVERWRITE mode loads all data correctly")
else:
    print("❌ TEST 2 FAILED")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Test 3: PARTITION_OVERWRITE Mode - Filter by Batch ID
print("\n" + "="*80)
print("TEST 3: PARTITION_OVERWRITE Mode - Filter by Batch ID")
print("="*80)

print(f"\nTable: {partition_overwrite_table}")
print(f"Scenario: Batch1 and Batch2 coexist in different partitions")
print(f"Expected: Filter by batch_id returns only that batch's data")

test3_passed = False

try:
    # Create config
    config = TableConfig(
        table_group=TEST_GROUP,
        table_family=TEST_GROUP,
        table_name=partition_overwrite_table,
        source_table="test_source.orders_partition_overwrite",
        write_mode="partition_overwrite",
        source_file_format="orc",
        partition_columns=["partition_date"],
        primary_keys=["order_id"],
        partition_datatypes={"partition_date": "string"}
    )
    
    loader = SourceTargetLoader(spark)
    
    # Test loading BATCH_1 (should only get partition_date=2024-11-02)
    print(f"\nTest 3a: Loading BATCH_1")
    target_df_b1 = loader._load_target_delta(config, BATCH_1)
    count_b1 = target_df_b1.count()
    
    # BATCH_1 should only have partition_date=2024-11-02 (1 row)
    # because partition_date=2024-11-01 was overwritten by BATCH_2
    expected_b1 = 1
    
    print(f"  Rows loaded: {count_b1}")
    print(f"  Expected: {expected_b1} (only untouched partition)")
    
    partitions_b1 = [row['partition_date'] for row in 
                    target_df_b1.select("partition_date").distinct().collect()]
    print(f"  Partitions: {sorted(partitions_b1)}")
    
    # Test loading BATCH_2 (should get both partitions it wrote)
    print(f"\nTest 3b: Loading BATCH_2")
    target_df_b2 = loader._load_target_delta(config, BATCH_2)
    count_b2 = target_df_b2.count()
    expected_b2 = len(batch2_data)
    
    print(f"  Rows loaded: {count_b2}")
    print(f"  Expected: {expected_b2}")
    
    partitions_b2 = [row['partition_date'] for row in 
                    target_df_b2.select("partition_date").distinct().collect()]
    print(f"  Partitions: {sorted(partitions_b2)}")
    
    # Verify
    if count_b1 == expected_b1 and count_b2 == expected_b2:
        print(f"\n  ✓ Both batches filtered correctly")
        test3_passed = True
    else:
        print(f"\n  ❌ Row count mismatch")
        
except Exception as e:
    print(f"  ❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
if test3_passed:
    print("✅ TEST 3 PASSED: PARTITION_OVERWRITE mode filters correctly")
else:
    print("❌ TEST 3 FAILED")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Test 4: APPEND Mode - Filter by Batch ID
print("\n" + "="*80)
print("TEST 4: APPEND Mode - Filter by Batch ID")
print("="*80)

print(f"\nTable: {append_table}")
print(f"Scenario: All 3 batches coexist")
print(f"Expected: Each batch_id returns only its data")

test4_passed = False

try:
    # Create config
    config = TableConfig(
        table_group=TEST_GROUP,
        table_family=TEST_GROUP,
        table_name=append_table,
        source_table="test_source.orders_append",
        write_mode="append",
        source_file_format="orc",
        partition_columns=["partition_date"],
        primary_keys=["order_id"],
        partition_datatypes={"partition_date": "string"}
    )
    
    loader = SourceTargetLoader(spark)
    
    # Test each batch
    tests_passed = []
    
    for batch_id, expected_data in [(BATCH_1, batch1_data), (BATCH_2, batch2_data), (BATCH_3, batch3_data)]:
        print(f"\nLoading {batch_id}:")
        target_df = loader._load_target_delta(config, batch_id)
        count = target_df.count()
        expected = len(expected_data)
        
        print(f"  Rows: {count} (expected: {expected})")
        
        # Verify only this batch returned
        actual_batches = [row['_aud_batch_load_id'] for row in 
                        target_df.select("_aud_batch_load_id").distinct().collect()]
        
        if count == expected and actual_batches == [batch_id]:
            print(f"  ✓ Correct")
            tests_passed.append(True)
        else:
            print(f"  ❌ Failed - batches: {actual_batches}")
            tests_passed.append(False)
    
    test4_passed = all(tests_passed)
        
except Exception as e:
    print(f"  ❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
if test4_passed:
    print("✅ TEST 4 PASSED: APPEND mode filters correctly for all batches")
else:
    print("❌ TEST 4 FAILED")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Test 5: Performance - SQL WHERE vs DataFrame Filter
print("\n" + "="*80)
print("TEST 5: Performance Comparison")
print("="*80)

print(f"\nComparing OLD (DataFrame filter) vs NEW (SQL WHERE)")
print(f"Table: {append_table}")

test5_passed = False

try:
    # OLD approach
    print(f"\nOLD approach (DataFrame filter):")
    start_old = time.time()
    df_old = spark.table(append_table)
    df_old_filtered = df_old.filter(f"_aud_batch_load_id = '{BATCH_2}'")
    count_old = df_old_filtered.count()
    time_old = time.time() - start_old
    print(f"  Rows: {count_old}, Time: {time_old:.3f}s")
    
    # NEW approach
    print(f"\nNEW approach (SQL WHERE):")
    start_new = time.time()
    df_new = spark.sql(f"SELECT * FROM {append_table} WHERE _aud_batch_load_id = '{BATCH_2}'")
    count_new = df_new.count()
    time_new = time.time() - start_new
    print(f"  Rows: {count_new}, Time: {time_new:.3f}s")
    
    # Compare
    if time_old > 0:
        speedup = time_old / time_new
        print(f"\nSpeedup: {speedup:.2f}x")
        
        if speedup > 1.0:
            print(f"✓ NEW approach is {speedup:.2f}x FASTER")
        else:
            print(f"⚠ NEW approach is {1/speedup:.2f}x SLOWER (small dataset)")
    
    # Verify same results
    if count_old == count_new:
        print(f"✓ Both return same row count: {count_old}")
        test5_passed = True
    else:
        print(f"❌ Different counts: OLD={count_old}, NEW={count_new}")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
if test5_passed:
    print("✅ TEST 5 PASSED: Performance test completed")
else:
    print("❌ TEST 5 FAILED")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Final Summary
print("\n" + "="*80)
print("FINAL TEST SUMMARY")
print("="*80)

test_results = {
    "TEST 1: Config Validation": test1_passed,
    "TEST 2: OVERWRITE Mode": test2_passed,
    "TEST 3: PARTITION_OVERWRITE Mode": test3_passed,
    "TEST 4: APPEND Mode": test4_passed,
    "TEST 5: Performance": test5_passed,
}

passed = sum(test_results.values())
total = len(test_results)

print(f"\nResults:")
for name, result in test_results.items():
    status = "✅ PASSED" if result else "❌ FAILED"
    print(f"  {name}: {status}")

print(f"\n{passed}/{total} tests passed")

if passed == total:
    print("\n🎉 ALL TESTS PASSED! 🎉")
    print("\nThe write mode fix is working correctly!")
    print("\n✅ READY FOR PRODUCTION DEPLOYMENT")
else:
    print(f"\n⚠️ {total - passed} test(s) failed")
    print("\nPlease review failed tests above")

print("="*80)

# COMMAND ----------

# DBTITLE 1,Cleanup (Optional)
print("\n" + "="*80)
print("CLEANUP (Optional - Set CLEANUP = True to execute)")
print("="*80)

CLEANUP = False  # Set to True to cleanup test data

if CLEANUP:
    print("\nCleaning up test data...")
    
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {TEST_CATALOG}.{TEST_SCHEMA} CASCADE")
        print(f"  ✓ Dropped schema")
    except Exception as e:
        print(f"  ⚠ {e}")
    
    try:
        dbutils.fs.rm(DBFS_BASE_PATH, recurse=True)
        print(f"  ✓ Removed DBFS files")
    except Exception as e:
        print(f"  ⚠ {e}")
    
    try:
        spark.sql(f"DELETE FROM {INGESTION_CONFIG_TABLE} WHERE group_name = '{TEST_GROUP}'")
        spark.sql(f"DELETE FROM {INGESTION_METADATA_TABLE} WHERE table_name LIKE '{TEST_CATALOG}.{TEST_SCHEMA}.%'")
        spark.sql(f"DELETE FROM {INGESTION_AUDIT_TABLE} WHERE group_name = '{TEST_GROUP}'")
        print(f"  ✓ Cleaned metadata tables")
    except Exception as e:
        print(f"  ⚠ {e}")
    
    print("\n✓ Cleanup completed!")
else:
    print("\n⚠ Cleanup skipped (CLEANUP = False)")
    print("  Set CLEANUP = True and re-run this cell to cleanup test data")

print("="*80)
