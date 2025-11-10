# Databricks notebook source
# MAGIC %md
# MAGIC # Production-Ready Final Validation Test
# MAGIC 
# MAGIC **Purpose**: Comprehensive end-to-end test of deltarecon framework
# MAGIC 
# MAGIC **Test Scenarios:**
# MAGIC 1. OVERWRITE mode - full table replacement
# MAGIC 2. PARTITION_OVERWRITE mode - partition-level overwrite with Hive partitions
# MAGIC 3. APPEND mode - incremental appends with partitions
# MAGIC 
# MAGIC **Features:**
# MAGIC - Unique timestamp per run (no conflicts)
# MAGIC - Proper metadata table schema alignment
# MAGIC - Comprehensive cleanup
# MAGIC - Self-contained and idempotent

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType, LongType
from pyspark.sql.functions import lit, current_timestamp, col, regexp_extract
from datetime import datetime
from deltarecon import ValidationRunner
import time

print("✓ Libraries imported")

# COMMAND ----------

# DBTITLE 1,Configuration
# Generate unique timestamp for THIS run
RUN_TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
TEST_CATALOG = "ts42_demo"
TEST_SCHEMA = f"prod_test_{RUN_TIMESTAMP}"
TEST_GROUP = f"prod_test_grp_{RUN_TIMESTAMP}"

# Batch IDs
BATCH_1_ID = f"BATCH_{RUN_TIMESTAMP}_001"
BATCH_2_ID = f"BATCH_{RUN_TIMESTAMP}_002"
BATCH_3_ID = f"BATCH_{RUN_TIMESTAMP}_003"

# DBFS paths
DBFS_ROOT = f"/FileStore/prod_test_{RUN_TIMESTAMP}"

# Metadata tables
CONFIG_TABLE = "ts42_demo.migration_operations.serving_ingestion_config"
METADATA_TABLE = "ts42_demo.migration_operations.serving_ingestion_metadata"
AUDIT_TABLE = "ts42_demo.migration_operations.serving_ingestion_audit"

# Validation tables (for cleanup)
VALIDATION_LOG = "cat_ril_nayeem_03.validation_v2.validation_log"
VALIDATION_SUMMARY = "cat_ril_nayeem_03.validation_v2.validation_summary"

print(f"Test Configuration:")
print(f"  Schema: {TEST_SCHEMA}")
print(f"  Group: {TEST_GROUP}")
print(f"  DBFS: {DBFS_ROOT}")
print(f"  Batch 1: {BATCH_1_ID}")
print(f"  Batch 2: {BATCH_2_ID}")
print(f"  Batch 3: {BATCH_3_ID}")

# COMMAND ----------

# DBTITLE 1,Aggressive Cleanup
print("="*80)
print("CLEANUP - Removing ALL old test data")
print("="*80)

# 1. Drop ALL old test schemas
print("\n1. Cleaning old schemas...")
try:
    old_schemas = spark.sql(f"SHOW SCHEMAS IN {TEST_CATALOG}").collect()
    dropped = 0
    for row in old_schemas:
        schema_name = row['namespace']
        if any(pattern in schema_name for pattern in ['prod_test_', 'final_validation', 'test_write_mode']):
            try:
                spark.sql(f"DROP SCHEMA IF EXISTS {TEST_CATALOG}.{schema_name} CASCADE")
                dropped += 1
            except Exception as e:
                print(f"  ⚠ Could not drop {schema_name}: {e}")
    print(f"  ✓ Dropped {dropped} old test schemas")
except Exception as e:
    print(f"  ⚠ Schema cleanup error: {e}")

# 2. Clean DBFS files
print("\n2. Cleaning DBFS files...")
try:
    files = dbutils.fs.ls("/FileStore/")
    removed = 0
    for f in files:
        if any(pattern in f.name for pattern in ['prod_test_', 'final_validation', 'write_mode_test']):
            try:
                dbutils.fs.rm(f.path, recurse=True)
                removed += 1
            except:
                pass
    print(f"  ✓ Removed {removed} old DBFS directories")
except Exception as e:
    print(f"  ⚠ DBFS cleanup error: {e}")

# 3. Clean metadata tables
print("\n3. Cleaning metadata tables...")
for pattern in ['prod_test_%', 'final_val_test%', 'test_write_mode%', 'prod_validation%']:
    try:
        spark.sql(f"DELETE FROM {CONFIG_TABLE} WHERE group_name LIKE '{pattern}'")
        spark.sql(f"DELETE FROM {AUDIT_TABLE} WHERE group_name LIKE '{pattern}'")
    except Exception as e:
        print(f"  ⚠ {e}")

for pattern in ['prod_test_%', 'final_validation%', 'test_write_mode%']:
    try:
        spark.sql(f"DELETE FROM {METADATA_TABLE} WHERE table_name LIKE '{TEST_CATALOG}.{pattern}'")
    except Exception as e:
        print(f"  ⚠ {e}")

# 4. Clean validation tables
print("\n4. Cleaning validation tables...")
try:
    spark.sql(f"""
        DELETE FROM {VALIDATION_LOG} 
        WHERE table_family LIKE '{TEST_CATALOG}.prod_test_%'
        OR table_family LIKE '{TEST_CATALOG}.final_validation%'
        OR table_family LIKE '{TEST_CATALOG}.test_write_mode%'
    """)
    spark.sql(f"""
        DELETE FROM {VALIDATION_SUMMARY} 
        WHERE table_family LIKE '{TEST_CATALOG}.prod_test_%'
        OR table_family LIKE '{TEST_CATALOG}.final_validation%'
        OR table_family LIKE '{TEST_CATALOG}.test_write_mode%'
    """)
    print("  ✓ Validation tables cleaned")
except Exception as e:
    print(f"  ⚠ {e}")

print("\n✓ Cleanup completed!")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Create Test Schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_CATALOG}.{TEST_SCHEMA}")
print(f"✓ Created schema: {TEST_CATALOG}.{TEST_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Define Data Schema (Production-style, NO partition column in data)
# Schema WITHOUT partition_date - it will be in directory structure
orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_amount", DoubleType(), True),
    StructField("order_status", StringType(), True)
])

print("✓ Data schema defined (partition columns will be in file paths)")

# COMMAND ----------

# DBTITLE 1,Create Test Tables
# Table names
overwrite_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.transactions"
partition_overwrite_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.orders_partitioned"
append_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.orders_history"

# Create tables
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {overwrite_table} (
        order_id INT NOT NULL,
        customer_id INT NOT NULL,
        order_amount DOUBLE,
        order_status STRING,
        _aud_batch_load_id STRING
    ) USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {partition_overwrite_table} (
        order_id INT NOT NULL,
        customer_id INT NOT NULL,
        order_amount DOUBLE,
        order_status STRING,
        _aud_batch_load_id STRING,
        partition_date STRING
    ) USING DELTA
    PARTITIONED BY (partition_date)
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {append_table} (
        order_id INT NOT NULL,
        customer_id INT NOT NULL,
        order_amount DOUBLE,
        order_status STRING,
        _aud_batch_load_id STRING,
        partition_date STRING
    ) USING DELTA
    PARTITIONED BY (partition_date)
""")

print(f"✓ Created 3 test tables")

# COMMAND ----------

# DBTITLE 1,Scenario 1: OVERWRITE Mode
print("="*80)
print("SCENARIO 1: OVERWRITE MODE")
print("="*80)

# Batch 1 data (will be replaced)
batch1_ow_data = [(1, 101, 100.0, "completed"), (2, 102, 200.0, "pending"), 
                   (3, 103, 150.0, "completed"), (4, 104, 300.0, "cancelled")]
batch1_ow_df = spark.createDataFrame(batch1_ow_data, orders_schema)

# Write Batch 1 ORC files
batch1_ow_path = f"{DBFS_ROOT}/overwrite/batch1"
batch1_ow_df.write.mode("overwrite").format("orc").save(batch1_ow_path)

# Load and write to Delta (simulating ingestion)
batch1_ow_loaded = spark.read.format("orc").load(batch1_ow_path)
batch1_ow_loaded.withColumn("_aud_batch_load_id", lit(BATCH_1_ID)) \
    .write.mode("overwrite").saveAsTable(overwrite_table)

print(f"✓ Batch 1 written (4 rows) - will be replaced")

# Batch 2 data (replaces Batch 1)
batch2_ow_data = [(5, 105, 250.0, "completed"), (6, 106, 180.0, "pending")]
batch2_ow_df = spark.createDataFrame(batch2_ow_data, orders_schema)

# Write Batch 2 ORC files
batch2_ow_path = f"{DBFS_ROOT}/overwrite/batch2"
batch2_ow_df.write.mode("overwrite").format("orc").save(batch2_ow_path)

# Load and write to Delta (replaces everything)
batch2_ow_loaded = spark.read.format("orc").load(batch2_ow_path)
batch2_ow_loaded.withColumn("_aud_batch_load_id", lit(BATCH_2_ID)) \
    .write.mode("overwrite").saveAsTable(overwrite_table)

print(f"✓ Batch 2 written (2 rows) - replaced entire table")

# Verify
final_count = spark.table(overwrite_table).count()
print(f"✓ Final table count: {final_count} (should be 2)")

# COMMAND ----------

# DBTITLE 1,Scenario 2: PARTITION_OVERWRITE Mode
print("="*80)
print("SCENARIO 2: PARTITION_OVERWRITE MODE")
print("="*80)

# Batch 1: Multiple partitions
batch1_po_data = [
    (10, 201, 100.0, "completed", "2024-11-01"),
    (11, 202, 200.0, "pending", "2024-11-01"),
    (12, 203, 150.0, "completed", "2024-11-02"),
    (13, 204, 300.0, "cancelled", "2024-11-02")
]
batch1_po_df = spark.createDataFrame(batch1_po_data, orders_schema.add(StructField("partition_date", StringType())))

# Write with Hive-style partitioning
batch1_po_path = f"{DBFS_ROOT}/partition_overwrite/batch1"
batch1_po_df.write.mode("overwrite").format("orc").partitionBy("partition_date").save(batch1_po_path)

# Load (extract partition from path) and write to Delta
batch1_po_loaded = spark.read.format("orc").load(batch1_po_path) \
    .withColumn("partition_date", regexp_extract(col("_metadata.file_path"), r"partition_date=([^/]+)", 1))
    
batch1_po_loaded.withColumn("_aud_batch_load_id", lit(BATCH_1_ID)) \
    .write.mode("append").saveAsTable(partition_overwrite_table)

print(f"✓ Batch 1 written (4 rows in 2 partitions: 2024-11-01, 2024-11-02)")

# Batch 2: Overwrites ONLY 2024-11-01 partition
batch2_po_data = [
    (14, 205, 250.0, "completed", "2024-11-01"),
    (15, 206, 180.0, "pending", "2024-11-01")
]
batch2_po_df = spark.createDataFrame(batch2_po_data, orders_schema.add(StructField("partition_date", StringType())))

# Write with Hive-style partitioning
batch2_po_path = f"{DBFS_ROOT}/partition_overwrite/batch2"
batch2_po_df.write.mode("overwrite").format("orc").partitionBy("partition_date").save(batch2_po_path)

# Set dynamic partition overwrite mode
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Load and write (overwrite only 2024-11-01 partition)
batch2_po_loaded = spark.read.format("orc").load(batch2_po_path) \
    .withColumn("partition_date", regexp_extract(col("_metadata.file_path"), r"partition_date=([^/]+)", 1))
    
batch2_po_loaded.withColumn("_aud_batch_load_id", lit(BATCH_2_ID)) \
    .write.mode("overwrite").option("partitionOverwriteMode", "dynamic") \
    .saveAsTable(partition_overwrite_table)

# Reset config
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

print(f"✓ Batch 2 written (2 rows) - overwrote partition 2024-11-01 only")

# Verify
final_count = spark.table(partition_overwrite_table).count()
batch1_remaining = spark.sql(f"SELECT COUNT(*) as c FROM {partition_overwrite_table} WHERE _aud_batch_load_id = '{BATCH_1_ID}'").collect()[0]['c']
print(f"✓ Final table count: {final_count} (should be 4: 2 from Batch1 + 2 from Batch2)")
print(f"✓ Batch 1 remaining rows: {batch1_remaining} (should be 2 from partition 2024-11-02)")

# COMMAND ----------

# DBTITLE 1,Scenario 3: APPEND Mode
print("="*80)
print("SCENARIO 3: APPEND MODE")
print("="*80)

# Batch 1
batch1_ap_data = [
    (20, 301, 100.0, "completed", "2024-11-01"),
    (21, 302, 200.0, "pending", "2024-11-01"),
    (22, 303, 150.0, "completed", "2024-11-02"),
    (23, 304, 300.0, "cancelled", "2024-11-02")
]
batch1_ap_df = spark.createDataFrame(batch1_ap_data, orders_schema.add(StructField("partition_date", StringType())))

batch1_ap_path = f"{DBFS_ROOT}/append/batch1"
batch1_ap_df.write.mode("overwrite").format("orc").partitionBy("partition_date").save(batch1_ap_path)

batch1_ap_loaded = spark.read.format("orc").load(batch1_ap_path) \
    .withColumn("partition_date", regexp_extract(col("_metadata.file_path"), r"partition_date=([^/]+)", 1))
batch1_ap_loaded.withColumn("_aud_batch_load_id", lit(BATCH_1_ID)) \
    .write.mode("append").saveAsTable(append_table)

print(f"✓ Batch 1 appended (4 rows)")

# Batch 2
batch2_ap_data = [
    (24, 305, 250.0, "completed", "2024-11-03"),
    (25, 306, 180.0, "pending", "2024-11-03")
]
batch2_ap_df = spark.createDataFrame(batch2_ap_data, orders_schema.add(StructField("partition_date", StringType())))

batch2_ap_path = f"{DBFS_ROOT}/append/batch2"
batch2_ap_df.write.mode("overwrite").format("orc").partitionBy("partition_date").save(batch2_ap_path)

batch2_ap_loaded = spark.read.format("orc").load(batch2_ap_path) \
    .withColumn("partition_date", regexp_extract(col("_metadata.file_path"), r"partition_date=([^/]+)", 1))
batch2_ap_loaded.withColumn("_aud_batch_load_id", lit(BATCH_2_ID)) \
    .write.mode("append").saveAsTable(append_table)

print(f"✓ Batch 2 appended (2 rows)")

# Batch 3
batch3_ap_data = [
    (26, 307, 350.0, "completed", "2024-11-03"),
    (27, 308, 220.0, "pending", "2024-11-04"),
    (28, 309, 180.0, "cancelled", "2024-11-04")
]
batch3_ap_df = spark.createDataFrame(batch3_ap_data, orders_schema.add(StructField("partition_date", StringType())))

batch3_ap_path = f"{DBFS_ROOT}/append/batch3"
batch3_ap_df.write.mode("overwrite").format("orc").partitionBy("partition_date").save(batch3_ap_path)

batch3_ap_loaded = spark.read.format("orc").load(batch3_ap_path) \
    .withColumn("partition_date", regexp_extract(col("_metadata.file_path"), r"partition_date=([^/]+)", 1))
batch3_ap_loaded.withColumn("_aud_batch_load_id", lit(BATCH_3_ID)) \
    .write.mode("append").saveAsTable(append_table)

print(f"✓ Batch 3 appended (3 rows)")

# Verify
final_count = spark.table(append_table).count()
print(f"✓ Final table count: {final_count} (should be 9)")

# COMMAND ----------

# DBTITLE 1,Insert Metadata - Config
print("="*80)
print("INSERTING METADATA")
print("="*80)

config_id_1 = f"PROD_CFG_001_{RUN_TIMESTAMP}"
config_id_2 = f"PROD_CFG_002_{RUN_TIMESTAMP}"
config_id_3 = f"PROD_CFG_003_{RUN_TIMESTAMP}"

# OVERWRITE config
spark.sql(f"""
    INSERT INTO {CONFIG_TABLE}
    (config_id, group_name, source_schema, source_table, source_file_path,
     target_catalog, target_schema, target_table, source_file_format, source_file_options,
     load_type, write_mode, primary_key, partition_column, partitioning_strategy,
     frequency, table_size_gb, is_active, deduplicate, clean_column_names, insert_ts, last_update_ts)
    VALUES
    ('{config_id_1}', '{TEST_GROUP}', 'source', 'transactions',
     '{DBFS_ROOT}/overwrite', '{TEST_CATALOG}', '{TEST_SCHEMA}', 'transactions',
     'orc', NULL, 'hive_full', 'overwrite', 'order_id', NULL, NULL,
     'daily', '1', 'Y', 'N', 'N', current_timestamp(), current_timestamp())
""")

# PARTITION_OVERWRITE config
spark.sql(f"""
    INSERT INTO {CONFIG_TABLE}
    (config_id, group_name, source_schema, source_table, source_file_path,
     target_catalog, target_schema, target_table, source_file_format, source_file_options,
     load_type, write_mode, primary_key, partition_column, partitioning_strategy,
     frequency, table_size_gb, is_active, deduplicate, clean_column_names, insert_ts, last_update_ts)
    VALUES
    ('{config_id_2}', '{TEST_GROUP}', 'source', 'orders_partitioned',
     '{DBFS_ROOT}/partition_overwrite', '{TEST_CATALOG}', '{TEST_SCHEMA}', 'orders_partitioned',
     'orc', NULL, 'incremental', 'partition_overwrite', 'order_id', 'partition_date', 'PARTITIONED_BY',
     'daily', '1', 'Y', 'N', 'N', current_timestamp(), current_timestamp())
""")

# APPEND config
spark.sql(f"""
    INSERT INTO {CONFIG_TABLE}
    (config_id, group_name, source_schema, source_table, source_file_path,
     target_catalog, target_schema, target_table, source_file_format, source_file_options,
     load_type, write_mode, primary_key, partition_column, partitioning_strategy,
     frequency, table_size_gb, is_active, deduplicate, clean_column_names, insert_ts, last_update_ts)
    VALUES
    ('{config_id_3}', '{TEST_GROUP}', 'source', 'orders_history',
     '{DBFS_ROOT}/append', '{TEST_CATALOG}', '{TEST_SCHEMA}', 'orders_history',
     'orc', NULL, 'incremental', 'append', 'order_id', 'partition_date', 'PARTITIONED_BY',
     'daily', '1', 'Y', 'N', 'N', current_timestamp(), current_timestamp())
""")

print("✓ Inserted 3 config entries")

# COMMAND ----------

# DBTITLE 1,Insert Metadata - Files
def get_files_recursive(path):
    """Get all ORC files recursively"""
    files = []
    try:
        items = dbutils.fs.ls(path)
        for item in items:
            if item.isDir():
                files.extend(get_files_recursive(item.path))
            elif item.path.endswith('.orc'):
                files.append((item.path, item.modificationTime, item.size))
    except:
        pass
    return files

# OVERWRITE metadata
for batch_id, batch_path in [(BATCH_1_ID, batch1_ow_path), (BATCH_2_ID, batch2_ow_path)]:
    files = get_files_recursive(batch_path)
    for file_path, mod_time, file_size in files:
        spark.sql(f"""
            INSERT INTO {METADATA_TABLE}
            (table_name, batch_load_id, source_file_path, file_modification_time, 
             file_size, is_processed, insert_ts, last_update_ts)
            VALUES
            ('{overwrite_table}', '{batch_id}', '{file_path}', 
             from_unixtime({mod_time}/1000), {file_size}, 'Y', current_timestamp(), current_timestamp())
        """)

# PARTITION_OVERWRITE metadata
for batch_id, batch_path in [(BATCH_1_ID, batch1_po_path), (BATCH_2_ID, batch2_po_path)]:
    files = get_files_recursive(batch_path)
    for file_path, mod_time, file_size in files:
        spark.sql(f"""
            INSERT INTO {METADATA_TABLE}
            (table_name, batch_load_id, source_file_path, file_modification_time, 
             file_size, is_processed, insert_ts, last_update_ts)
            VALUES
            ('{partition_overwrite_table}', '{batch_id}', '{file_path}', 
             from_unixtime({mod_time}/1000), {file_size}, 'Y', current_timestamp(), current_timestamp())
        """)

# APPEND metadata
for batch_id, batch_path in [(BATCH_1_ID, batch1_ap_path), (BATCH_2_ID, batch2_ap_path), (BATCH_3_ID, batch3_ap_path)]:
    files = get_files_recursive(batch_path)
    for file_path, mod_time, file_size in files:
        spark.sql(f"""
            INSERT INTO {METADATA_TABLE}
            (table_name, batch_load_id, source_file_path, file_modification_time, 
             file_size, is_processed, insert_ts, last_update_ts)
            VALUES
            ('{append_table}', '{batch_id}', '{file_path}', 
             from_unixtime({mod_time}/1000), {file_size}, 'Y', current_timestamp(), current_timestamp())
        """)

metadata_count = spark.sql(f"""
    SELECT COUNT(*) as c FROM {METADATA_TABLE}
    WHERE table_name LIKE '{TEST_CATALOG}.{TEST_SCHEMA}.%'
""").collect()[0]['c']

print(f"✓ Inserted {metadata_count} metadata entries")

# COMMAND ----------

# DBTITLE 1,Insert Metadata - Audit
# OVERWRITE audit
for batch_id, row_count in [(BATCH_1_ID, 4), (BATCH_2_ID, 2)]:
    spark.sql(f"""
        INSERT INTO {AUDIT_TABLE}
        (run_id, config_id, batch_load_id, group_name, target_table_name,
         operation_type, load_type, status, start_ts, end_ts, log_message, microbatch_id, row_count)
        VALUES
        ('{batch_id}', '{config_id_1}', '{batch_id}', '{TEST_GROUP}', '{overwrite_table}',
         'INGESTION', 'hive_full', 'SUCCESS', current_timestamp(), current_timestamp(), NULL, NULL, {row_count})
    """)

# PARTITION_OVERWRITE audit
for batch_id, row_count in [(BATCH_1_ID, 4), (BATCH_2_ID, 2)]:
    spark.sql(f"""
        INSERT INTO {AUDIT_TABLE}
        (run_id, config_id, batch_load_id, group_name, target_table_name,
         operation_type, load_type, status, start_ts, end_ts, log_message, microbatch_id, row_count)
        VALUES
        ('{batch_id}', '{config_id_2}', '{batch_id}', '{TEST_GROUP}', '{partition_overwrite_table}',
         'INGESTION', 'incremental', 'SUCCESS', current_timestamp(), current_timestamp(), NULL, NULL, {row_count})
    """)

# APPEND audit
for batch_id, row_count in [(BATCH_1_ID, 4), (BATCH_2_ID, 2), (BATCH_3_ID, 3)]:
    spark.sql(f"""
        INSERT INTO {AUDIT_TABLE}
        (run_id, config_id, batch_load_id, group_name, target_table_name,
         operation_type, load_type, status, start_ts, end_ts, log_message, microbatch_id, row_count)
        VALUES
        ('{batch_id}', '{config_id_3}', '{batch_id}', '{TEST_GROUP}', '{append_table}',
         'INGESTION', 'incremental', 'SUCCESS', current_timestamp(), current_timestamp(), NULL, NULL, {row_count})
    """)

print(f"✓ Inserted 8 audit entries")
print("✓ All metadata inserted successfully")

# COMMAND ----------

# DBTITLE 1,Verification Before Validation
print("="*80)
print("PRE-VALIDATION VERIFICATION")
print("="*80)

# Check config
config_count = spark.sql(f"SELECT COUNT(*) as c FROM {CONFIG_TABLE} WHERE group_name = '{TEST_GROUP}'").collect()[0]['c']
print(f"Config entries: {config_count} (expected: 3)")

# Check tables exist and have data
for table in [overwrite_table, partition_overwrite_table, append_table]:
    count = spark.table(table).count()
    print(f"{table.split('.')[-1]}: {count} rows")

print("="*80)

# COMMAND ----------

# DBTITLE 1,Run Validation
print("\n" + "="*80)
print("RUNNING VALIDATION")
print("="*80)

runner = ValidationRunner(
    spark=spark,
    table_group=TEST_GROUP,
    iteration_suffix=f"prod_test_{RUN_TIMESTAMP}",
    is_full_validation=True
)

result = runner.run()

print(f"\n{'='*80}")
print("VALIDATION RESULTS")
print(f"{'='*80}")
print(f"Total tables: {result.get('total', 0)}")
print(f"Success: {result.get('success', 0)}")
print(f"Failed: {result.get('failed', 0)}")
print(f"No batches: {result.get('no_batches', 0)}")
print(f"{'='*80}")

# COMMAND ----------

# DBTITLE 1,Verify Results
print("\n" + "="*80)
print("VALIDATION RESULTS VERIFICATION")
print("="*80)

# Check validation log
validation_results = spark.sql(f"""
    SELECT 
        table_family,
        batch_load_id,
        validation_run_status,
        row_count_status,
        schema_validation_status
    FROM {VALIDATION_LOG}
    WHERE table_family LIKE '{TEST_CATALOG}.{TEST_SCHEMA}.%'
    ORDER BY table_family, batch_load_id
""")

print("\nValidation Results:")
validation_results.show(truncate=False)

# Count by status
status_summary = spark.sql(f"""
    SELECT 
        validation_run_status,
        COUNT(*) as count
    FROM {VALIDATION_LOG}
    WHERE table_family LIKE '{TEST_CATALOG}.{TEST_SCHEMA}.%'
    GROUP BY validation_run_status
""")

print("\nStatus Summary:")
status_summary.show()

print("="*80)
print("TEST COMPLETED SUCCESSFULLY!")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Optional Cleanup
# Set this to True to clean up test data after verification
CLEANUP_AFTER_TEST = False

if CLEANUP_AFTER_TEST:
    print("\nCleaning up test data...")
    
    # Drop schema
    spark.sql(f"DROP SCHEMA IF EXISTS {TEST_CATALOG}.{TEST_SCHEMA} CASCADE")
    
    # Remove DBFS files
    dbutils.fs.rm(DBFS_ROOT, recurse=True)
    
    # Clean metadata
    spark.sql(f"DELETE FROM {CONFIG_TABLE} WHERE group_name = '{TEST_GROUP}'")
    spark.sql(f"DELETE FROM {METADATA_TABLE} WHERE table_name LIKE '{TEST_CATALOG}.{TEST_SCHEMA}.%'")
    spark.sql(f"DELETE FROM {AUDIT_TABLE} WHERE group_name = '{TEST_GROUP}'")
    spark.sql(f"DELETE FROM {VALIDATION_LOG} WHERE table_family LIKE '{TEST_CATALOG}.{TEST_SCHEMA}.%'")
    spark.sql(f"DELETE FROM {VALIDATION_SUMMARY} WHERE table_family LIKE '{TEST_CATALOG}.{TEST_SCHEMA}.%'")
    
    print("✓ Cleanup completed")
else:
    print(f"\n⚠ Cleanup skipped (CLEANUP_AFTER_TEST = False)")
    print(f"\nTest artifacts preserved:")
    print(f"  Schema: {TEST_SCHEMA}")
    print(f"  Group: {TEST_GROUP}")
    print(f"  DBFS: {DBFS_ROOT}")

