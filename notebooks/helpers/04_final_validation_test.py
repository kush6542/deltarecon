# Databricks notebook source
# MAGIC %md
# MAGIC # Final Validation Test - Production-Style Configuration
# MAGIC 
# MAGIC **Purpose**: Final end-to-end validation of deltarecon framework with production-style naming
# MAGIC 
# MAGIC **Test Scenarios:**
# MAGIC 1. OVERWRITE mode (no partitions)
# MAGIC 2. PARTITION_OVERWRITE mode (Hive-style partitions)
# MAGIC 3. APPEND mode (Hive-style partitions)
# MAGIC 
# MAGIC **Key Features:**
# MAGIC - Production-style file naming and paths
# MAGIC - Hive-style partition directories (partition_col=value/)
# MAGIC - Simulates actual ingestion process
# MAGIC - Self-contained (creates own test data and metadata)

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType, LongType
from pyspark.sql.functions import lit, current_timestamp, col, regexp_extract
from datetime import datetime
from deltarecon import ValidationRunner

print("✓ Libraries imported successfully")

# COMMAND ----------

# DBTITLE 1,Configuration - Production Style
print("="*80)
print("FINAL VALIDATION TEST - PRODUCTION CONFIGURATION")
print("="*80)

# Production-style configuration with timestamp (unique per run)
RUN_TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
RUN_ID = f"final_v2_{RUN_TIMESTAMP}"  # Version 2 to avoid conflicts
TEST_CATALOG = "ts42_demo"
TEST_SCHEMA = f"final_validation_{RUN_TIMESTAMP}"
TEST_GROUP = f"final_val_test_{RUN_TIMESTAMP}"

# Generate batch IDs (production style: BATCH_YYYYMMDD_HHMMSS_NNN)
BATCH_1_ID = f"BATCH_{RUN_TIMESTAMP}_001"
BATCH_2_ID = f"BATCH_{RUN_TIMESTAMP}_002"
BATCH_3_ID = f"BATCH_{RUN_TIMESTAMP}_003"

# Production-style DBFS paths
DBFS_ROOT = f"/FileStore/final_validation_v2_{RUN_TIMESTAMP}"
SOURCE_BASE_PATH = f"{DBFS_ROOT}/source_data"

# Metadata tables (actual production tables)
INGESTION_CONFIG_TABLE = "ts42_demo.migration_operations.serving_ingestion_config"
INGESTION_METADATA_TABLE = "ts42_demo.migration_operations.serving_ingestion_metadata"
INGESTION_AUDIT_TABLE = "ts42_demo.migration_operations.serving_ingestion_audit"

print(f"\nTest Configuration:")
print(f"  Run ID: {RUN_ID}")
print(f"  Catalog: {TEST_CATALOG}")
print(f"  Schema: {TEST_SCHEMA}")
print(f"  Group: {TEST_GROUP}")
print(f"  Source Path: {SOURCE_BASE_PATH}")
print(f"  Batch IDs: {BATCH_1_ID}, {BATCH_2_ID}, {BATCH_3_ID}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Cleanup Previous Test Data
print("\n" + "="*80)
print("CLEANING UP PREVIOUS TEST DATA")
print("="*80)

CLEAN_ALL = True

if CLEAN_ALL:
    # Clean up ALL old test schemas
    print("\n1. Finding and dropping old test schemas...")
    try:
        old_schemas = spark.sql(f"""
            SHOW SCHEMAS IN {TEST_CATALOG} LIKE 'final_validation%'
        """).collect()
        
        for row in old_schemas:
            schema_name = row['namespace']
            full_schema = f"{TEST_CATALOG}.{schema_name}"
            try:
                spark.sql(f"DROP SCHEMA IF EXISTS {full_schema} CASCADE")
                print(f"   ✓ Dropped schema: {full_schema}")
            except Exception as e:
                print(f"   ⚠ Could not drop {full_schema}: {e}")
    except Exception as e:
        print(f"   ⚠ Could not list schemas: {e}")
    
    print("\n2. Removing old DBFS files...")
    try:
        base_dir = "/FileStore/"
        files = dbutils.fs.ls(base_dir)
        for f in files:
            if "final_validation" in f.name:
                try:
                    dbutils.fs.rm(f.path, recurse=True)
                    print(f"   ✓ Removed: {f.path}")
                except Exception as e:
                    print(f"   ⚠ Could not remove {f.path}: {e}")
    except Exception as e:
        print(f"   ⚠ Could not clean DBFS: {e}")
    
    print("\n3. Cleaning metadata tables...")
    
    # Clean config
    try:
        spark.sql(f"""
            DELETE FROM {INGESTION_CONFIG_TABLE} 
            WHERE group_name LIKE 'final_val_test%'
        """)
        config_count = spark.sql(f"""
            SELECT COUNT(*) as count FROM {INGESTION_CONFIG_TABLE}
            WHERE group_name LIKE 'final_val_test%'
        """).collect()[0]['count']
        print(f"   ✓ Cleaned {INGESTION_CONFIG_TABLE} (remaining: {config_count})")
    except Exception as e:
        print(f"   ⚠ Could not clean config: {e}")
    
    # Clean metadata
    try:
        spark.sql(f"""
            DELETE FROM {INGESTION_METADATA_TABLE} 
            WHERE table_name LIKE '{TEST_CATALOG}.final_validation%'
        """)
        metadata_count = spark.sql(f"""
            SELECT COUNT(*) as count FROM {INGESTION_METADATA_TABLE}
            WHERE table_name LIKE '{TEST_CATALOG}.final_validation%'
        """).collect()[0]['count']
        print(f"   ✓ Cleaned {INGESTION_METADATA_TABLE} (remaining: {metadata_count})")
    except Exception as e:
        print(f"   ⚠ Could not clean metadata: {e}")
    
    # Clean audit
    try:
        spark.sql(f"""
            DELETE FROM {INGESTION_AUDIT_TABLE} 
            WHERE group_name LIKE 'final_val_test%'
        """)
        audit_count = spark.sql(f"""
            SELECT COUNT(*) as count FROM {INGESTION_AUDIT_TABLE}
            WHERE group_name LIKE 'final_val_test%'
        """).collect()[0]['count']
        print(f"   ✓ Cleaned {INGESTION_AUDIT_TABLE} (remaining: {audit_count})")
    except Exception as e:
        print(f"   ⚠ Could not clean audit: {e}")
    
    print("\n✓ Cleanup completed!")

print(f"\nCurrent test will use:")
print(f"  Schema: {TEST_SCHEMA}")
print(f"  Group: {TEST_GROUP}")
print(f"  Source Path: {SOURCE_BASE_PATH}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Create Test Schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {TEST_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_CATALOG}.{TEST_SCHEMA}")
print(f"✓ Created schema: {TEST_CATALOG}.{TEST_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Define Test Data - Production Style
print("="*80)
print("PRODUCTION-STYLE TEST DATA CONFIGURATION")
print("="*80)
print("• Source files: Hive-style partitioned directories")
print("• File format: ORC (production standard)")
print("• Partition columns: Extracted from directory structure")
print("• Naming: Production-style batch IDs and paths")
print("="*80)

# Order transaction schema (typical production schema)
order_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_amount", DoubleType(), True),
    StructField("order_status", StringType(), True),
    # partition_date will be in directory structure, NOT in data
])

# Batch 1 data (simulating production data)
batch1_orders = [
    (100001, 50001, 125.50, "COMPLETED"),
    (100002, 50002, 250.00, "PENDING"),
    (100003, 50003, 175.25, "COMPLETED"),
    (100004, 50004, 300.00, "SHIPPED"),
]

# Batch 2 data
batch2_orders = [
    (200001, 60001, 450.00, "COMPLETED"),
    (200002, 60002, 325.75, "PENDING"),
]

# Batch 3 data
batch3_orders = [
    (300001, 70001, 500.00, "COMPLETED"),
    (300002, 70002, 425.50, "SHIPPED"),
    (300003, 70003, 275.00, "PENDING"),
]

print(f"\n✓ Test data defined:")
print(f"  Batch 1: {len(batch1_orders)} orders")
print(f"  Batch 2: {len(batch2_orders)} orders")
print(f"  Batch 3: {len(batch3_orders)} orders")
print(f"  Schema: {len(order_schema.fields)} columns (NO partition columns in data)")

# COMMAND ----------

# DBTITLE 1,Scenario 1: OVERWRITE Mode - Transaction Summary Table
print("\n" + "="*80)
print("SCENARIO 1: OVERWRITE MODE - Transaction Summary")
print("="*80)
print("Use Case: Daily summary table that gets completely replaced")
print("="*80)

overwrite_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.transaction_summary"
overwrite_source_path = f"{SOURCE_BASE_PATH}/transaction_summary"

# Batch 1: Initial summary
print(f"\nBatch 1 ({BATCH_1_ID}): Initial load with {len(batch1_orders)} records")
df1_ow = spark.createDataFrame(batch1_orders, order_schema)

# Save source files (no partitioning for overwrite mode)
batch1_ow_path = f"{overwrite_source_path}/{BATCH_1_ID}"
df1_ow.write.mode("overwrite").format("orc").save(batch1_ow_path)
print(f"  ✓ Source files: {batch1_ow_path}")

# Simulate ingestion: Write to Delta
df1_ow.withColumn("_aud_batch_load_id", lit(BATCH_1_ID)) \
     .write.mode("overwrite").format("delta").saveAsTable(overwrite_table)
print(f"  ✓ Delta table: {spark.table(overwrite_table).count()} rows")

# Batch 2: Complete replacement (BATCH 1 GONE!)
print(f"\nBatch 2 ({BATCH_2_ID}): OVERWRITE with {len(batch2_orders)} records")
df2_ow = spark.createDataFrame(batch2_orders, order_schema)

# Save source files
batch2_ow_path = f"{overwrite_source_path}/{BATCH_2_ID}"
df2_ow.write.mode("overwrite").format("orc").save(batch2_ow_path)
print(f"  ✓ Source files: {batch2_ow_path}")

# Simulate ingestion: Overwrite Delta (BATCH 1 data lost!)
df2_ow.withColumn("_aud_batch_load_id", lit(BATCH_2_ID)) \
     .write.mode("overwrite").format("delta").saveAsTable(overwrite_table)
print(f"  ✓ Delta table: {spark.table(overwrite_table).count()} rows")

# Verify only BATCH_2 exists
actual_batches = [row['_aud_batch_load_id'] for row in 
                 spark.table(overwrite_table).select("_aud_batch_load_id").distinct().collect()]
print(f"\n  Final state: {actual_batches}")
assert actual_batches == [BATCH_2_ID], f"Expected only {BATCH_2_ID}!"

print(f"\n✓ OVERWRITE scenario created: {overwrite_table}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Scenario 2: PARTITION_OVERWRITE Mode - Daily Orders (Hive Partitions)
print("\n" + "="*80)
print("SCENARIO 2: PARTITION_OVERWRITE MODE - Daily Orders")
print("="*80)
print("Use Case: Daily partitioned table where partitions get overwritten")
print("="*80)

partition_overwrite_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.daily_orders_partitioned"
partition_overwrite_source_path = f"{SOURCE_BASE_PATH}/daily_orders"

# Batch 1: Load for 2024-11-08 (3 orders) and 2024-11-09 (1 order)
print(f"\nBatch 1 ({BATCH_1_ID}): Load partitions 2024-11-08 & 2024-11-09")
df1_po = spark.createDataFrame(batch1_orders, order_schema)

# Split data by partition
df1_po_part1 = df1_po.limit(3).withColumn("partition_date", lit("2024-11-08"))  # 3 orders
df1_po_part2 = df1_po.tail(1)[0]
df1_po_part2 = spark.createDataFrame([df1_po_part2], order_schema).withColumn("partition_date", lit("2024-11-09"))  # 1 order

# Save source files in Hive-style directories
batch1_po_path = f"{partition_overwrite_source_path}/{BATCH_1_ID}"
df1_po_part1.write.mode("overwrite").format("orc").partitionBy("partition_date").save(batch1_po_path)
df1_po_part2.write.mode("append").format("orc").partitionBy("partition_date").save(batch1_po_path)
print(f"  ✓ Source files: {batch1_po_path}/partition_date=<value>/")

# Simulate ingestion: Read with partition extraction
source_df1_po = spark.read.format("orc").load(batch1_po_path)
source_df1_po = source_df1_po.withColumn("partition_date", 
    regexp_extract(col("_metadata.file_path"), "partition_date=([^/]+)", 1))

# Write to Delta
source_df1_po.withColumn("_aud_batch_load_id", lit(BATCH_1_ID)) \
    .write.mode("overwrite").format("delta") \
    .partitionBy("partition_date").saveAsTable(partition_overwrite_table)

print(f"  ✓ Delta table: {spark.table(partition_overwrite_table).count()} rows")
print(f"     Partitions: 2024-11-08 (3 rows), 2024-11-09 (1 row)")

# Batch 2: Overwrite 2024-11-08 & add new partition 2024-11-10
print(f"\nBatch 2 ({BATCH_2_ID}): OVERWRITE partition 2024-11-08 + add 2024-11-10")
df2_po = spark.createDataFrame(batch2_orders, order_schema)

# Split data by partition
df2_po_part1 = df2_po.limit(1).withColumn("partition_date", lit("2024-11-08"))  # Overwrites!
df2_po_part2 = df2_po.tail(1)[0]
df2_po_part2 = spark.createDataFrame([df2_po_part2], order_schema).withColumn("partition_date", lit("2024-11-10"))

# Save source files
batch2_po_path = f"{partition_overwrite_source_path}/{BATCH_2_ID}"
df2_po_part1.write.mode("overwrite").format("orc").partitionBy("partition_date").save(batch2_po_path)
df2_po_part2.write.mode("append").format("orc").partitionBy("partition_date").save(batch2_po_path)
print(f"  ✓ Source files: {batch2_po_path}/partition_date=<value>/")

# Simulate ingestion: Read with partition extraction
source_df2_po = spark.read.format("orc").load(batch2_po_path)
source_df2_po = source_df2_po.withColumn("partition_date", 
    regexp_extract(col("_metadata.file_path"), "partition_date=([^/]+)", 1))

# Dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
source_df2_po.withColumn("_aud_batch_load_id", lit(BATCH_2_ID)) \
    .write.mode("overwrite").format("delta") \
    .partitionBy("partition_date").saveAsTable(partition_overwrite_table)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

print(f"  ✓ Delta table: {spark.table(partition_overwrite_table).count()} rows")

# Verify final state
result = spark.sql(f"""
    SELECT partition_date, _aud_batch_load_id, COUNT(*) as row_count
    FROM {partition_overwrite_table}
    GROUP BY partition_date, _aud_batch_load_id
    ORDER BY partition_date
""").collect()

print(f"\n  Final partition state:")
for row in result:
    print(f"    {row['partition_date']}: {row['_aud_batch_load_id']} ({row['row_count']} rows)")

print(f"\n  Expected:")
print(f"    2024-11-08: {BATCH_2_ID} (overwritten)")
print(f"    2024-11-09: {BATCH_1_ID} (untouched)")
print(f"    2024-11-10: {BATCH_2_ID} (new)")

print(f"\n✓ PARTITION_OVERWRITE scenario created: {partition_overwrite_table}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Scenario 3: APPEND Mode - Order History (Hive Partitions)
print("\n" + "="*80)
print("SCENARIO 3: APPEND MODE - Order History")
print("="*80)
print("Use Case: Historical order log where all batches coexist")
print("="*80)

append_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.order_history"
append_source_path = f"{SOURCE_BASE_PATH}/order_history"

# Batch 1: Initial load
print(f"\nBatch 1 ({BATCH_1_ID}): Initial load with {len(batch1_orders)} orders")
df1_ap = spark.createDataFrame(batch1_orders, order_schema)

# Split by partition
df1_ap_part1 = df1_ap.limit(2).withColumn("partition_date", lit("2024-11-08"))
df1_ap_part2 = df1_ap.subtract(df1_ap_part1.select(*order_schema.fieldNames())).withColumn("partition_date", lit("2024-11-09"))

# Save source files
batch1_ap_path = f"{append_source_path}/{BATCH_1_ID}"
df1_ap_part1.write.mode("overwrite").format("orc").partitionBy("partition_date").save(batch1_ap_path)
df1_ap_part2.write.mode("append").format("orc").partitionBy("partition_date").save(batch1_ap_path)
print(f"  ✓ Source files: {batch1_ap_path}/partition_date=<value>/")

# Simulate ingestion
source_df1_ap = spark.read.format("orc").load(batch1_ap_path)
source_df1_ap = source_df1_ap.withColumn("partition_date", 
    regexp_extract(col("_metadata.file_path"), "partition_date=([^/]+)", 1))
source_df1_ap.withColumn("_aud_batch_load_id", lit(BATCH_1_ID)) \
    .write.mode("overwrite").format("delta") \
    .partitionBy("partition_date").saveAsTable(append_table)
print(f"  ✓ Delta table: {spark.table(append_table).count()} rows")

# Batch 2: Append more data
print(f"\nBatch 2 ({BATCH_2_ID}): APPEND {len(batch2_orders)} orders")
df2_ap = spark.createDataFrame(batch2_orders, order_schema)

# Split by partition
df2_ap_part1 = df2_ap.limit(1).withColumn("partition_date", lit("2024-11-08"))
df2_ap_part2 = df2_ap.tail(1)[0]
df2_ap_part2 = spark.createDataFrame([df2_ap_part2], order_schema).withColumn("partition_date", lit("2024-11-10"))

# Save source files
batch2_ap_path = f"{append_source_path}/{BATCH_2_ID}"
df2_ap_part1.write.mode("overwrite").format("orc").partitionBy("partition_date").save(batch2_ap_path)
df2_ap_part2.write.mode("append").format("orc").partitionBy("partition_date").save(batch2_ap_path)
print(f"  ✓ Source files: {batch2_ap_path}/partition_date=<value>/")

# Simulate ingestion (APPEND mode)
source_df2_ap = spark.read.format("orc").load(batch2_ap_path)
source_df2_ap = source_df2_ap.withColumn("partition_date", 
    regexp_extract(col("_metadata.file_path"), "partition_date=([^/]+)", 1))
source_df2_ap.withColumn("_aud_batch_load_id", lit(BATCH_2_ID)) \
    .write.mode("append").format("delta") \
    .partitionBy("partition_date").saveAsTable(append_table)
print(f"  ✓ Delta table: {spark.table(append_table).count()} rows")

# Batch 3: Append even more data
print(f"\nBatch 3 ({BATCH_3_ID}): APPEND {len(batch3_orders)} orders")
df3_ap = spark.createDataFrame(batch3_orders, order_schema)
df3_ap = df3_ap.withColumn("partition_date", lit("2024-11-11"))

# Save source files
batch3_ap_path = f"{append_source_path}/{BATCH_3_ID}"
df3_ap.write.mode("overwrite").format("orc").partitionBy("partition_date").save(batch3_ap_path)
print(f"  ✓ Source files: {batch3_ap_path}/partition_date=<value>/")

# Simulate ingestion (APPEND mode)
source_df3_ap = spark.read.format("orc").load(batch3_ap_path)
source_df3_ap = source_df3_ap.withColumn("partition_date", 
    regexp_extract(col("_metadata.file_path"), "partition_date=([^/]+)", 1))
source_df3_ap.withColumn("_aud_batch_load_id", lit(BATCH_3_ID)) \
    .write.mode("append").format("delta") \
    .partitionBy("partition_date").saveAsTable(append_table)
print(f"  ✓ Delta table: {spark.table(append_table).count()} rows")

# Verify all batches coexist
result = spark.sql(f"""
    SELECT _aud_batch_load_id, COUNT(*) as row_count
    FROM {append_table}
    GROUP BY _aud_batch_load_id
    ORDER BY _aud_batch_load_id
""").collect()

print(f"\n  Final state - All batches coexist:")
for row in result:
    print(f"    {row['_aud_batch_load_id']}: {row['row_count']} rows")

total_rows = spark.table(append_table).count()
expected_rows = len(batch1_orders) + len(batch2_orders) + len(batch3_orders)
print(f"\n  Total: {total_rows} rows (expected: {expected_rows})")
assert total_rows == expected_rows, "Row count mismatch!"

print(f"\n✓ APPEND scenario created: {append_table}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Insert Metadata - Production Style
print("\n" + "="*80)
print("INSERTING METADATA FOR TEST TABLES")
print("="*80)

# Helper function to get file metadata recursively
def get_file_metadata_recursive(path):
    try:
        all_files = []
        
        def recurse_dir(dir_path):
            items = dbutils.fs.ls(dir_path)
            for item in items:
                if item.isDir():
                    recurse_dir(item.path)
                else:
                    if not item.name.startswith('_') and item.size > 0:
                        all_files.append((
                            item.path, 
                            datetime.fromtimestamp(item.modificationTime / 1000), 
                            item.size
                        ))
        
        recurse_dir(path)
        return all_files
    except Exception as e:
        print(f"  ⚠ Error reading {path}: {e}")
        return []

# 1. Insert into serving_ingestion_config
# Use SQL INSERT instead of DataFrame to avoid type inference issues with None values

print("\nInserting config entries...")

# OVERWRITE table config
config_id_1 = f"FINAL_TEST_001_{RUN_TIMESTAMP}"
spark.sql(f"""
    INSERT INTO {INGESTION_CONFIG_TABLE} 
    (config_id, group_name, source_schema, source_table, source_file_path, 
     target_catalog, target_schema, target_table, source_file_format, source_file_options,
     load_type, write_mode, primary_key, partition_column, partitioning_strategy,
     frequency, table_size_gb, is_active, deduplicate, clean_column_names, insert_ts, last_update_ts)
    VALUES 
    ('{config_id_1}', '{TEST_GROUP}', 'source_transactions', 'transaction_summary', 
     '{overwrite_source_path}', '{TEST_CATALOG}', '{TEST_SCHEMA}', 'transaction_summary', 
     'orc', NULL, 'hive_full', 'overwrite', 'order_id', NULL, NULL,
     'daily', '1', 'Y', 'N', 'N', current_timestamp(), current_timestamp())
""")

# PARTITION_OVERWRITE table config
config_id_2 = f"FINAL_TEST_002_{RUN_TIMESTAMP}"
spark.sql(f"""
    INSERT INTO {INGESTION_CONFIG_TABLE} 
    (config_id, group_name, source_schema, source_table, source_file_path, 
     target_catalog, target_schema, target_table, source_file_format, source_file_options,
     load_type, write_mode, primary_key, partition_column, partitioning_strategy,
     frequency, table_size_gb, is_active, deduplicate, clean_column_names, insert_ts, last_update_ts)
    VALUES 
    ('{config_id_2}', '{TEST_GROUP}', 'source_orders', 'daily_orders', 
     '{partition_overwrite_source_path}', '{TEST_CATALOG}', '{TEST_SCHEMA}', 'daily_orders_partitioned', 
     'orc', NULL, 'incremental', 'partition_overwrite', 'order_id', 'partition_date', 'PARTITIONED_BY',
     'daily', '1', 'Y', 'N', 'N', current_timestamp(), current_timestamp())
""")

# APPEND table config
config_id_3 = f"FINAL_TEST_003_{RUN_TIMESTAMP}"
spark.sql(f"""
    INSERT INTO {INGESTION_CONFIG_TABLE} 
    (config_id, group_name, source_schema, source_table, source_file_path, 
     target_catalog, target_schema, target_table, source_file_format, source_file_options,
     load_type, write_mode, primary_key, partition_column, partitioning_strategy,
     frequency, table_size_gb, is_active, deduplicate, clean_column_names, insert_ts, last_update_ts)
    VALUES 
    ('{config_id_3}', '{TEST_GROUP}', 'source_history', 'order_history', 
     '{append_source_path}', '{TEST_CATALOG}', '{TEST_SCHEMA}', 'order_history', 
     'orc', NULL, 'incremental', 'append', 'order_id', 'partition_date', 'PARTITIONED_BY',
     'daily', '1', 'Y', 'N', 'N', current_timestamp(), current_timestamp())
""")

print(f"✓ Inserted 3 entries into {INGESTION_CONFIG_TABLE}")

# 2. Insert into serving_ingestion_metadata
metadata_entries = []

# OVERWRITE table metadata
for batch_id, batch_path in [(BATCH_1_ID, batch1_ow_path), (BATCH_2_ID, batch2_ow_path)]:
    files = get_file_metadata_recursive(batch_path)
    for file_path, mod_time, file_size in files:
        metadata_entries.append({
            "table_name": overwrite_table,
            "batch_load_id": batch_id,
            "source_file_path": file_path,
            "file_modification_time": mod_time,
            "file_size": file_size,
            "is_processed": "Y",
            "insert_ts": datetime.now(),
            "last_update_ts": datetime.now()
        })

# PARTITION_OVERWRITE table metadata
for batch_id, batch_path in [(BATCH_1_ID, batch1_po_path), (BATCH_2_ID, batch2_po_path)]:
    files = get_file_metadata_recursive(batch_path)
    for file_path, mod_time, file_size in files:
        metadata_entries.append({
            "table_name": partition_overwrite_table,
            "batch_load_id": batch_id,
            "source_file_path": file_path,
            "file_modification_time": mod_time,
            "file_size": file_size,
            "is_processed": "Y",
            "insert_ts": datetime.now(),
            "last_update_ts": datetime.now()
        })

# APPEND table metadata
for batch_id, batch_path in [(BATCH_1_ID, batch1_ap_path), (BATCH_2_ID, batch2_ap_path), (BATCH_3_ID, batch3_ap_path)]:
    files = get_file_metadata_recursive(batch_path)
    for file_path, mod_time, file_size in files:
        metadata_entries.append({
            "table_name": append_table,
            "batch_load_id": batch_id,
            "source_file_path": file_path,
            "file_modification_time": mod_time,
            "file_size": file_size,
            "is_processed": "Y",
            "insert_ts": datetime.now(),
            "last_update_ts": datetime.now()
        })

# Insert metadata entries
if metadata_entries:
    metadata_df = spark.createDataFrame(metadata_entries)
    metadata_df.write.mode("append").saveAsTable(INGESTION_METADATA_TABLE)
    print(f"✓ Inserted {len(metadata_entries)} entries into {INGESTION_METADATA_TABLE}")

# 3. Insert into serving_ingestion_audit
audit_entries = []

# OVERWRITE table audit entries
for batch_id in [BATCH_1_ID, BATCH_2_ID]:
    audit_entries.append({
        "run_id": batch_id,  # Using batch_id as run_id
        "config_id": config_id_1,
        "batch_load_id": batch_id,
        "group_name": TEST_GROUP,
        "target_table_name": overwrite_table,
        "operation_type": "INGESTION",
        "load_type": "hive_full",
        "status": "SUCCESS",
        "start_ts": datetime.now(),
        "end_ts": datetime.now(),
        "log_message": None,
        "microbatch_id": None,
        "row_count": 2 if batch_id == BATCH_2_ID else 4
    })

# PARTITION_OVERWRITE table audit entries
for batch_id, row_count in [(BATCH_1_ID, 4), (BATCH_2_ID, 2)]:
    audit_entries.append({
        "run_id": batch_id,
        "config_id": config_id_2,
        "batch_load_id": batch_id,
        "group_name": TEST_GROUP,
        "target_table_name": partition_overwrite_table,
        "operation_type": "INGESTION",
        "load_type": "incremental",
        "status": "SUCCESS",
        "start_ts": datetime.now(),
        "end_ts": datetime.now(),
        "log_message": None,
        "microbatch_id": None,
        "row_count": row_count
    })

# APPEND table audit entries
for batch_id, row_count in [(BATCH_1_ID, 4), (BATCH_2_ID, 2), (BATCH_3_ID, 3)]:
    audit_entries.append({
        "run_id": batch_id,
        "config_id": config_id_3,
        "batch_load_id": batch_id,
        "group_name": TEST_GROUP,
        "target_table_name": append_table,
        "operation_type": "INGESTION",
        "load_type": "incremental",
        "status": "SUCCESS",
        "start_ts": datetime.now(),
        "end_ts": datetime.now(),
        "log_message": None,
        "microbatch_id": None,
        "row_count": row_count
    })

# Insert audit entries - Use SQL INSERT to handle None values
print("\nInserting audit entries...")
for entry in audit_entries:
    spark.sql(f"""
        INSERT INTO {INGESTION_AUDIT_TABLE}
        (run_id, config_id, batch_load_id, group_name, target_table_name,
         operation_type, load_type, status, start_ts, end_ts, log_message, microbatch_id, row_count)
        VALUES
        ('{entry["run_id"]}', '{entry["config_id"]}', '{entry["batch_load_id"]}', 
         '{entry["group_name"]}', '{entry["target_table_name"]}',
         '{entry["operation_type"]}', '{entry["load_type"]}', '{entry["status"]}',
         current_timestamp(), current_timestamp(), NULL, NULL, {entry["row_count"]})
    """)
print(f"✓ Inserted {len(audit_entries)} entries into {INGESTION_AUDIT_TABLE}")

print(f"\n✓ All metadata inserted successfully")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Run Validation - Final Test
print("\n" + "="*80)
print("RUNNING DELTARECON VALIDATION - FINAL TEST")
print("="*80)

# Initialize ValidationRunner
runner = ValidationRunner(
    spark=spark,
    table_group=TEST_GROUP,
    iteration_suffix=f"final_v2_{RUN_TIMESTAMP}",
    is_full_validation=True
)

# Run validation
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
print("VERIFICATION - Check Validation Log")
print("="*80)

# Check validation log
validation_log = spark.sql(f"""
    SELECT 
        table_family,
        batch_load_id,
        validation_run_status,
        validation_run_start_time
    FROM cat_ril_nayeem_03.validation_v2.validation_log
    WHERE workflow_name LIKE '%{TEST_GROUP}%'
    ORDER BY validation_run_start_time DESC
    LIMIT 10
""")

print("\nValidation Log:")
validation_log.show(truncate=False)

# Check validation summary
validation_summary = spark.sql(f"""
    SELECT 
        table_family,
        batch_load_id,
        row_count_status,
        schema_validation_status,
        pk_compliance_status,
        overall_status
    FROM cat_ril_nayeem_03.validation_v2.validation_summary
    WHERE workflow_name LIKE '%{TEST_GROUP}%'
    ORDER BY validation_run_start_time DESC
    LIMIT 10
""")

print("\nValidation Summary:")
validation_summary.show(truncate=False)

print("="*80)

# COMMAND ----------

# DBTITLE 1,Final Cleanup (Optional)
print("\n" + "="*80)
print("FINAL CLEANUP - Remove Test Data")
print("="*80)

CLEANUP = False  # Set to True to cleanup

if CLEANUP:
    print(f"\nCleaning up test data for: {TEST_SCHEMA}")
    
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {TEST_CATALOG}.{TEST_SCHEMA} CASCADE")
        print(f"  ✓ Dropped schema: {TEST_SCHEMA}")
    except Exception as e:
        print(f"  ⚠ {e}")
    
    try:
        dbutils.fs.rm(DBFS_ROOT, recurse=True)
        print(f"  ✓ Removed DBFS files: {DBFS_ROOT}")
    except Exception as e:
        print(f"  ⚠ {e}")
    
    try:
        spark.sql(f"DELETE FROM {INGESTION_CONFIG_TABLE} WHERE group_name = '{TEST_GROUP}'")
        spark.sql(f"DELETE FROM {INGESTION_METADATA_TABLE} WHERE table_name LIKE '{TEST_CATALOG}.{TEST_SCHEMA}.%'")
        spark.sql(f"DELETE FROM {INGESTION_AUDIT_TABLE} WHERE group_name = '{TEST_GROUP}'")
        print(f"  ✓ Cleaned metadata tables")
    except Exception as e:
        print(f"  ⚠ {e}")
    
    print(f"\n✓ Cleanup completed for run {RUN_ID}")
else:
    print("\n⚠ Cleanup skipped (CLEANUP = False)")
    print(f"\nTo cleanup this test run:")
    print(f"  1. Set CLEANUP = True")
    print(f"  2. Re-run this cell")
    print(f"\nTest artifacts:")
    print(f"  - Schema: {TEST_SCHEMA}")
    print(f"  - DBFS Path: {DBFS_ROOT}")
    print(f"  - Metadata group: {TEST_GROUP}")

print("="*80)

