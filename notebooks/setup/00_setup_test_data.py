# Databricks notebook source
"""
Setup Test Data for DeltaRecon Validation Framework

This notebook creates:
1. Source ORC files in DBFS
2. Target Delta tables with test data
3. Ingestion metadata tables (config, audit, metadata, partition mapping)
4. Two table groups:
   - group1_basic: Basic validation checks (row count, schema, PK)
   - group2_advanced: Advanced checks (write mode, multiple PKs, partitions)

Run this notebook to set up test data for validation framework testing.
"""

# COMMAND ----------

# DBTITLE 1,Load Constants and Imports
from deltarecon.config import constants
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import col, lit, current_timestamp, when
from datetime import datetime, timedelta
import random

print(f"Framework Version: {constants.FRAMEWORK_VERSION}")
print(f"Ingestion Catalog: {constants.INGESTION_OPS_CATALOG}")
print(f"Ingestion Schema: {constants.INGESTION_OPS_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Setup: Create Catalogs and Schemas
# Create catalog and schema if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {constants.INGESTION_OPS_CATALOG}")
spark.sql(f"USE CATALOG {constants.INGESTION_OPS_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {constants.INGESTION_OPS_SCHEMA.split('.')[1]}")
spark.sql(f"USE SCHEMA {constants.INGESTION_OPS_SCHEMA.split('.')[1]}")

print(f"Catalog and schema ready: {constants.INGESTION_OPS_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Setup: Create Ingestion Metadata Tables (if not exist)
# Create ingestion_config table
ingestion_config_ddl = f"""
CREATE TABLE IF NOT EXISTS {constants.INGESTION_CONFIG_TABLE} (
  group_name STRING,
  target_catalog STRING,
  target_schema STRING,
  target_table STRING,
  source_schema STRING,
  source_table STRING,
  write_mode STRING,
  primary_key STRING,
  partition_column STRING,
  is_active STRING
)
USING DELTA
COMMENT 'Ingestion configuration table'
"""

spark.sql(ingestion_config_ddl)
print(f"Created/verified: {constants.INGESTION_CONFIG_TABLE}")

# COMMAND ----------

# Create ingestion_audit table
ingestion_audit_ddl = f"""
CREATE TABLE IF NOT EXISTS {constants.INGESTION_AUDIT_TABLE} (
  batch_load_id STRING,
  target_table_name STRING,
  status STRING,
  group_name STRING,
  load_start_time TIMESTAMP,
  load_end_time TIMESTAMP
)
USING DELTA
COMMENT 'Ingestion audit table'
"""

spark.sql(ingestion_audit_ddl)
print(f"Created/verified: {constants.INGESTION_AUDIT_TABLE}")

# COMMAND ----------

# Create ingestion_metadata table
ingestion_metadata_ddl = f"""
CREATE TABLE IF NOT EXISTS {constants.INGESTION_METADATA_TABLE} (
  table_name STRING,
  batch_load_id STRING,
  source_file_path STRING,
  file_size BIGINT,
  record_count BIGINT
)
USING DELTA
COMMENT 'Ingestion metadata table'
"""

spark.sql(ingestion_metadata_ddl)
print(f"Created/verified: {constants.INGESTION_METADATA_TABLE}")

# COMMAND ----------

# Create source_table_partition_mapping table
partition_mapping_ddl = f"""
CREATE TABLE IF NOT EXISTS {constants.INGESTION_SRC_TABLE_PARTITION_MAPPING} (
  schema_name STRING,
  table_name STRING,
  partition_column_name STRING,
  datatype STRING,
  index INT
)
USING DELTA
COMMENT 'Source table partition mapping'
"""

spark.sql(partition_mapping_ddl)
print(f"Created/verified: {constants.INGESTION_SRC_TABLE_PARTITION_MAPPING}")

# COMMAND ----------

# DBTITLE 1,Setup: Create Target Catalog and Schema
target_catalog = "ts42_demo"
target_schema = "taxi_example"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
spark.sql(f"USE CATALOG {target_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
spark.sql(f"USE SCHEMA {target_schema}")

print(f"Target catalog/schema ready: {target_catalog}.{target_schema}")

# COMMAND ----------

# DBTITLE 1,Setup: Base Paths for ORC Files
# Base path for storing ORC files in DBFS
dbfs_base_path = "/FileStore/test_data/deltarecon"
orc_base_path = f"{dbfs_base_path}/orc_files"

# Clean up existing test data (optional - comment out if you want to keep existing data)
# dbutils.fs.rm(orc_base_path, True)

print(f"ORC files will be stored at: {orc_base_path}")

# COMMAND ----------

# DBTITLE 1,Group 1 - Basic: Table 1 - Row Count PASS (Perfect Match)
table_name = "group1_rowcount_pass"
batch_id = "TEST_20250101_120000"

# Create source ORC data
source_data = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75),
    (3, "Charlie", 300, 3000.25),
    (4, "Diana", 400, 4000.00),
    (5, "Eve", 500, 5000.50)
]

source_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

source_df = spark.createDataFrame(source_data, source_schema)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

# Create target Delta table (identical data)
target_df = source_df.withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"✅ Created {table_name} - Row count will PASS (5 rows = 5 rows)")

# COMMAND ----------

# DBTITLE 1,Group 1 - Basic: Table 2 - Row Count FAIL (Source has more rows)
table_name = "group1_rowcount_fail_source_more"
batch_id = "TEST_20250101_120001"

# Source has 7 rows
source_data = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75),
    (3, "Charlie", 300, 3000.25),
    (4, "Diana", 400, 4000.00),
    (5, "Eve", 500, 5000.50),
    (6, "Frank", 600, 6000.00),
    (7, "Grace", 700, 7000.00)
]

source_df = spark.createDataFrame(source_data, source_schema)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

# Target has only 5 rows (missing 2 rows)
target_data = source_data[:5]
target_df = spark.createDataFrame(target_data, source_schema).withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"❌ Created {table_name} - Row count will FAIL (Source: 7, Target: 5)")

# COMMAND ----------

# DBTITLE 1,Group 1 - Basic: Table 3 - Row Count FAIL (Target has more rows)
table_name = "group1_rowcount_fail_target_more"
batch_id = "TEST_20250101_120002"

# Source has 5 rows
source_data = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75),
    (3, "Charlie", 300, 3000.25),
    (4, "Diana", 400, 4000.00),
    (5, "Eve", 500, 5000.50)
]

source_df = spark.createDataFrame(source_data, source_schema)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

# Target has 7 rows (2 extra rows)
target_data = source_data + [(6, "Frank", 600, 6000.00), (7, "Grace", 700, 7000.00)]
target_df = spark.createDataFrame(target_data, source_schema).withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"❌ Created {table_name} - Row count will FAIL (Source: 5, Target: 7)")

# COMMAND ----------

# DBTITLE 1,Group 1 - Basic: Table 4 - Schema PASS (Perfect Match)
table_name = "group1_schema_pass"
batch_id = "TEST_20250101_120003"

source_data = [
    (1, "Alice", 100, 1000.50, "2024-01-01"),
    (2, "Bob", 200, 2000.75, "2024-01-02"),
    (3, "Charlie", 300, 3000.25, "2024-01-03")
]

source_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("date_col", StringType(), True)
])

source_df = spark.createDataFrame(source_data, source_schema)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

# Target has same schema
target_df = source_df.withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"✅ Created {table_name} - Schema will PASS (perfect match)")

# COMMAND ----------

# DBTITLE 1,Group 1 - Basic: Table 5 - Schema FAIL (Missing Column)
table_name = "group1_schema_fail_missing_col"
batch_id = "TEST_20250101_120004"

source_data = [
    (1, "Alice", 100, 1000.50, "2024-01-01"),
    (2, "Bob", 200, 2000.75, "2024-01-02"),
    (3, "Charlie", 300, 3000.25, "2024-01-03")
]

source_df = spark.createDataFrame(source_data, source_schema)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

# Target missing 'date_col'
target_schema_no_date = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("price", DoubleType(), True)
])
target_data = [(row[0], row[1], row[2], row[3]) for row in source_data]
target_df = spark.createDataFrame(target_data, target_schema_no_date).withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"❌ Created {table_name} - Schema will FAIL (target missing 'date_col')")

# COMMAND ----------

# DBTITLE 1,Group 1 - Basic: Table 6 - Schema FAIL (Type Mismatch)
table_name = "group1_schema_fail_type_mismatch"
batch_id = "TEST_20250101_120005"

source_data = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75),
    (3, "Charlie", 300, 3000.25)
]

source_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

source_df = spark.createDataFrame(source_data, source_schema)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

# Target has 'amount' as String instead of Integer
target_schema_type_mismatch = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", StringType(), True),  # Type mismatch!
    StructField("price", DoubleType(), True)
])
target_data = [(row[0], row[1], str(row[2]), row[3]) for row in source_data]
target_df = spark.createDataFrame(target_data, target_schema_type_mismatch).withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"❌ Created {table_name} - Schema will FAIL (type mismatch: amount INT vs STRING)")

# COMMAND ----------

# DBTITLE 1,Group 1 - Basic: Table 7 - PK PASS (No Duplicates)
table_name = "group1_pk_pass"
batch_id = "TEST_20250101_120006"

source_data = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75),
    (3, "Charlie", 300, 3000.25),
    (4, "Diana", 400, 4000.00),
    (5, "Eve", 500, 5000.50)
]

source_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

source_df = spark.createDataFrame(source_data, source_schema)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

target_df = source_df.withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"✅ Created {table_name} - PK will PASS (no duplicates)")

# COMMAND ----------

# DBTITLE 1,Group 1 - Basic: Table 8 - PK FAIL (Duplicates in Source)
table_name = "group1_pk_fail_source_duplicates"
batch_id = "TEST_20250101_120007"

# Source has duplicate IDs
source_data = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75),
    (2, "BobDuplicate", 250, 2050.00),  # Duplicate ID!
    (3, "Charlie", 300, 3000.25),
    (4, "Diana", 400, 4000.00)
]

source_df = spark.createDataFrame(source_data, source_schema)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

# Target has unique IDs (duplicate removed)
target_data = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75),
    (3, "Charlie", 300, 3000.25),
    (4, "Diana", 400, 4000.00)
]
target_df = spark.createDataFrame(target_data, source_schema).withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"❌ Created {table_name} - PK will FAIL (source has duplicate ID=2)")

# COMMAND ----------

# DBTITLE 1,Group 1 - Basic: Table 9 - PK FAIL (Duplicates in Target)
table_name = "group1_pk_fail_target_duplicates"
batch_id = "TEST_20250101_120008"

# Source has unique IDs
source_data = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75),
    (3, "Charlie", 300, 3000.25),
    (4, "Diana", 400, 4000.00)
]

source_df = spark.createDataFrame(source_data, source_schema)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

# Target has duplicate IDs
target_data = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75),
    (2, "BobDuplicate", 250, 2050.00),  # Duplicate ID!
    (3, "Charlie", 300, 3000.25),
    (4, "Diana", 400, 4000.00)
]
target_df = spark.createDataFrame(target_data, source_schema).withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"❌ Created {table_name} - PK will FAIL (target has duplicate ID=2)")

# COMMAND ----------

# DBTITLE 1,Group 1 - Basic: Table 10 - All Checks PASS
table_name = "group1_all_pass"
batch_id = "TEST_20250101_120009"

source_data = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75),
    (3, "Charlie", 300, 3000.25),
    (4, "Diana", 400, 4000.00),
    (5, "Eve", 500, 5000.50)
]

source_df = spark.createDataFrame(source_data, source_schema)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

target_df = source_df.withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"✅ Created {table_name} - All checks will PASS (perfect match)")

# COMMAND ----------

# DBTITLE 1,Group 2 - Advanced: Table 1 - Append Mode (Multiple Batches)
table_name = "group2_append_multiple_batches"
batch_id_1 = "TEST_20250101_130000"
batch_id_2 = "TEST_20250101_140000"
batch_id_3 = "TEST_20250101_150000"

# Batch 1
source_data_1 = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75)
]
source_df_1 = spark.createDataFrame(source_data_1, source_schema)
orc_path_1 = f"{orc_base_path}/{table_name}/{batch_id_1}/data.orc"
source_df_1.write.mode("overwrite").format("orc").save(orc_path_1)

# Batch 2
source_data_2 = [
    (3, "Charlie", 300, 3000.25),
    (4, "Diana", 400, 4000.00)
]
source_df_2 = spark.createDataFrame(source_data_2, source_schema)
orc_path_2 = f"{orc_base_path}/{table_name}/{batch_id_2}/data.orc"
source_df_2.write.mode("overwrite").format("orc").save(orc_path_2)

# Batch 3
source_data_3 = [
    (5, "Eve", 500, 5000.50)
]
source_df_3 = spark.createDataFrame(source_data_3, source_schema)
orc_path_3 = f"{orc_base_path}/{table_name}/{batch_id_3}/data.orc"
source_df_3.write.mode("overwrite").format("orc").save(orc_path_3)

# Target has all batches (append mode)
target_data_all = source_data_1 + source_data_2 + source_data_3
target_df_all = spark.createDataFrame(target_data_all, source_schema)

# Add batch IDs
target_df_all = target_df_all.withColumn("_aud_batch_load_id", 
    when(col("id") <= 2, lit(batch_id_1))
    .when(col("id") <= 4, lit(batch_id_2))
    .otherwise(lit(batch_id_3))
)

target_df_all.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"✅ Created {table_name} - Append mode with 3 batches (all will be validated)")

# COMMAND ----------

# DBTITLE 1,Group 2 - Advanced: Table 2 - Overwrite Mode (Latest Batch Only)
table_name = "group2_overwrite_latest_only"
batch_id_1 = "TEST_20250101_130000"
batch_id_2 = "TEST_20250101_140000"
batch_id_3 = "TEST_20250101_150000"  # Latest

# Batch 1
source_data_1 = [
    (1, "Old1", 100, 1000.50),
    (2, "Old2", 200, 2000.75)
]
source_df_1 = spark.createDataFrame(source_data_1, source_schema)
orc_path_1 = f"{orc_base_path}/{table_name}/{batch_id_1}/data.orc"
source_df_1.write.mode("overwrite").format("orc").save(orc_path_1)

# Batch 2
source_data_2 = [
    (3, "Old3", 300, 3000.25),
    (4, "Old4", 400, 4000.00)
]
source_df_2 = spark.createDataFrame(source_data_2, source_schema)
orc_path_2 = f"{orc_base_path}/{table_name}/{batch_id_2}/data.orc"
source_df_2.write.mode("overwrite").format("orc").save(orc_path_2)

# Batch 3 (Latest - overwrite mode should only validate this)
source_data_3 = [
    (1, "New1", 150, 1500.00),
    (2, "New2", 250, 2500.00),
    (3, "New3", 350, 3500.00)
]
source_df_3 = spark.createDataFrame(source_data_3, source_schema)
orc_path_3 = f"{orc_base_path}/{table_name}/{batch_id_3}/data.orc"
source_df_3.write.mode("overwrite").format("orc").save(orc_path_3)

# Target has only latest batch (overwrite mode)
target_df = spark.createDataFrame(source_data_3, source_schema).withColumn("_aud_batch_load_id", lit(batch_id_3))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"✅ Created {table_name} - Overwrite mode (only latest batch will be validated)")

# COMMAND ----------

# DBTITLE 1,Group 2 - Advanced: Table 3 - Composite Primary Key
table_name = "group2_composite_pk"

source_data = [
    (1, "A", "Alice", 100, 1000.50),
    (1, "B", "Bob", 200, 2000.75),  # Same id but different code = unique
    (2, "A", "Charlie", 300, 3000.25),
    (2, "B", "Diana", 400, 4000.00)
]

source_schema_composite = StructType([
    StructField("id", IntegerType(), False),
    StructField("code", StringType(), False),
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

source_df = spark.createDataFrame(source_data, source_schema_composite)
batch_id = "TEST_20250101_160000"
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

target_df = source_df.withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"✅ Created {table_name} - Composite PK (id, code) - all unique")

# COMMAND ----------

# DBTITLE 1,Group 2 - Advanced: Table 4 - Composite PK with Duplicates
table_name = "group2_composite_pk_fail"

source_data = [
    (1, "A", "Alice", 100, 1000.50),
    (1, "A", "AliceDuplicate", 150, 1050.00),  # Duplicate composite key!
    (2, "B", "Bob", 200, 2000.75)
]

source_df = spark.createDataFrame(source_data, source_schema_composite)
batch_id = "TEST_20250101_160001"
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

# Target has unique keys (duplicate removed)
target_data = [
    (1, "A", "Alice", 100, 1000.50),
    (2, "B", "Bob", 200, 2000.75)
]
target_df = spark.createDataFrame(target_data, source_schema_composite).withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"❌ Created {table_name} - Composite PK FAIL (source has duplicate (1, 'A'))")

# COMMAND ----------

# DBTITLE 1,Group 2 - Advanced: Table 5 - Partitioned Table
table_name = "group2_partitioned"

source_data = [
    (1, "Alice", 100, 1000.50, "2024-01-01"),
    (2, "Bob", 200, 2000.75, "2024-01-01"),
    (3, "Charlie", 300, 3000.25, "2024-01-02"),
    (4, "Diana", 400, 4000.00, "2024-01-02"),
    (5, "Eve", 500, 5000.50, "2024-01-03")
]

source_schema_partitioned = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("date_partition", StringType(), True)
])

source_df = spark.createDataFrame(source_data, source_schema_partitioned)
batch_id = "TEST_20250101_170000"

# Save with partition structure in path
orc_path = f"{orc_base_path}/{table_name}/date_partition=2024-01-01/{batch_id}/data.orc"
source_df.filter(col("date_partition") == "2024-01-01").drop("date_partition").write.mode("overwrite").format("orc").save(orc_path)

orc_path_2 = f"{orc_base_path}/{table_name}/date_partition=2024-01-02/{batch_id}/data.orc"
source_df.filter(col("date_partition") == "2024-01-02").drop("date_partition").write.mode("overwrite").format("orc").save(orc_path_2)

orc_path_3 = f"{orc_base_path}/{table_name}/date_partition=2024-01-03/{batch_id}/data.orc"
source_df.filter(col("date_partition") == "2024-01-03").drop("date_partition").write.mode("overwrite").format("orc").save(orc_path_3)

# Target Delta table (partitioned)
target_df = source_df.withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").partitionBy("date_partition").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"✅ Created {table_name} - Partitioned table (date_partition)")

# COMMAND ----------

# DBTITLE 1,Group 2 - Advanced: Table 6 - No Primary Key (PK Check Skipped)
table_name = "group2_no_pk"

source_data = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75),
    (3, "Charlie", 300, 3000.25)
]

source_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

source_df = spark.createDataFrame(source_data, source_schema)
batch_id = "TEST_20250101_180000"
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

target_df = source_df.withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"✅ Created {table_name} - No PK defined (PK check will be SKIPPED)")

# COMMAND ----------

# DBTITLE 1,Populate Ingestion Config Table
# Clear existing data
spark.sql(f"TRUNCATE TABLE {constants.INGESTION_CONFIG_TABLE}")

# Group 1 - Basic tables
config_data_group1 = [
    ("group1_basic", "ts42_demo", "taxi_example", "group1_rowcount_pass", "source_system", "group1_rowcount_pass", "append", "id", None, "Y"),
    ("group1_basic", "ts42_demo", "taxi_example", "group1_rowcount_fail_source_more", "source_system", "group1_rowcount_fail_source_more", "append", "id", None, "Y"),
    ("group1_basic", "ts42_demo", "taxi_example", "group1_rowcount_fail_target_more", "source_system", "group1_rowcount_fail_target_more", "append", "id", None, "Y"),
    ("group1_basic", "ts42_demo", "taxi_example", "group1_schema_pass", "source_system", "group1_schema_pass", "append", "id", None, "Y"),
    ("group1_basic", "ts42_demo", "taxi_example", "group1_schema_fail_missing_col", "source_system", "group1_schema_fail_missing_col", "append", "id", None, "Y"),
    ("group1_basic", "ts42_demo", "taxi_example", "group1_schema_fail_type_mismatch", "source_system", "group1_schema_fail_type_mismatch", "append", "id", None, "Y"),
    ("group1_basic", "ts42_demo", "taxi_example", "group1_pk_pass", "source_system", "group1_pk_pass", "append", "id", None, "Y"),
    ("group1_basic", "ts42_demo", "taxi_example", "group1_pk_fail_source_duplicates", "source_system", "group1_pk_fail_source_duplicates", "append", "id", None, "Y"),
    ("group1_basic", "ts42_demo", "taxi_example", "group1_pk_fail_target_duplicates", "source_system", "group1_pk_fail_target_duplicates", "append", "id", None, "Y"),
    ("group1_basic", "ts42_demo", "taxi_example", "group1_all_pass", "source_system", "group1_all_pass", "append", "id", None, "Y"),
]

# Group 2 - Advanced tables
config_data_group2 = [
    ("group2_advanced", "ts42_demo", "taxi_example", "group2_append_multiple_batches", "source_system", "group2_append_multiple_batches", "append", "id", None, "Y"),
    ("group2_advanced", "ts42_demo", "taxi_example", "group2_overwrite_latest_only", "source_system", "group2_overwrite_latest_only", "overwrite", "id", None, "Y"),
    ("group2_advanced", "ts42_demo", "taxi_example", "group2_composite_pk", "source_system", "group2_composite_pk", "append", "id|code", None, "Y"),
    ("group2_advanced", "ts42_demo", "taxi_example", "group2_composite_pk_fail", "source_system", "group2_composite_pk_fail", "append", "id|code", None, "Y"),
    ("group2_advanced", "ts42_demo", "taxi_example", "group2_partitioned", "source_system", "group2_partitioned", "append", "id", "date_partition", "Y"),
    ("group2_advanced", "ts42_demo", "taxi_example", "group2_no_pk", "source_system", "group2_no_pk", "append", None, None, "Y"),
]

config_schema = StructType([
    StructField("group_name", StringType(), False),
    StructField("target_catalog", StringType(), False),
    StructField("target_schema", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("source_schema", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("write_mode", StringType(), False),
    StructField("primary_key", StringType(), True),
    StructField("partition_column", StringType(), True),
    StructField("is_active", StringType(), False)
])

all_config_data = config_data_group1 + config_data_group2
config_df = spark.createDataFrame(all_config_data, config_schema)
config_df.write.mode("overwrite").format("delta").saveAsTable(constants.INGESTION_CONFIG_TABLE)

print(f"✅ Populated {constants.INGESTION_CONFIG_TABLE} with {len(all_config_data)} records")

# COMMAND ----------

# DBTITLE 1,Populate Ingestion Audit Table
# Clear existing data
spark.sql(f"TRUNCATE TABLE {constants.INGESTION_AUDIT_TABLE}")

base_time = datetime(2025, 1, 1, 12, 0, 0)

# Group 1 tables - single batch each
audit_data_group1 = []
for i, table in enumerate([
    "group1_rowcount_pass", "group1_rowcount_fail_source_more", "group1_rowcount_fail_target_more",
    "group1_schema_pass", "group1_schema_fail_missing_col", "group1_schema_fail_type_mismatch",
    "group1_pk_pass", "group1_pk_fail_source_duplicates", "group1_pk_fail_target_duplicates",
    "group1_all_pass"
]):
    batch_id = f"TEST_20250101_{120000 + i:06d}"
    audit_data_group1.append((
        batch_id,
        f"ts42_demo.taxi_example.{table}",
        "COMPLETED",
        "group1_basic",
        base_time + timedelta(minutes=i),
        base_time + timedelta(minutes=i+1)
    ))

# Group 2 tables
audit_data_group2 = [
    # Append mode - 3 batches
    ("TEST_20250101_130000", "ts42_demo.taxi_example.group2_append_multiple_batches", "COMPLETED", "group2_advanced", base_time + timedelta(hours=1), base_time + timedelta(hours=1, minutes=1)),
    ("TEST_20250101_140000", "ts42_demo.taxi_example.group2_append_multiple_batches", "COMPLETED", "group2_advanced", base_time + timedelta(hours=2), base_time + timedelta(hours=2, minutes=1)),
    ("TEST_20250101_150000", "ts42_demo.taxi_example.group2_append_multiple_batches", "COMPLETED", "group2_advanced", base_time + timedelta(hours=3), base_time + timedelta(hours=3, minutes=1)),
    
    # Overwrite mode - 3 batches (latest will be validated)
    ("TEST_20250101_130000", "ts42_demo.taxi_example.group2_overwrite_latest_only", "COMPLETED", "group2_advanced", base_time + timedelta(hours=1), base_time + timedelta(hours=1, minutes=1)),
    ("TEST_20250101_140000", "ts42_demo.taxi_example.group2_overwrite_latest_only", "COMPLETED", "group2_advanced", base_time + timedelta(hours=2), base_time + timedelta(hours=2, minutes=1)),
    ("TEST_20250101_150000", "ts42_demo.taxi_example.group2_overwrite_latest_only", "COMPLETED", "group2_advanced", base_time + timedelta(hours=3), base_time + timedelta(hours=3, minutes=1)),
    
    # Other group2 tables
    ("TEST_20250101_160000", "ts42_demo.taxi_example.group2_composite_pk", "COMPLETED", "group2_advanced", base_time + timedelta(hours=4), base_time + timedelta(hours=4, minutes=1)),
    ("TEST_20250101_160001", "ts42_demo.taxi_example.group2_composite_pk_fail", "COMPLETED", "group2_advanced", base_time + timedelta(hours=4, minutes=1), base_time + timedelta(hours=4, minutes=2)),
    ("TEST_20250101_170000", "ts42_demo.taxi_example.group2_partitioned", "COMPLETED", "group2_advanced", base_time + timedelta(hours=5), base_time + timedelta(hours=5, minutes=1)),
    ("TEST_20250101_180000", "ts42_demo.taxi_example.group2_no_pk", "COMPLETED", "group2_advanced", base_time + timedelta(hours=6), base_time + timedelta(hours=6, minutes=1)),
]

audit_schema = StructType([
    StructField("batch_load_id", StringType(), False),
    StructField("target_table_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("group_name", StringType(), False),
    StructField("load_start_time", TimestampType(), False),
    StructField("load_end_time", TimestampType(), False)
])

all_audit_data = audit_data_group1 + audit_data_group2
audit_df = spark.createDataFrame(all_audit_data, audit_schema)
audit_df.write.mode("overwrite").format("delta").saveAsTable(constants.INGESTION_AUDIT_TABLE)

print(f"✅ Populated {constants.INGESTION_AUDIT_TABLE} with {len(all_audit_data)} records")

# COMMAND ----------

# DBTITLE 1,Populate Ingestion Metadata Table
# Clear existing data
spark.sql(f"TRUNCATE TABLE {constants.INGESTION_METADATA_TABLE}")

metadata_data = []

# Group 1 tables - single ORC file each
for i, table in enumerate([
    "group1_rowcount_pass", "group1_rowcount_fail_source_more", "group1_rowcount_fail_target_more",
    "group1_schema_pass", "group1_schema_fail_missing_col", "group1_schema_fail_type_mismatch",
    "group1_pk_pass", "group1_pk_fail_source_duplicates", "group1_pk_fail_target_duplicates",
    "group1_all_pass"
]):
    batch_id = f"TEST_20250101_{120000 + i:06d}"
    orc_path = f"{orc_base_path}/{table}/{batch_id}/data.orc"
    metadata_data.append((
        f"ts42_demo.taxi_example.{table}",
        batch_id,
        f"dbfs:{orc_path}",
        1024,  # file_size
        5  # record_count (approximate)
    ))

# Group 2 tables
# Append mode - 3 files
for batch_id in ["TEST_20250101_130000", "TEST_20250101_140000", "TEST_20250101_150000"]:
    orc_path = f"{orc_base_path}/group2_append_multiple_batches/{batch_id}/data.orc"
    metadata_data.append((
        "ts42_demo.taxi_example.group2_append_multiple_batches",
        batch_id,
        f"dbfs:{orc_path}",
        1024,
        2
    ))

# Overwrite mode - 3 files
for batch_id in ["TEST_20250101_130000", "TEST_20250101_140000", "TEST_20250101_150000"]:
    orc_path = f"{orc_base_path}/group2_overwrite_latest_only/{batch_id}/data.orc"
    metadata_data.append((
        "ts42_demo.taxi_example.group2_overwrite_latest_only",
        batch_id,
        f"dbfs:{orc_path}",
        1024,
        2
    ))

# Other group2 tables
for table, batch_id in [
    ("group2_composite_pk", "TEST_20250101_160000"),
    ("group2_composite_pk_fail", "TEST_20250101_160001"),
    ("group2_no_pk", "TEST_20250101_180000")
]:
    orc_path = f"{orc_base_path}/{table}/{batch_id}/data.orc"
    metadata_data.append((
        f"ts42_demo.taxi_example.{table}",
        batch_id,
        f"dbfs:{orc_path}",
        1024,
        4
    ))

# Partitioned table - 3 files (one per partition)
for partition in ["2024-01-01", "2024-01-02", "2024-01-03"]:
    batch_id = "TEST_20250101_170000"
    orc_path = f"{orc_base_path}/group2_partitioned/date_partition={partition}/{batch_id}/data.orc"
    metadata_data.append((
        "ts42_demo.taxi_example.group2_partitioned",
        batch_id,
        f"dbfs:{orc_path}",
        1024,
        2
    ))

metadata_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("batch_load_id", StringType(), False),
    StructField("source_file_path", StringType(), False),
    StructField("file_size", LongType(), False),
    StructField("record_count", LongType(), False)
])

metadata_df = spark.createDataFrame(metadata_data, metadata_schema)
metadata_df.write.mode("overwrite").format("delta").saveAsTable(constants.INGESTION_METADATA_TABLE)

print(f"✅ Populated {constants.INGESTION_METADATA_TABLE} with {len(metadata_data)} records")

# COMMAND ----------

# DBTITLE 1,Populate Partition Mapping Table
# Clear existing data
spark.sql(f"TRUNCATE TABLE {constants.INGESTION_SRC_TABLE_PARTITION_MAPPING}")

partition_mapping_data = [
    ("taxi_example", "group2_partitioned", "date_partition", "string", 0)
]

partition_mapping_schema = StructType([
    StructField("schema_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("partition_column_name", StringType(), False),
    StructField("datatype", StringType(), False),
    StructField("index", IntegerType(), False)
])

partition_mapping_df = spark.createDataFrame(partition_mapping_data, partition_mapping_schema)
partition_mapping_df.write.mode("overwrite").format("delta").saveAsTable(constants.INGESTION_SRC_TABLE_PARTITION_MAPPING)

print(f"✅ Populated {constants.INGESTION_SRC_TABLE_PARTITION_MAPPING} with {len(partition_mapping_data)} records")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 80)
print("TEST DATA SETUP COMPLETE")
print("=" * 80)

print("\n📊 Table Groups Created:")
print("\n1. group1_basic (10 tables):")
print("   ✅ group1_rowcount_pass - Row count PASS")
print("   ❌ group1_rowcount_fail_source_more - Row count FAIL (source has more)")
print("   ❌ group1_rowcount_fail_target_more - Row count FAIL (target has more)")
print("   ✅ group1_schema_pass - Schema PASS")
print("   ❌ group1_schema_fail_missing_col - Schema FAIL (missing column)")
print("   ❌ group1_schema_fail_type_mismatch - Schema FAIL (type mismatch)")
print("   ✅ group1_pk_pass - PK PASS")
print("   ❌ group1_pk_fail_source_duplicates - PK FAIL (duplicates in source)")
print("   ❌ group1_pk_fail_target_duplicates - PK FAIL (duplicates in target)")
print("   ✅ group1_all_pass - All checks PASS")

print("\n2. group2_advanced (6 tables):")
print("   ✅ group2_append_multiple_batches - Append mode (3 batches)")
print("   ✅ group2_overwrite_latest_only - Overwrite mode (only latest validated)")
print("   ✅ group2_composite_pk - Composite primary key (id, code)")
print("   ❌ group2_composite_pk_fail - Composite PK FAIL (duplicates)")
print("   ✅ group2_partitioned - Partitioned table")
print("   ✅ group2_no_pk - No PK defined (PK check skipped)")

print("\n📁 Metadata Tables Populated:")
print(f"   ✅ {constants.INGESTION_CONFIG_TABLE}")
print(f"   ✅ {constants.INGESTION_AUDIT_TABLE}")
print(f"   ✅ {constants.INGESTION_METADATA_TABLE}")
print(f"   ✅ {constants.INGESTION_SRC_TABLE_PARTITION_MAPPING}")

print("\n📂 ORC Files Location:")
print(f"   {orc_base_path}")

print("\n✅ Next Steps:")
print("   1. Run 02_setup_validation_mapping.py to sync validation_mapping")
print("   2. Run validation framework with:")
print("      - table_group='group1_basic'")
print("      - table_group='group2_advanced'")
print("   3. Review validation results in validation_log and validation_summary tables")

print("\n" + "=" * 80)

