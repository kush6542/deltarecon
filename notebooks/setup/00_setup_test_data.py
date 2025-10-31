# Databricks notebook source
"""
Setup Test Data for DeltaRecon Validation Framework

This notebook creates:
1. Source ORC files in DBFS (new location: dbfs:/FileStore/deltarecon_test_data/)
2. Target Delta tables with test data
3. Ingestion metadata tables (config, audit, metadata, partition mapping) - uses INSERT
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
import hashlib

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

# DBTITLE 1,Setup: Create Target Catalog and Schema
target_catalog = "ts42_demo"
target_schema = "taxi_example"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
spark.sql(f"USE CATALOG {target_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
spark.sql(f"USE SCHEMA {target_schema}")

print(f"Target catalog/schema ready: {target_catalog}.{target_schema}")

# COMMAND ----------

# DBTITLE 1,Setup: Base Paths for ORC Files (NEW LOCATION)
# Base path for storing ORC files in DBFS (NEW LOCATION)
dbfs_base_path = "/FileStore/deltarecon_test_data"
orc_base_path = f"{dbfs_base_path}/orc_files"

print(f"ORC files will be stored at: dbfs:{orc_base_path}")

# COMMAND ----------

# DBTITLE 1,Helper Function: Generate config_id
def generate_config_id(prefix: str, table_name: str) -> str:
    """Generate a UUID-like config_id"""
    hash_input = f"{prefix}_{table_name}_{datetime.now().timestamp()}"
    hash_obj = hashlib.md5(hash_input.encode())
    return hash_obj.hexdigest()

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

source_schema_with_date = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("date_col", StringType(), True)
])

source_df = spark.createDataFrame(source_data, source_schema_with_date)
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

source_df = spark.createDataFrame(source_data, source_schema_with_date)
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
    (3, "Charlie", 300, 3000.25)
]

source_df = spark.createDataFrame(source_data, source_schema)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

target_df = source_df.withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"✅ Created {table_name} - PK will PASS (no duplicates)")

# COMMAND ----------

# DBTITLE 1,Group 1 - Basic: Table 8 - PK FAIL (Source has duplicates)
table_name = "group1_pk_fail_source_duplicates"
batch_id = "TEST_20250101_120007"

# Source has duplicate ID
source_data = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75),
    (1, "Charlie", 300, 3000.25)  # Duplicate ID!
]

source_df = spark.createDataFrame(source_data, source_schema)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

# Target has no duplicates
target_data = [(1, "Alice", 100, 1000.50), (2, "Bob", 200, 2000.75)]
target_df = spark.createDataFrame(target_data, source_schema).withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"❌ Created {table_name} - PK will FAIL (source has duplicate ID=1)")

# COMMAND ----------

# DBTITLE 1,Group 1 - Basic: Table 9 - PK FAIL (Target has duplicates)
table_name = "group1_pk_fail_target_duplicates"
batch_id = "TEST_20250101_120008"

source_data = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75)
]

source_df = spark.createDataFrame(source_data, source_schema)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

# Target has duplicate ID
target_data = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75),
    (1, "Charlie", 300, 3000.25)  # Duplicate ID!
]
target_df = spark.createDataFrame(target_data, source_schema).withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"❌ Created {table_name} - PK will FAIL (target has duplicate ID=1)")

# COMMAND ----------

# DBTITLE 1,Group 1 - Basic: Table 10 - ALL PASS (Perfect Match)
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

print(f"✅ Created {table_name} - All validations will PASS")

# COMMAND ----------

# DBTITLE 1,Group 2 - Advanced: Table 1 - Append Mode with Multiple Batches
table_name = "group2_append_multiple_batches"
batch_id_1 = "TEST_20250101_130000"
batch_id_2 = "TEST_20250101_140000"
batch_id_3 = "TEST_20250101_150000"

# Batch 1
source_data_1 = [(1, "A", 100, 1000.0), (2, "B", 200, 2000.0)]
source_df_1 = spark.createDataFrame(source_data_1, source_schema)
orc_path_1 = f"{orc_base_path}/{table_name}/{batch_id_1}/data.orc"
source_df_1.write.mode("overwrite").format("orc").save(orc_path_1)
target_df_1 = source_df_1.withColumn("_aud_batch_load_id", lit(batch_id_1))
target_df_1.write.mode("append").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

# Batch 2
source_data_2 = [(3, "C", 300, 3000.0), (4, "D", 400, 4000.0)]
source_df_2 = spark.createDataFrame(source_data_2, source_schema)
orc_path_2 = f"{orc_base_path}/{table_name}/{batch_id_2}/data.orc"
source_df_2.write.mode("overwrite").format("orc").save(orc_path_2)
target_df_2 = source_df_2.withColumn("_aud_batch_load_id", lit(batch_id_2))
target_df_2.write.mode("append").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

# Batch 3
source_data_3 = [(5, "E", 500, 5000.0)]
source_df_3 = spark.createDataFrame(source_data_3, source_schema)
orc_path_3 = f"{orc_base_path}/{table_name}/{batch_id_3}/data.orc"
source_df_3.write.mode("overwrite").format("orc").save(orc_path_3)
target_df_3 = source_df_3.withColumn("_aud_batch_load_id", lit(batch_id_3))
target_df_3.write.mode("append").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"✅ Created {table_name} - Append mode with 3 batches")

# COMMAND ----------

# DBTITLE 1,Group 2 - Advanced: Table 2 - Overwrite Mode (Latest Only)
table_name = "group2_overwrite_latest_only"
batch_id_1 = "TEST_20250101_130000"
batch_id_2 = "TEST_20250101_140000"
batch_id_3 = "TEST_20250101_150000"

# Batch 1 (will be overwritten)
source_data_1 = [(1, "A", 100, 1000.0), (2, "B", 200, 2000.0)]
source_df_1 = spark.createDataFrame(source_data_1, source_schema)
orc_path_1 = f"{orc_base_path}/{table_name}/{batch_id_1}/data.orc"
source_df_1.write.mode("overwrite").format("orc").save(orc_path_1)
target_df_1 = source_df_1.withColumn("_aud_batch_load_id", lit(batch_id_1))
target_df_1.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

# Batch 2 (will be overwritten)
source_data_2 = [(3, "C", 300, 3000.0)]
source_df_2 = spark.createDataFrame(source_data_2, source_schema)
orc_path_2 = f"{orc_base_path}/{table_name}/{batch_id_2}/data.orc"
source_df_2.write.mode("overwrite").format("orc").save(orc_path_2)
target_df_2 = source_df_2.withColumn("_aud_batch_load_id", lit(batch_id_2))
target_df_2.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

# Batch 3 (latest, will be validated)
source_data_3 = [(10, "Z", 1000, 10000.0)]
source_df_3 = spark.createDataFrame(source_data_3, source_schema)
orc_path_3 = f"{orc_base_path}/{table_name}/{batch_id_3}/data.orc"
source_df_3.write.mode("overwrite").format("orc").save(orc_path_3)
target_df_3 = source_df_3.withColumn("_aud_batch_load_id", lit(batch_id_3))
target_df_3.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"✅ Created {table_name} - Overwrite mode (latest batch only)")

# COMMAND ----------

# DBTITLE 1,Group 2 - Advanced: Table 3 - Composite Primary Key (PASS)
table_name = "group2_composite_pk"
batch_id = "TEST_20250101_160000"

source_schema_composite = StructType([
    StructField("id", IntegerType(), False),
    StructField("code", StringType(), False),
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

source_data = [
    (1, "A", "Alice", 100, 1000.50),
    (1, "B", "Bob", 200, 2000.75),
    (2, "A", "Charlie", 300, 3000.25)
]

source_df = spark.createDataFrame(source_data, source_schema_composite)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

target_df = source_df.withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"✅ Created {table_name} - Composite PK (id|code) will PASS")

# COMMAND ----------

# DBTITLE 1,Group 2 - Advanced: Table 4 - Composite Primary Key (FAIL)
table_name = "group2_composite_pk_fail"
batch_id = "TEST_20250101_160001"

source_data = [
    (1, "A", "Alice", 100, 1000.50),
    (1, "B", "Bob", 200, 2000.75),
    (1, "A", "Charlie", 300, 3000.25)  # Duplicate composite key (1, A)!
]

source_df = spark.createDataFrame(source_data, source_schema_composite)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

# Target has no duplicates
target_data = source_data[:2]
target_df = spark.createDataFrame(target_data, source_schema_composite).withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"❌ Created {table_name} - Composite PK will FAIL (source has duplicate (1, A))")

# COMMAND ----------

# DBTITLE 1,Group 2 - Advanced: Table 5 - Partitioned Table
table_name = "group2_partitioned"
batch_id = "TEST_20250101_170000"

# Ensure we're in the correct catalog/schema context
spark.sql(f"USE CATALOG {target_catalog}")
spark.sql(f"USE SCHEMA {target_schema}")

# Drop table if it exists to ensure clean state
spark.sql(f"DROP TABLE IF EXISTS {target_catalog}.{target_schema}.{table_name}")

source_schema_partitioned = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("date_partition", DateType(), True)
])

source_data = [
    (1, "Alice", 100, 1000.50, datetime(2024, 1, 1).date()),
    (2, "Bob", 200, 2000.75, datetime(2024, 1, 1).date()),
    (3, "Charlie", 300, 3000.25, datetime(2024, 1, 2).date())
]

source_df = spark.createDataFrame(source_data, source_schema_partitioned)

# CRITICAL: Write ORC files with Hive-style partitioning
# This creates files like: .../date_partition=2024-01-01/part-00000.orc
orc_path_base = f"{orc_base_path}/{table_name}/{batch_id}"
source_df.write.mode("overwrite").format("orc").partitionBy("date_partition").save(orc_path_base)

print(f"✅ Created partitioned ORC files at: {orc_path_base}")

# List the created partition files for verification
partition_files = dbutils.fs.ls(orc_path_base.replace("dbfs:", "/dbfs"))
print(f"   Created {len(partition_files)} partition folders:")
for pf in partition_files:
    print(f"     - {pf.name}")

target_df = source_df.withColumn("_aud_batch_load_id", lit(batch_id))

# Create table with explicit DDL to ensure proper schema and partitioning
create_table_ddl = f"""
CREATE TABLE {target_catalog}.{target_schema}.{table_name} (
    id INT NOT NULL,
    name STRING,
    amount INT,
    price DOUBLE,
    date_partition DATE,
    _aud_batch_load_id STRING
)
USING DELTA
PARTITIONED BY (date_partition)
"""

spark.sql(create_table_ddl)

# Insert data into the table
target_df.write.mode("append").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"✅ Created {table_name} - Partitioned table (date_partition)")

# COMMAND ----------

# DBTITLE 1,Group 2 - Advanced: Table 6 - No Primary Key (PK Check Skipped)
table_name = "group2_no_pk"
batch_id = "TEST_20250101_180000"

source_data = [
    (1, "Alice", 100, 1000.50),
    (2, "Bob", 200, 2000.75),
    (3, "Charlie", 300, 3000.25)
]

source_df = spark.createDataFrame(source_data, source_schema)
orc_path = f"{orc_base_path}/{table_name}/{batch_id}/data.orc"
source_df.write.mode("overwrite").format("orc").save(orc_path)

target_df = source_df.withColumn("_aud_batch_load_id", lit(batch_id))
target_df.write.mode("overwrite").format("delta").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")

print(f"✅ Created {table_name} - No PK defined (PK check will be SKIPPED)")

# COMMAND ----------

# DBTITLE 1,Populate Ingestion Config Table (INSERT)
# Switch back to ingestion schema
spark.sql(f"USE CATALOG {constants.INGESTION_OPS_CATALOG}")
spark.sql(f"USE SCHEMA {constants.INGESTION_OPS_SCHEMA.split('.')[1]}")

base_time = datetime.now()

# Group 1 - Basic tables
config_data_group1 = [
    (
        generate_config_id("group1", "group1_rowcount_pass"),
        "group1_basic",
        "source_system",
        "group1_rowcount_pass",
        f"dbfs:{orc_base_path}/group1_rowcount_pass",
        "ts42_demo",
        "taxi_example",
        "group1_rowcount_pass",
        "orc",
        None,
        "incremental",
        "append",
        "id",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
    (
        generate_config_id("group1", "group1_rowcount_fail_source_more"),
        "group1_basic",
        "source_system",
        "group1_rowcount_fail_source_more",
        f"dbfs:{orc_base_path}/group1_rowcount_fail_source_more",
        "ts42_demo",
        "taxi_example",
        "group1_rowcount_fail_source_more",
        "orc",
        None,
        "incremental",
        "append",
        "id",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
    (
        generate_config_id("group1", "group1_rowcount_fail_target_more"),
        "group1_basic",
        "source_system",
        "group1_rowcount_fail_target_more",
        f"dbfs:{orc_base_path}/group1_rowcount_fail_target_more",
        "ts42_demo",
        "taxi_example",
        "group1_rowcount_fail_target_more",
        "orc",
        None,
        "incremental",
        "append",
        "id",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
    (
        generate_config_id("group1", "group1_schema_pass"),
        "group1_basic",
        "source_system",
        "group1_schema_pass",
        f"dbfs:{orc_base_path}/group1_schema_pass",
        "ts42_demo",
        "taxi_example",
        "group1_schema_pass",
        "orc",
        None,
        "incremental",
        "append",
        "id",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
    (
        generate_config_id("group1", "group1_schema_fail_missing_col"),
        "group1_basic",
        "source_system",
        "group1_schema_fail_missing_col",
        f"dbfs:{orc_base_path}/group1_schema_fail_missing_col",
        "ts42_demo",
        "taxi_example",
        "group1_schema_fail_missing_col",
        "orc",
        None,
        "incremental",
        "append",
        "id",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
    (
        generate_config_id("group1", "group1_schema_fail_type_mismatch"),
        "group1_basic",
        "source_system",
        "group1_schema_fail_type_mismatch",
        f"dbfs:{orc_base_path}/group1_schema_fail_type_mismatch",
        "ts42_demo",
        "taxi_example",
        "group1_schema_fail_type_mismatch",
        "orc",
        None,
        "incremental",
        "append",
        "id",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
    (
        generate_config_id("group1", "group1_pk_pass"),
        "group1_basic",
        "source_system",
        "group1_pk_pass",
        f"dbfs:{orc_base_path}/group1_pk_pass",
        "ts42_demo",
        "taxi_example",
        "group1_pk_pass",
        "orc",
        None,
        "incremental",
        "append",
        "id",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
    (
        generate_config_id("group1", "group1_pk_fail_source_duplicates"),
        "group1_basic",
        "source_system",
        "group1_pk_fail_source_duplicates",
        f"dbfs:{orc_base_path}/group1_pk_fail_source_duplicates",
        "ts42_demo",
        "taxi_example",
        "group1_pk_fail_source_duplicates",
        "orc",
        None,
        "incremental",
        "append",
        "id",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
    (
        generate_config_id("group1", "group1_pk_fail_target_duplicates"),
        "group1_basic",
        "source_system",
        "group1_pk_fail_target_duplicates",
        f"dbfs:{orc_base_path}/group1_pk_fail_target_duplicates",
        "ts42_demo",
        "taxi_example",
        "group1_pk_fail_target_duplicates",
        "orc",
        None,
        "incremental",
        "append",
        "id",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
    (
        generate_config_id("group1", "group1_all_pass"),
        "group1_basic",
        "source_system",
        "group1_all_pass",
        f"dbfs:{orc_base_path}/group1_all_pass",
        "ts42_demo",
        "taxi_example",
        "group1_all_pass",
        "orc",
        None,
        "incremental",
        "append",
        "id",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
]

# Group 2 - Advanced tables
config_data_group2 = [
    (
        generate_config_id("group2", "group2_append_multiple_batches"),
        "group2_advanced",
        "source_system",
        "group2_append_multiple_batches",
        f"dbfs:{orc_base_path}/group2_append_multiple_batches",
        "ts42_demo",
        "taxi_example",
        "group2_append_multiple_batches",
        "orc",
        None,
        "incremental",
        "append",
        "id",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
    (
        generate_config_id("group2", "group2_overwrite_latest_only"),
        "group2_advanced",
        "source_system",
        "group2_overwrite_latest_only",
        f"dbfs:{orc_base_path}/group2_overwrite_latest_only",
        "ts42_demo",
        "taxi_example",
        "group2_overwrite_latest_only",
        "orc",
        None,
        "incremental",
        "overwrite",
        "id",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
    (
        generate_config_id("group2", "group2_composite_pk"),
        "group2_advanced",
        "source_system",
        "group2_composite_pk",
        f"dbfs:{orc_base_path}/group2_composite_pk",
        "ts42_demo",
        "taxi_example",
        "group2_composite_pk",
        "orc",
        None,
        "incremental",
        "append",
        "id|code",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
    (
        generate_config_id("group2", "group2_composite_pk_fail"),
        "group2_advanced",
        "source_system",
        "group2_composite_pk_fail",
        f"dbfs:{orc_base_path}/group2_composite_pk_fail",
        "ts42_demo",
        "taxi_example",
        "group2_composite_pk_fail",
        "orc",
        None,
        "incremental",
        "append",
        "id|code",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
    (
        generate_config_id("group2", "group2_partitioned"),
        "group2_advanced",
        "source_system",
        "group2_partitioned",
        f"dbfs:{orc_base_path}/group2_partitioned",
        "ts42_demo",
        "taxi_example",
        "group2_partitioned",
        "orc",
        None,
        "incremental",
        "append",
        "id",
        "date_partition",
        "PARTITIONED_BY",
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
    (
        generate_config_id("group2", "group2_no_pk"),
        "group2_advanced",
        "source_system",
        "group2_no_pk",
        f"dbfs:{orc_base_path}/group2_no_pk",
        "ts42_demo",
        "taxi_example",
        "group2_no_pk",
        "orc",
        None,
        "incremental",
        "append",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Y",
        base_time,
        None,
        None,
        None,
        None
    ),
]

# Create DataFrame with all columns matching the actual schema
config_schema = StructType([
    StructField("config_id", StringType(), False),
    StructField("group_name", StringType(), True),
    StructField("source_schema", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("source_file_path", StringType(), True),
    StructField("target_catalog", StringType(), True),
    StructField("target_schema", StringType(), True),
    StructField("target_table", StringType(), True),
    StructField("source_file_format", StringType(), True),
    StructField("source_file_options", StringType(), True),
    StructField("load_type", StringType(), True),
    StructField("write_mode", StringType(), True),
    StructField("primary_key", StringType(), True),
    StructField("partition_column", StringType(), True),
    StructField("partitioning_strategy", StringType(), True),
    StructField("frequency", StringType(), True),
    StructField("table_size_gb", StringType(), True),
    StructField("column_datatype_mapping", StringType(), True),
    StructField("delta_properties", StringType(), True),
    StructField("clean_column_names", StringType(), True),
    StructField("deduplicate", StringType(), True),
    StructField("is_active", StringType(), True),
    StructField("insert_ts", TimestampType(), True),
    StructField("last_update_ts", TimestampType(), True),
    StructField("timestamp_column", StringType(), True),
    StructField("schedule", StringType(), True),
    StructField("job_tags", StringType(), True),
])

all_config_data = config_data_group1 + config_data_group2
config_df = spark.createDataFrame(all_config_data, config_schema)

# Use INSERT INTO instead of TRUNCATE + overwrite
config_df.write.mode("append").format("delta").saveAsTable(constants.INGESTION_CONFIG_TABLE)

print(f"✅ Inserted {len(all_config_data)} records into {constants.INGESTION_CONFIG_TABLE}")

# COMMAND ----------

# DBTITLE 1,Populate Ingestion Audit Table (INSERT)
# Generate run_id for each audit entry
def generate_run_id():
    return hashlib.md5(f"run_{datetime.now().timestamp()}_{random.random()}".encode()).hexdigest()

# Group 1 tables - single batch each
audit_data_group1 = []
for i, table in enumerate([
    "group1_rowcount_pass", "group1_rowcount_fail_source_more", "group1_rowcount_fail_target_more",
    "group1_schema_pass", "group1_schema_fail_missing_col", "group1_schema_fail_type_mismatch",
    "group1_pk_pass", "group1_pk_fail_source_duplicates", "group1_pk_fail_target_duplicates",
    "group1_all_pass"
]):
    batch_id = f"TEST_20250101_{120000 + i:06d}"
    config_id = config_data_group1[i][0]  # Get config_id from config_data
    audit_data_group1.append((
        generate_run_id(),
        config_id,
        batch_id,
        "group1_basic",
        f"ts42_demo.taxi_example.{table}",
        "incremental_batch_ingestion",
        "incremental",
        "COMPLETED",
        base_time + timedelta(minutes=i),
        base_time + timedelta(minutes=i+1),
        None,
        -1,
        -1
    ))

# Group 2 tables
audit_data_group2 = []
# Append mode - 3 batches
for i, batch_id in enumerate(["TEST_20250101_130000", "TEST_20250101_140000", "TEST_20250101_150000"]):
    config_id = config_data_group2[0][0]  # group2_append_multiple_batches
    audit_data_group2.append((
        generate_run_id(),
        config_id,
        batch_id,
        "group2_advanced",
        "ts42_demo.taxi_example.group2_append_multiple_batches",
        "incremental_batch_ingestion",
        "incremental",
        "COMPLETED",
        base_time + timedelta(hours=1+i),
        base_time + timedelta(hours=1+i, minutes=1),
        None,
        -1,
        -1
    ))

# Overwrite mode - 3 batches (latest will be validated)
for i, batch_id in enumerate(["TEST_20250101_130000", "TEST_20250101_140000", "TEST_20250101_150000"]):
    config_id = config_data_group2[1][0]  # group2_overwrite_latest_only
    audit_data_group2.append((
        generate_run_id(),
        config_id,
        batch_id,
        "group2_advanced",
        "ts42_demo.taxi_example.group2_overwrite_latest_only",
        "incremental_batch_ingestion",
        "incremental",
        "COMPLETED",
        base_time + timedelta(hours=1+i),
        base_time + timedelta(hours=1+i, minutes=1),
        None,
        -1,
        -1
    ))

# Other group2 tables
other_tables = [
    ("group2_composite_pk", "TEST_20250101_160000"),
    ("group2_composite_pk_fail", "TEST_20250101_160001"),
    ("group2_partitioned", "TEST_20250101_170000"),
    ("group2_no_pk", "TEST_20250101_180000")
]
for i, (table, batch_id) in enumerate(other_tables):
    config_id = config_data_group2[2+i][0]  # Get config_id from config_data
    audit_data_group2.append((
        generate_run_id(),
        config_id,
        batch_id,
        "group2_advanced",
        f"ts42_demo.taxi_example.{table}",
        "incremental_batch_ingestion",
        "incremental",
        "COMPLETED",
        base_time + timedelta(hours=4+i),
        base_time + timedelta(hours=4+i, minutes=1),
        None,
        -1,
        -1
    ))

audit_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("config_id", StringType(), True),
    StructField("batch_load_id", StringType(), True),
    StructField("group_name", StringType(), True),
    StructField("target_table_name", StringType(), True),
    StructField("operation_type", StringType(), True),
    StructField("load_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("start_ts", TimestampType(), True),
    StructField("end_ts", TimestampType(), True),
    StructField("log_message", StringType(), True),
    StructField("microbatch_id", IntegerType(), True),
    StructField("row_count", LongType(), True),
])

all_audit_data = audit_data_group1 + audit_data_group2
audit_df = spark.createDataFrame(all_audit_data, audit_schema)

# Use INSERT INTO instead of TRUNCATE + overwrite
audit_df.write.mode("append").format("delta").saveAsTable(constants.INGESTION_AUDIT_TABLE)

print(f"✅ Inserted {len(all_audit_data)} records into {constants.INGESTION_AUDIT_TABLE}")

# COMMAND ----------

# DBTITLE 1,Populate Ingestion Metadata Table (INSERT)
# This table tracks which ORC files belong to which batch
metadata_data = []

# Group 1 tables
for i, table in enumerate([
    "group1_rowcount_pass", "group1_rowcount_fail_source_more", "group1_rowcount_fail_target_more",
    "group1_schema_pass", "group1_schema_fail_missing_col", "group1_schema_fail_type_mismatch",
    "group1_pk_pass", "group1_pk_fail_source_duplicates", "group1_pk_fail_target_duplicates",
    "group1_all_pass"
]):
    batch_id = f"TEST_20250101_{120000 + i:06d}"
    full_table_name = f"ts42_demo.taxi_example.{table}"
    orc_file_path = f"dbfs:{orc_base_path}/{table}/{batch_id}/data.orc"
    
    # Get file size (approximate)
    try:
        file_size = dbutils.fs.ls(orc_file_path.replace("dbfs:", "/dbfs"))[0].size if len(dbutils.fs.ls(orc_file_path.replace("dbfs:", "/dbfs"))) > 0 else 1000
    except:
        file_size = 1000
    
    metadata_data.append((
        full_table_name,
        batch_id,
        orc_file_path,
        base_time + timedelta(minutes=i),
        file_size,
        "N",  # is_processed
        base_time + timedelta(minutes=i),
        base_time + timedelta(minutes=i)
    ))

# Group 2 tables
# Append mode - 3 batches
for i, batch_id in enumerate(["TEST_20250101_130000", "TEST_20250101_140000", "TEST_20250101_150000"]):
    table = "group2_append_multiple_batches"
    full_table_name = f"ts42_demo.taxi_example.{table}"
    orc_file_path = f"dbfs:{orc_base_path}/{table}/{batch_id}/data.orc"
    
    try:
        file_size = dbutils.fs.ls(orc_file_path.replace("dbfs:", "/dbfs"))[0].size if len(dbutils.fs.ls(orc_file_path.replace("dbfs:", "/dbfs"))) > 0 else 1000
    except:
        file_size = 1000
    
    metadata_data.append((
        full_table_name,
        batch_id,
        orc_file_path,
        base_time + timedelta(hours=1+i),
        file_size,
        "N",
        base_time + timedelta(hours=1+i),
        base_time + timedelta(hours=1+i)
    ))

# Overwrite mode - 3 batches
for i, batch_id in enumerate(["TEST_20250101_130000", "TEST_20250101_140000", "TEST_20250101_150000"]):
    table = "group2_overwrite_latest_only"
    full_table_name = f"ts42_demo.taxi_example.{table}"
    orc_file_path = f"dbfs:{orc_base_path}/{table}/{batch_id}/data.orc"
    
    try:
        file_size = dbutils.fs.ls(orc_file_path.replace("dbfs:", "/dbfs"))[0].size if len(dbutils.fs.ls(orc_file_path.replace("dbfs:", "/dbfs"))) > 0 else 1000
    except:
        file_size = 1000
    
    metadata_data.append((
        full_table_name,
        batch_id,
        orc_file_path,
        base_time + timedelta(hours=1+i),
        file_size,
        "N",
        base_time + timedelta(hours=1+i),
        base_time + timedelta(hours=1+i)
    ))

# Other group2 tables
other_tables = [
    ("group2_composite_pk", "TEST_20250101_160000", False),
    ("group2_composite_pk_fail", "TEST_20250101_160001", False),
    ("group2_partitioned", "TEST_20250101_170000", True),  # This is partitioned!
    ("group2_no_pk", "TEST_20250101_180000", False)
]
for i, (table, batch_id, is_partitioned) in enumerate(other_tables):
    full_table_name = f"ts42_demo.taxi_example.{table}"
    
    if is_partitioned:
        # For partitioned table, list all partition folders and their ORC files
        batch_base_path = f"{orc_base_path}/{table}/{batch_id}"
        try:
            # List partition folders (e.g., date_partition=2024-01-01/)
            partition_folders = dbutils.fs.ls(batch_base_path.replace("dbfs:", "/dbfs"))
            
            for partition_folder in partition_folders:
                if partition_folder.isDir():
                    # List ORC files within each partition
                    partition_files = dbutils.fs.ls(partition_folder.path.replace("dbfs:", "/dbfs"))
                    
                    for orc_file in partition_files:
                        if orc_file.name.endswith(".orc"):
                            orc_file_path = f"dbfs:{orc_file.path.replace('/dbfs/', '/')}"
                            
                            metadata_data.append((
                                full_table_name,
                                batch_id,
                                orc_file_path,
                                base_time + timedelta(hours=4+i),
                                orc_file.size,
                                "N",
                                base_time + timedelta(hours=4+i),
                                base_time + timedelta(hours=4+i)
                            ))
                            print(f"   Added metadata for: {orc_file_path}")
        except Exception as e:
            print(f"⚠️ Warning: Could not list partitioned files for {table}: {e}")
            # Fallback to generic path
            orc_file_path = f"dbfs:{batch_base_path}/data.orc"
            metadata_data.append((
                full_table_name,
                batch_id,
                orc_file_path,
                base_time + timedelta(hours=4+i),
                1000,
                "N",
                base_time + timedelta(hours=4+i),
                base_time + timedelta(hours=4+i)
            ))
    else:
        # Non-partitioned tables - single ORC file
        orc_file_path = f"dbfs:{orc_base_path}/{table}/{batch_id}/data.orc"
        
        try:
            file_size = dbutils.fs.ls(orc_file_path.replace("dbfs:", "/dbfs"))[0].size if len(dbutils.fs.ls(orc_file_path.replace("dbfs:", "/dbfs"))) > 0 else 1000
        except:
            file_size = 1000
        
        metadata_data.append((
            full_table_name,
            batch_id,
            orc_file_path,
            base_time + timedelta(hours=4+i),
            file_size,
            "N",
            base_time + timedelta(hours=4+i),
            base_time + timedelta(hours=4+i)
        ))

metadata_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("batch_load_id", StringType(), False),
    StructField("source_file_path", StringType(), True),
    StructField("file_modification_time", TimestampType(), True),
    StructField("file_size", LongType(), True),
    StructField("is_processed", StringType(), True),
    StructField("insert_ts", TimestampType(), True),
    StructField("last_update_ts", TimestampType(), True),
])

metadata_df = spark.createDataFrame(metadata_data, metadata_schema)

# Use INSERT INTO instead of TRUNCATE + overwrite
metadata_df.write.mode("append").format("delta").saveAsTable(constants.INGESTION_METADATA_TABLE)

print(f"✅ Inserted {len(metadata_data)} records into {constants.INGESTION_METADATA_TABLE}")

# COMMAND ----------

# DBTITLE 1,Populate Source Table Partition Mapping (INSERT)
# Only group2_partitioned has partitions
partition_mapping_data = [
    (
        "source_system",
        "group2_partitioned",
        "date_partition",
        "date",
        "DATE",
        0,
        base_time,
        base_time
    )
]

partition_mapping_schema = StructType([
    StructField("schema_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("partition_column_name", StringType(), True),
    StructField("datatype", StringType(), True),
    StructField("derived_datatype", StringType(), True),
    StructField("index", IntegerType(), True),
    StructField("insert_ts", TimestampType(), True),
    StructField("last_update_ts", TimestampType(), True),
])

partition_mapping_df = spark.createDataFrame(partition_mapping_data, partition_mapping_schema)

# Use INSERT INTO instead of TRUNCATE + overwrite
partition_mapping_df.write.mode("append").format("delta").saveAsTable(constants.INGESTION_SRC_TABLE_PARTITION_MAPPING)

print(f"✅ Inserted {len(partition_mapping_data)} records into {constants.INGESTION_SRC_TABLE_PARTITION_MAPPING}")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 80)
print("SETUP COMPLETE!")
print("=" * 80)
print(f"\n✅ Created {len(config_data_group1 + config_data_group2)} test tables")
print(f"✅ Created ORC files in: dbfs:{orc_base_path}")
print(f"✅ Inserted {len(all_config_data)} records into ingestion_config")
print(f"✅ Inserted {len(all_audit_data)} records into ingestion_audit")
print(f"✅ Inserted {len(metadata_data)} records into ingestion_metadata")
print(f"✅ Inserted {len(partition_mapping_data)} records into partition_mapping")
print("\nTable Groups:")
print(f"  - group1_basic: {len(config_data_group1)} tables")
print(f"  - group2_advanced: {len(config_data_group2)} tables")
print("\n" + "=" * 80)
