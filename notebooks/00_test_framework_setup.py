# Databricks notebook source
"""
Test Framework Setup - Core Functionality Testing

This notebook creates a simple test environment to validate the DeltaRecon framework:
- Creates 2 test tables (products, customers)
- Creates 2 batches for each table
- Sets up source ORC files in DBFS
- Populates all ingestion metadata tables
- Tests metrics collection

After running this notebook, you can:
1. Run 02_setup_validation_mapping.py to sync validation configs
2. Create and run validation jobs
"""

# COMMAND ----------

# DBTITLE 1,Setup and Imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, LongType
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime, timedelta
import random

# Import framework constants
from deltarecon.config import constants

# COMMAND ----------

# DBTITLE 1,Configuration
# Test configuration
TEST_GROUP = "test_group_1"
TEST_CATALOG = "ts42_demo"
TEST_SCHEMA = "test_validation"
SOURCE_SCHEMA = "source_system"
SOURCE_DATABASE = "raw_db"

# DBFS paths
DBFS_BASE_PATH = "dbfs:/FileStore/deltarecon_test"
PRODUCTS_PATH = f"{DBFS_BASE_PATH}/products"
CUSTOMERS_PATH = f"{DBFS_BASE_PATH}/customers"

# Batch IDs
BATCH_1 = f"BATCH_{datetime.now().strftime('%Y%m%d')}_000001"
BATCH_2 = f"BATCH_{datetime.now().strftime('%Y%m%d')}_000002"

print("=" * 80)
print("TEST CONFIGURATION")
print("=" * 80)
print(f"Test Group: {TEST_GROUP}")
print(f"Test Catalog: {TEST_CATALOG}")
print(f"Test Schema: {TEST_SCHEMA}")
print(f"Source: {SOURCE_SCHEMA}.{SOURCE_DATABASE}")
print(f"DBFS Base Path: {DBFS_BASE_PATH}")
print(f"Batch 1: {BATCH_1}")
print(f"Batch 2: {BATCH_2}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Create Test Schema
print("Creating test schema...")

# Create catalog if not exists
spark.sql(f"CREATE CATALOG IF NOT EXISTS {TEST_CATALOG}")

# Create test schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_CATALOG}.{TEST_SCHEMA}")

print(f"✓ Schema created: {TEST_CATALOG}.{TEST_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Define Test Data Schemas

# Products schema
products_schema = StructType([
    StructField('product_id', IntegerType(), False),
    StructField('product_name', StringType(), True),
    StructField('category', StringType(), True),
    StructField('price', DecimalType(10, 2), True),
    StructField('stock_quantity', IntegerType(), True),
    StructField('_aud_batch_load_id', StringType(), True)
])

# Customers schema
customers_schema = StructType([
    StructField('customer_id', IntegerType(), False),
    StructField('customer_name', StringType(), True),
    StructField('email', StringType(), True),
    StructField('country', StringType(), True),
    StructField('signup_date', StringType(), True),
    StructField('_aud_batch_load_id', StringType(), True)
])

print("✓ Schemas defined")

# COMMAND ----------

# DBTITLE 1,Generate Test Data - Products Batch 1
print(f"Generating products data for {BATCH_1}...")

products_batch1_data = [
    (1, 'Laptop Pro 15', 'Electronics', 1299.99, 50, BATCH_1),
    (2, 'Wireless Mouse', 'Electronics', 29.99, 200, BATCH_1),
    (3, 'Office Desk', 'Furniture', 349.99, 30, BATCH_1),
    (4, 'Coffee Maker', 'Appliances', 89.99, 75, BATCH_1),
    (5, 'Notebook Set', 'Stationery', 12.99, 150, BATCH_1),
]

products_batch1_df = spark.createDataFrame(products_batch1_data, products_schema)
print(f"✓ Created {products_batch1_df.count()} products for Batch 1")
display(products_batch1_df)

# COMMAND ----------

# DBTITLE 1,Generate Test Data - Products Batch 2
print(f"Generating products data for {BATCH_2}...")

products_batch2_data = [
    (6, 'USB-C Cable', 'Electronics', 19.99, 300, BATCH_2),
    (7, 'Standing Desk', 'Furniture', 499.99, 20, BATCH_2),
    (8, 'Water Bottle', 'Accessories', 24.99, 100, BATCH_2),
    (9, 'Monitor 27"', 'Electronics', 349.99, 40, BATCH_2),
    (10, 'Desk Lamp', 'Furniture', 45.99, 80, BATCH_2),
]

products_batch2_df = spark.createDataFrame(products_batch2_data, products_schema)
print(f"✓ Created {products_batch2_df.count()} products for Batch 2")
display(products_batch2_df)

# COMMAND ----------

# DBTITLE 1,Generate Test Data - Customers Batch 1
print(f"Generating customers data for {BATCH_1}...")

customers_batch1_data = [
    (101, 'Alice Johnson', 'alice.j@email.com', 'USA', '2024-01-15', BATCH_1),
    (102, 'Bob Smith', 'bob.smith@email.com', 'UK', '2024-01-16', BATCH_1),
    (103, 'Charlie Brown', 'charlie.b@email.com', 'Canada', '2024-01-17', BATCH_1),
    (104, 'Diana Prince', 'diana.p@email.com', 'USA', '2024-01-18', BATCH_1),
    (105, 'Eve Wilson', 'eve.w@email.com', 'Australia', '2024-01-19', BATCH_1),
]

customers_batch1_df = spark.createDataFrame(customers_batch1_data, customers_schema)
print(f"✓ Created {customers_batch1_df.count()} customers for Batch 1")
display(customers_batch1_df)

# COMMAND ----------

# DBTITLE 1,Generate Test Data - Customers Batch 2
print(f"Generating customers data for {BATCH_2}...")

customers_batch2_data = [
    (106, 'Frank Miller', 'frank.m@email.com', 'Germany', '2024-01-20', BATCH_2),
    (107, 'Grace Lee', 'grace.lee@email.com', 'Singapore', '2024-01-21', BATCH_2),
    (108, 'Henry Ford', 'henry.f@email.com', 'USA', '2024-01-22', BATCH_2),
    (109, 'Ivy Chen', 'ivy.chen@email.com', 'China', '2024-01-23', BATCH_2),
    (110, 'Jack Davis', 'jack.d@email.com', 'UK', '2024-01-24', BATCH_2),
]

customers_batch2_df = spark.createDataFrame(customers_batch2_data, customers_schema)
print(f"✓ Created {customers_batch2_df.count()} customers for Batch 2")
display(customers_batch2_df)

# COMMAND ----------

# DBTITLE 1,Write Source ORC Files - Products
print("Writing products ORC files to DBFS...")

# Write Batch 1
products_batch1_path = f"{PRODUCTS_PATH}/{BATCH_1}"
products_batch1_df.write.mode("overwrite").format("orc").save(products_batch1_path)
print(f"✓ Wrote products Batch 1 to: {products_batch1_path}")

# Write Batch 2
products_batch2_path = f"{PRODUCTS_PATH}/{BATCH_2}"
products_batch2_df.write.mode("overwrite").format("orc").save(products_batch2_path)
print(f"✓ Wrote products Batch 2 to: {products_batch2_path}")

# List files to verify
print("\nVerifying products files:")
dbutils.fs.ls(PRODUCTS_PATH)

# COMMAND ----------

# DBTITLE 1,Write Source ORC Files - Customers
print("Writing customers ORC files to DBFS...")

# Write Batch 1
customers_batch1_path = f"{CUSTOMERS_PATH}/{BATCH_1}"
customers_batch1_df.write.mode("overwrite").format("orc").save(customers_batch1_path)
print(f"✓ Wrote customers Batch 1 to: {customers_batch1_path}")

# Write Batch 2
customers_batch2_path = f"{CUSTOMERS_PATH}/{BATCH_2}"
customers_batch2_df.write.mode("overwrite").format("orc").save(customers_batch2_path)
print(f"✓ Wrote customers Batch 2 to: {customers_batch2_path}")

# List files to verify
print("\nVerifying customers files:")
dbutils.fs.ls(CUSTOMERS_PATH)

# COMMAND ----------

# DBTITLE 1,Create Target Tables - Products
print("Creating target products table...")

target_products_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.products"

# Create table from batch 1 and batch 2 data
products_all_df = products_batch1_df.union(products_batch2_df)
products_all_df.write.mode("overwrite").format("delta").saveAsTable(target_products_table)

print(f"✓ Created target table: {target_products_table}")
print(f"  Total rows: {spark.table(target_products_table).count()}")
display(spark.table(target_products_table))

# COMMAND ----------

# DBTITLE 1,Create Target Tables - Customers
print("Creating target customers table...")

target_customers_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.customers"

# Create table from batch 1 and batch 2 data
customers_all_df = customers_batch1_df.union(customers_batch2_df)
customers_all_df.write.mode("overwrite").format("delta").saveAsTable(target_customers_table)

print(f"✓ Created target table: {target_customers_table}")
print(f"  Total rows: {spark.table(target_customers_table).count()}")
display(spark.table(target_customers_table))

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_config - Products
print("Inserting products config into serving_ingestion_config...")

products_config_insert = f"""
INSERT INTO {constants.INGESTION_CONFIG_TABLE}
(
    config_id,
    group_name,
    source_schema,
    source_table,
    source_file_path,
    target_catalog,
    target_schema,
    target_table,
    source_file_format,
    load_type,
    write_mode,
    primary_key,
    partition_column,
    is_active,
    insert_ts,
    last_update_ts
)
VALUES (
    'CONFIG_PRODUCTS_001',
    '{TEST_GROUP}',
    '{SOURCE_DATABASE}',
    'products',
    '{PRODUCTS_PATH}',
    '{TEST_CATALOG}',
    '{TEST_SCHEMA}',
    'products',
    'orc',
    'batch',
    'append',
    'product_id',
    NULL,
    'Y',
    current_timestamp(),
    current_timestamp()
)
"""

spark.sql(products_config_insert)
print("✓ Products config inserted")

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_config - Customers
print("Inserting customers config into serving_ingestion_config...")

customers_config_insert = f"""
INSERT INTO {constants.INGESTION_CONFIG_TABLE}
(
    config_id,
    group_name,
    source_schema,
    source_table,
    source_file_path,
    target_catalog,
    target_schema,
    target_table,
    source_file_format,
    load_type,
    write_mode,
    primary_key,
    partition_column,
    is_active,
    insert_ts,
    last_update_ts
)
VALUES (
    'CONFIG_CUSTOMERS_001',
    '{TEST_GROUP}',
    '{SOURCE_DATABASE}',
    'customers',
    '{CUSTOMERS_PATH}',
    '{TEST_CATALOG}',
    '{TEST_SCHEMA}',
    'customers',
    'orc',
    'batch',
    'append',
    'customer_id',
    NULL,
    'Y',
    current_timestamp(),
    current_timestamp()
)
"""

spark.sql(customers_config_insert)
print("✓ Customers config inserted")

# COMMAND ----------

# DBTITLE 1,Verify ingestion_config entries
print("Verifying ingestion_config entries...")

config_check = spark.sql(f"""
    SELECT 
        config_id,
        group_name,
        source_table,
        target_catalog,
        target_schema,
        target_table,
        primary_key,
        is_active
    FROM {constants.INGESTION_CONFIG_TABLE}
    WHERE group_name = '{TEST_GROUP}'
    ORDER BY target_table
""")

print(f"✓ Found {config_check.count()} config entries for group '{TEST_GROUP}'")
display(config_check)

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_metadata - Products Batch 1
print(f"Inserting products metadata for {BATCH_1}...")

# Get file info
products_b1_files = dbutils.fs.ls(products_batch1_path)
orc_files_b1 = [f for f in products_b1_files if f.path.endswith('.orc')]

for file_info in orc_files_b1:
    metadata_insert = f"""
    INSERT INTO {constants.INGESTION_METADATA_TABLE}
    (
        table_name,
        batch_load_id,
        source_file_path,
        file_modification_time,
        file_size,
        is_processed,
        insert_ts,
        last_update_ts
    )
    VALUES (
        '{target_products_table}',
        '{BATCH_1}',
        '{file_info.path}',
        current_timestamp(),
        {file_info.size},
        'N',
        current_timestamp(),
        current_timestamp()
    )
    """
    spark.sql(metadata_insert)

print(f"✓ Inserted {len(orc_files_b1)} file(s) for products {BATCH_1}")

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_metadata - Products Batch 2
print(f"Inserting products metadata for {BATCH_2}...")

# Get file info
products_b2_files = dbutils.fs.ls(products_batch2_path)
orc_files_b2 = [f for f in products_b2_files if f.path.endswith('.orc')]

for file_info in orc_files_b2:
    metadata_insert = f"""
    INSERT INTO {constants.INGESTION_METADATA_TABLE}
    (
        table_name,
        batch_load_id,
        source_file_path,
        file_modification_time,
        file_size,
        is_processed,
        insert_ts,
        last_update_ts
    )
    VALUES (
        '{target_products_table}',
        '{BATCH_2}',
        '{file_info.path}',
        current_timestamp(),
        {file_info.size},
        'N',
        current_timestamp(),
        current_timestamp()
    )
    """
    spark.sql(metadata_insert)

print(f"✓ Inserted {len(orc_files_b2)} file(s) for products {BATCH_2}")

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_metadata - Customers Batch 1
print(f"Inserting customers metadata for {BATCH_1}...")

# Get file info
customers_b1_files = dbutils.fs.ls(customers_batch1_path)
orc_files_b1_cust = [f for f in customers_b1_files if f.path.endswith('.orc')]

for file_info in orc_files_b1_cust:
    metadata_insert = f"""
    INSERT INTO {constants.INGESTION_METADATA_TABLE}
    (
        table_name,
        batch_load_id,
        source_file_path,
        file_modification_time,
        file_size,
        is_processed,
        insert_ts,
        last_update_ts
    )
    VALUES (
        '{target_customers_table}',
        '{BATCH_1}',
        '{file_info.path}',
        current_timestamp(),
        {file_info.size},
        'N',
        current_timestamp(),
        current_timestamp()
    )
    """
    spark.sql(metadata_insert)

print(f"✓ Inserted {len(orc_files_b1_cust)} file(s) for customers {BATCH_1}")

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_metadata - Customers Batch 2
print(f"Inserting customers metadata for {BATCH_2}...")

# Get file info
customers_b2_files = dbutils.fs.ls(customers_batch2_path)
orc_files_b2_cust = [f for f in customers_b2_files if f.path.endswith('.orc')]

for file_info in orc_files_b2_cust:
    metadata_insert = f"""
    INSERT INTO {constants.INGESTION_METADATA_TABLE}
    (
        table_name,
        batch_load_id,
        source_file_path,
        file_modification_time,
        file_size,
        is_processed,
        insert_ts,
        last_update_ts
    )
    VALUES (
        '{target_customers_table}',
        '{BATCH_2}',
        '{file_info.path}',
        current_timestamp(),
        {file_info.size},
        'N',
        current_timestamp(),
        current_timestamp()
    )
    """
    spark.sql(metadata_insert)

print(f"✓ Inserted {len(orc_files_b2_cust)} file(s) for customers {BATCH_2}")

# COMMAND ----------

# DBTITLE 1,Verify serving_ingestion_metadata entries
print("Verifying serving_ingestion_metadata entries...")

metadata_check = spark.sql(f"""
    SELECT 
        table_name,
        batch_load_id,
        source_file_path,
        file_size,
        is_processed
    FROM {constants.INGESTION_METADATA_TABLE}
    WHERE table_name IN ('{target_products_table}', '{target_customers_table}')
    ORDER BY table_name, batch_load_id
""")

print(f"✓ Found {metadata_check.count()} metadata entries")
display(metadata_check)

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_audit - Products Batch 1
print(f"Inserting audit entry for products {BATCH_1}...")

audit_insert = f"""
INSERT INTO {constants.INGESTION_AUDIT_TABLE}
(
    run_id,
    config_id,
    batch_load_id,
    group_name,
    target_table_name,
    operation_type,
    load_type,
    status,
    start_ts,
    end_ts,
    row_count
)
VALUES (
    'RUN_{BATCH_1}',
    'CONFIG_PRODUCTS_001',
    '{BATCH_1}',
    '{TEST_GROUP}',
    '{target_products_table}',
    'LOAD',
    'BATCH',
    'COMPLETED',
    current_timestamp(),
    current_timestamp(),
    {products_batch1_df.count()}
)
"""

spark.sql(audit_insert)
print(f"✓ Audit entry inserted for products {BATCH_1}")

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_audit - Products Batch 2
print(f"Inserting audit entry for products {BATCH_2}...")

audit_insert = f"""
INSERT INTO {constants.INGESTION_AUDIT_TABLE}
(
    run_id,
    config_id,
    batch_load_id,
    group_name,
    target_table_name,
    operation_type,
    load_type,
    status,
    start_ts,
    end_ts,
    row_count
)
VALUES (
    'RUN_{BATCH_2}',
    'CONFIG_PRODUCTS_001',
    '{BATCH_2}',
    '{TEST_GROUP}',
    '{target_products_table}',
    'LOAD',
    'BATCH',
    'COMPLETED',
    current_timestamp(),
    current_timestamp(),
    {products_batch2_df.count()}
)
"""

spark.sql(audit_insert)
print(f"✓ Audit entry inserted for products {BATCH_2}")

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_audit - Customers Batch 1
print(f"Inserting audit entry for customers {BATCH_1}...")

audit_insert = f"""
INSERT INTO {constants.INGESTION_AUDIT_TABLE}
(
    run_id,
    config_id,
    batch_load_id,
    group_name,
    target_table_name,
    operation_type,
    load_type,
    status,
    start_ts,
    end_ts,
    row_count
)
VALUES (
    'RUN_{BATCH_1}',
    'CONFIG_CUSTOMERS_001',
    '{BATCH_1}',
    '{TEST_GROUP}',
    '{target_customers_table}',
    'LOAD',
    'BATCH',
    'COMPLETED',
    current_timestamp(),
    current_timestamp(),
    {customers_batch1_df.count()}
)
"""

spark.sql(audit_insert)
print(f"✓ Audit entry inserted for customers {BATCH_1}")

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_audit - Customers Batch 2
print(f"Inserting audit entry for customers {BATCH_2}...")

audit_insert = f"""
INSERT INTO {constants.INGESTION_AUDIT_TABLE}
(
    run_id,
    config_id,
    batch_load_id,
    group_name,
    target_table_name,
    operation_type,
    load_type,
    status,
    start_ts,
    end_ts,
    row_count
)
VALUES (
    'RUN_{BATCH_2}',
    'CONFIG_CUSTOMERS_001',
    '{BATCH_2}',
    '{TEST_GROUP}',
    '{target_customers_table}',
    'LOAD',
    'BATCH',
    'COMPLETED',
    current_timestamp(),
    current_timestamp(),
    {customers_batch2_df.count()}
)
"""

spark.sql(audit_insert)
print(f"✓ Audit entry inserted for customers {BATCH_2}")

# COMMAND ----------

# DBTITLE 1,Verify serving_ingestion_audit entries
print("Verifying serving_ingestion_audit entries...")

audit_check = spark.sql(f"""
    SELECT 
        run_id,
        batch_load_id,
        group_name,
        target_table_name,
        operation_type,
        load_type,
        status,
        row_count
    FROM {constants.INGESTION_AUDIT_TABLE}
    WHERE group_name = '{TEST_GROUP}'
    ORDER BY target_table_name, batch_load_id
""")

print(f"✓ Found {audit_check.count()} audit entries for group '{TEST_GROUP}'")
display(audit_check)

# COMMAND ----------

# DBTITLE 1,Setup Summary
print("=" * 80)
print("SETUP COMPLETE - SUMMARY")
print("=" * 80)
print(f"\n📋 Test Group: {TEST_GROUP}")
print(f"\n✅ Tables Created:")
print(f"   1. {target_products_table} ({spark.table(target_products_table).count()} rows)")
print(f"   2. {target_customers_table} ({spark.table(target_customers_table).count()} rows)")

print(f"\n✅ Batches Created:")
print(f"   Batch 1: {BATCH_1}")
print(f"   Batch 2: {BATCH_2}")

print(f"\n✅ Source Files Created:")
print(f"   Products: {PRODUCTS_PATH}")
print(f"   Customers: {CUSTOMERS_PATH}")

print(f"\n✅ Metadata Tables Populated:")
print(f"   - {constants.INGESTION_CONFIG_TABLE}")
print(f"   - {constants.INGESTION_METADATA_TABLE}")
print(f"   - {constants.INGESTION_AUDIT_TABLE}")

print(f"\n📊 Expected Validation Results:")
print(f"   - 2 tables to validate")
print(f"   - 2 batches per table")
print(f"   - 4 validation runs total")

print(f"\n🔍 Quick Validation Check:")
metadata_summary = spark.sql(f"""
    SELECT 
        table_name,
        batch_load_id,
        COUNT(*) as file_count,
        is_processed
    FROM {constants.INGESTION_METADATA_TABLE}
    WHERE table_name IN ('{target_products_table}', '{target_customers_table}')
    GROUP BY table_name, batch_load_id, is_processed
    ORDER BY table_name, batch_load_id
""")
display(metadata_summary)

print(f"\n🎯 Next Steps:")
print(f"   1. Run: notebooks/setup/02_setup_validation_mapping.py")
print(f"      This will create validation_mapping entries for '{TEST_GROUP}'")
print(f"")
print(f"   2. Create validation job manually or using job_utils")
print(f"")
print(f"   3. Run the validation job to test the framework")
print(f"")
print(f"   4. Check results in:")
print(f"      - {constants.VALIDATION_LOG_TABLE}")
print(f"      - {constants.VALIDATION_SUMMARY_TABLE}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Test Data Preview
print("TEST DATA PREVIEW")
print("-" * 80)

print("\n📦 Products Sample:")
display(spark.table(target_products_table).limit(5))

print("\n👥 Customers Sample:")
display(spark.table(target_customers_table).limit(5))

print("\n📁 Files by Batch:")
files_by_batch = spark.sql(f"""
    SELECT 
        CASE 
            WHEN table_name LIKE '%products' THEN 'Products'
            WHEN table_name LIKE '%customers' THEN 'Customers'
        END as table,
        batch_load_id,
        COUNT(*) as file_count
    FROM {constants.INGESTION_METADATA_TABLE}
    WHERE table_name IN ('{target_products_table}', '{target_customers_table}')
    GROUP BY table_name, batch_load_id
    ORDER BY table, batch_load_id
""")
display(files_by_batch)

# COMMAND ----------

