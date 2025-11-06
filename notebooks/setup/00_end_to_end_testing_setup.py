# Databricks notebook source
"""
End-to-End Testing Setup for Delta Recon Framework

This notebook creates a complete test environment with:
- 6 source tables (2 ORC, 2 TEXT, 2 CSV)
- Sample data files in DBFS
- Target Delta tables
- Complete metadata entries in all required tables

Run this notebook BEFORE running the validation framework to test all features.
"""

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, LongType
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime, timedelta
import json

# Note: lit() is used to add the _aud_batch_load_id column to target tables

# COMMAND ----------

# DBTITLE 1,Configuration
# Test configuration
TEST_CATALOG = "ts42_demo"
TEST_SCHEMA = "test_e2e_validation"
TEST_GROUP_NAME = "test_e2e_group"
BATCH_LOAD_ID = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# DBFS paths for source files
DBFS_BASE_PATH = "/FileStore/delta_recon_test"

# Ingestion metadata tables
INGESTION_CONFIG_TABLE = "ts42_demo.migration_operations.serving_ingestion_config"
INGESTION_METADATA_TABLE = "ts42_demo.migration_operations.serving_ingestion_metadata"
INGESTION_AUDIT_TABLE = "ts42_demo.migration_operations.serving_ingestion_audit"

print(f"Test Catalog: {TEST_CATALOG}")
print(f"Test Schema: {TEST_SCHEMA}")
print(f"Test Group: {TEST_GROUP_NAME}")
print(f"Batch Load ID: {BATCH_LOAD_ID}")
print(f"DBFS Base Path: {DBFS_BASE_PATH}")

# COMMAND ----------

# DBTITLE 1,Cleanup Existing Test Data (Run this before re-running)

print("=" * 80)
print("CLEANING UP EXISTING TEST DATA")
print("=" * 80)

# Option 1: Clean everything (recommended for fresh start)
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
        deleted = spark.sql(f"""
            DELETE FROM {INGESTION_CONFIG_TABLE}
            WHERE group_name = '{TEST_GROUP_NAME}'
        """)
        print(f"   ✓ Cleaned {INGESTION_CONFIG_TABLE}")
    except Exception as e:
        print(f"   ⚠ Could not clean ingestion_config: {e}")
    
    # Clean ingestion_metadata
    try:
        deleted = spark.sql(f"""
            DELETE FROM {INGESTION_METADATA_TABLE}
            WHERE table_name LIKE '{TEST_CATALOG}.{TEST_SCHEMA}.%'
        """)
        print(f"   ✓ Cleaned {INGESTION_METADATA_TABLE}")
    except Exception as e:
        print(f"   ⚠ Could not clean ingestion_metadata: {e}")
    
    # Clean ingestion_audit
    try:
        deleted = spark.sql(f"""
            DELETE FROM {INGESTION_AUDIT_TABLE}
            WHERE group_name = '{TEST_GROUP_NAME}'
        """)
        print(f"   ✓ Cleaned {INGESTION_AUDIT_TABLE}")
    except Exception as e:
        print(f"   ⚠ Could not clean ingestion_audit: {e}")
    
    print("\n✓ Cleanup completed! Ready for fresh test setup.")
    print("=" * 80)
else:
    print("\n⚠ Cleanup skipped (CLEAN_ALL = False)")
    print("   WARNING: Re-running without cleanup will create duplicate metadata entries!")
    print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Create Test Schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {TEST_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_CATALOG}.{TEST_SCHEMA}")
print(f"✓ Created schema: {TEST_CATALOG}.{TEST_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Define Sample Data and Schemas

# ========================================
# 1. ORC Table 1: Customer Orders
# ========================================
orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_date", DateType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("order_status", StringType(), True),
    StructField("partition_date", StringType(), True)
])

orders_data = [
    (1001, 5001, datetime(2024, 11, 1).date(), 250.50, "COMPLETED", "20241101"),
    (1002, 5002, datetime(2024, 11, 1).date(), 175.25, "PENDING", "20241101"),
    (1003, 5003, datetime(2024, 11, 1).date(), 500.00, "COMPLETED", "20241101"),
    (1004, 5004, datetime(2024, 11, 2).date(), 89.99, "CANCELLED", "20241102"),
    (1005, 5005, datetime(2024, 11, 2).date(), 320.75, "COMPLETED", "20241102"),
    (1006, 5001, datetime(2024, 11, 2).date(), 125.00, "PENDING", "20241102"),
]

# ========================================
# 2. ORC Table 2: Product Inventory
# ========================================
inventory_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("warehouse_location", StringType(), True),
    StructField("last_updated", TimestampType(), True)
])

inventory_data = [
    (2001, "Widget A", 100, 25.50, "WAREHOUSE_1", datetime(2024, 11, 1, 10, 0, 0)),
    (2002, "Widget B", 250, 15.75, "WAREHOUSE_1", datetime(2024, 11, 1, 10, 0, 0)),
    (2003, "Gadget X", 50, 99.99, "WAREHOUSE_2", datetime(2024, 11, 1, 11, 30, 0)),
    (2004, "Gadget Y", 75, 150.00, "WAREHOUSE_2", datetime(2024, 11, 1, 11, 30, 0)),
    (2005, "Tool Z", 200, 45.25, "WAREHOUSE_3", datetime(2024, 11, 1, 14, 15, 0)),
]

# ========================================
# 3. TEXT Table 1: Customer Details (tab-separated)
# ========================================
customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("registration_date", DateType(), True),
    StructField("customer_tier", StringType(), True)
])

customers_data = [
    (5001, "John Smith", "john.smith@example.com", "555-0101", datetime(2023, 1, 15).date(), "GOLD"),
    (5002, "Jane Doe", "jane.doe@example.com", "555-0102", datetime(2023, 3, 20).date(), "SILVER"),
    (5003, "Bob Wilson", "bob.wilson@example.com", "555-0103", datetime(2023, 6, 10).date(), "GOLD"),
    (5004, "Alice Brown", "alice.brown@example.com", "555-0104", datetime(2023, 9, 5).date(), "BRONZE"),
    (5005, "Charlie Davis", "charlie.davis@example.com", "555-0105", datetime(2023, 11, 25).date(), "SILVER"),
]

# ========================================
# 4. TEXT Table 2: Transaction Log (pipe-separated)
# ========================================
transactions_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("transaction_date", TimestampType(), True),
    StructField("payment_method", StringType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_status", StringType(), True)
])

transactions_data = [
    ("TXN001", 1001, datetime(2024, 11, 1, 9, 30, 0), "CREDIT_CARD", 250.50, "SUCCESS"),
    ("TXN002", 1002, datetime(2024, 11, 1, 10, 15, 0), "DEBIT_CARD", 175.25, "SUCCESS"),
    ("TXN003", 1003, datetime(2024, 11, 1, 11, 45, 0), "CREDIT_CARD", 500.00, "SUCCESS"),
    ("TXN004", 1004, datetime(2024, 11, 2, 8, 20, 0), "PAYPAL", 89.99, "FAILED"),
    ("TXN005", 1005, datetime(2024, 11, 2, 13, 10, 0), "CREDIT_CARD", 320.75, "SUCCESS"),
]

# ========================================
# 5. CSV Table 1: Employee Records
# ========================================
employees_schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("hire_date", DateType(), True),
    StructField("is_active", StringType(), True)
])

employees_data = [
    (3001, "Michael", "Johnson", "ENGINEERING", 95000.00, datetime(2020, 1, 15).date(), "Y"),
    (3002, "Sarah", "Williams", "SALES", 75000.00, datetime(2021, 3, 20).date(), "Y"),
    (3003, "David", "Martinez", "ENGINEERING", 105000.00, datetime(2019, 6, 10).date(), "Y"),
    (3004, "Emily", "Garcia", "MARKETING", 68000.00, datetime(2022, 2, 5).date(), "Y"),
    (3005, "James", "Rodriguez", "SALES", 82000.00, datetime(2021, 11, 15).date(), "N"),
]

# ========================================
# 6. CSV Table 2: Sales Metrics
# ========================================
sales_metrics_schema = StructType([
    StructField("metric_id", StringType(), False),
    StructField("sales_date", DateType(), True),
    StructField("region", StringType(), True),
    StructField("total_sales", DoubleType(), True),
    StructField("total_orders", IntegerType(), True),
    StructField("avg_order_value", DoubleType(), True)
])

sales_metrics_data = [
    ("M001", datetime(2024, 11, 1).date(), "NORTH", 5250.50, 42, 125.01),
    ("M002", datetime(2024, 11, 1).date(), "SOUTH", 4175.25, 38, 109.88),
    ("M003", datetime(2024, 11, 1).date(), "EAST", 6500.00, 55, 118.18),
    ("M004", datetime(2024, 11, 2).date(), "WEST", 3890.99, 31, 125.52),
    ("M005", datetime(2024, 11, 2).date(), "NORTH", 7320.75, 60, 122.01),
]

print("✓ Sample data and schemas defined")

# COMMAND ----------

# DBTITLE 1,Create and Store ORC Source Files

# Table 1: Customer Orders (ORC)
orders_df = spark.createDataFrame(orders_data, orders_schema)
orders_path = f"{DBFS_BASE_PATH}/orc/orders/{BATCH_LOAD_ID}"
orders_df.write.mode("overwrite").format("orc").save(orders_path)
print(f"✓ Created ORC file: {orders_path}")

# Table 2: Product Inventory (ORC)
inventory_df = spark.createDataFrame(inventory_data, inventory_schema)
inventory_path = f"{DBFS_BASE_PATH}/orc/inventory/{BATCH_LOAD_ID}"
inventory_df.write.mode("overwrite").format("orc").save(inventory_path)
print(f"✓ Created ORC file: {inventory_path}")

# COMMAND ----------

# DBTITLE 1,Create and Store TEXT Source Files

# Table 3: Customer Details (TEXT - tab delimited, no header)
customers_df = spark.createDataFrame(customers_data, customers_schema)
customers_path = f"{DBFS_BASE_PATH}/text/customers/{BATCH_LOAD_ID}"
customers_df.write.mode("overwrite").option("sep", "\t").option("header", "false").format("csv").save(customers_path)
print(f"✓ Created TEXT file (tab-delimited): {customers_path}")

# Table 4: Transaction Log (TEXT - pipe delimited, no header)
transactions_df = spark.createDataFrame(transactions_data, transactions_schema)
transactions_path = f"{DBFS_BASE_PATH}/text/transactions/{BATCH_LOAD_ID}"
transactions_df.write.mode("overwrite").option("sep", "|").option("header", "false").format("csv").save(transactions_path)
print(f"✓ Created TEXT file (pipe-delimited): {transactions_path}")

# COMMAND ----------

# DBTITLE 1,Create and Store CSV Source Files

# Table 5: Employee Records (CSV with header)
employees_df = spark.createDataFrame(employees_data, employees_schema)
employees_path = f"{DBFS_BASE_PATH}/csv/employees/{BATCH_LOAD_ID}"
employees_df.write.mode("overwrite").option("header", "true").format("csv").save(employees_path)
print(f"✓ Created CSV file (with header): {employees_path}")

# Table 6: Sales Metrics (CSV with header)
sales_metrics_df = spark.createDataFrame(sales_metrics_data, sales_metrics_schema)
sales_metrics_path = f"{DBFS_BASE_PATH}/csv/sales_metrics/{BATCH_LOAD_ID}"
sales_metrics_df.write.mode("overwrite").option("header", "true").format("csv").save(sales_metrics_path)
print(f"✓ Created CSV file (with header): {sales_metrics_path}")

# COMMAND ----------

# DBTITLE 1,Create Target Delta Tables

# IMPORTANT: Target tables must have _aud_batch_load_id column for validation to work
# This column is typically added during ingestion process

# Table 1: Orders (with audit column)
orders_target = f"{TEST_CATALOG}.{TEST_SCHEMA}.orders"
orders_df_with_audit = orders_df.withColumn("_aud_batch_load_id", lit(BATCH_LOAD_ID))
orders_df_with_audit.write.mode("overwrite").format("delta").saveAsTable(orders_target)
print(f"✓ Created target table: {orders_target}")

# Table 2: Inventory (with audit column)
inventory_target = f"{TEST_CATALOG}.{TEST_SCHEMA}.inventory"
inventory_df_with_audit = inventory_df.withColumn("_aud_batch_load_id", lit(BATCH_LOAD_ID))
inventory_df_with_audit.write.mode("overwrite").format("delta").saveAsTable(inventory_target)
print(f"✓ Created target table: {inventory_target}")

# Table 3: Customers (with audit column)
customers_target = f"{TEST_CATALOG}.{TEST_SCHEMA}.customers"
customers_df_with_audit = customers_df.withColumn("_aud_batch_load_id", lit(BATCH_LOAD_ID))
customers_df_with_audit.write.mode("overwrite").format("delta").saveAsTable(customers_target)
print(f"✓ Created target table: {customers_target}")

# Table 4: Transactions (with audit column)
transactions_target = f"{TEST_CATALOG}.{TEST_SCHEMA}.transactions"
transactions_df_with_audit = transactions_df.withColumn("_aud_batch_load_id", lit(BATCH_LOAD_ID))
transactions_df_with_audit.write.mode("overwrite").format("delta").saveAsTable(transactions_target)
print(f"✓ Created target table: {transactions_target}")

# Table 5: Employees (with audit column)
employees_target = f"{TEST_CATALOG}.{TEST_SCHEMA}.employees"
employees_df_with_audit = employees_df.withColumn("_aud_batch_load_id", lit(BATCH_LOAD_ID))
employees_df_with_audit.write.mode("overwrite").format("delta").saveAsTable(employees_target)
print(f"✓ Created target table: {employees_target}")

# Table 6: Sales Metrics (with audit column)
sales_metrics_target = f"{TEST_CATALOG}.{TEST_SCHEMA}.sales_metrics"
sales_metrics_df_with_audit = sales_metrics_df.withColumn("_aud_batch_load_id", lit(BATCH_LOAD_ID))
sales_metrics_df_with_audit.write.mode("overwrite").format("delta").saveAsTable(sales_metrics_target)
print(f"✓ Created target table: {sales_metrics_target}")

print(f"\n✓ All target tables created with _aud_batch_load_id = '{BATCH_LOAD_ID}'")

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_config

# Helper function to create config entries
def create_config_entry(
    config_id, 
    table_name, 
    source_file_path, 
    source_file_format,
    source_file_options,
    primary_key,
    partition_column=None
):
    return {
        "config_id": config_id,
        "group_name": TEST_GROUP_NAME,
        "source_schema": "source_system",
        "source_table": table_name,
        "source_file_path": source_file_path,
        "target_catalog": TEST_CATALOG,
        "target_schema": TEST_SCHEMA,
        "target_table": table_name,
        "source_file_format": source_file_format,
        "source_file_options": json.dumps(source_file_options) if source_file_options else None,
        "load_type": "incremental",
        "write_mode": "partition_overwrite" if partition_column else "append",
        "primary_key": primary_key,
        "partition_column": partition_column,
        "partitioning_strategy": "PARTITIONED_BY" if partition_column else None,
        "frequency": "daily",
        "table_size_gb": "1",
        "is_active": "Y",
        "deduplicate": "N",
        "clean_column_names": "N",
        "insert_ts": datetime.now(),
        "last_update_ts": datetime.now()
    }

# Create config entries for all 6 tables
# NOTE: source_file_path must point to the directory containing the batch folder
config_entries = [
    # 1. Orders (ORC with partition_date column)
    create_config_entry(
        config_id="TEST_E2E_001",
        table_name="orders",
        source_file_path=f"{DBFS_BASE_PATH}/orc/orders/{BATCH_LOAD_ID}",
        source_file_format="orc",
        source_file_options={},
        primary_key="order_id"
        # Note: partition_date exists as a regular column in the data, not Hive-style partitioning
    ),
    
    # 2. Inventory (ORC, no partition)
    create_config_entry(
        config_id="TEST_E2E_002",
        table_name="inventory",
        source_file_path=f"{DBFS_BASE_PATH}/orc/inventory/{BATCH_LOAD_ID}",
        source_file_format="orc",
        source_file_options={},
        primary_key="product_id"
    ),
    
    # 3. Customers (TEXT, tab-delimited)
    create_config_entry(
        config_id="TEST_E2E_003",
        table_name="customers",
        source_file_path=f"{DBFS_BASE_PATH}/text/customers/{BATCH_LOAD_ID}",
        source_file_format="text",
        source_file_options={
            "header": "false",
            "sep": "\t",
            "schema": "customer_id INT, customer_name STRING, email STRING, phone STRING, registration_date DATE, customer_tier STRING"
        },
        primary_key="customer_id"
    ),
    
    # 4. Transactions (TEXT, pipe-delimited)
    create_config_entry(
        config_id="TEST_E2E_004",
        table_name="transactions",
        source_file_path=f"{DBFS_BASE_PATH}/text/transactions/{BATCH_LOAD_ID}",
        source_file_format="text",
        source_file_options={
            "header": "false",
            "sep": "|",
            "schema": "transaction_id STRING, order_id INT, transaction_date TIMESTAMP, payment_method STRING, transaction_amount DOUBLE, transaction_status STRING"
        },
        primary_key="transaction_id"
    ),
    
    # 5. Employees (CSV with header)
    create_config_entry(
        config_id="TEST_E2E_005",
        table_name="employees",
        source_file_path=f"{DBFS_BASE_PATH}/csv/employees/{BATCH_LOAD_ID}",
        source_file_format="csv",
        source_file_options={
            "header": "true",
            "inferSchema": "false",
            "sep": ",",
            "schema": "employee_id INT, first_name STRING, last_name STRING, department STRING, salary DOUBLE, hire_date DATE, is_active STRING"
        },
        primary_key="employee_id"
    ),
    
    # 6. Sales Metrics (CSV with header)
    create_config_entry(
        config_id="TEST_E2E_006",
        table_name="sales_metrics",
        source_file_path=f"{DBFS_BASE_PATH}/csv/sales_metrics/{BATCH_LOAD_ID}",
        source_file_format="csv",
        source_file_options={
            "header": "true",
            "inferSchema": "false",
            "sep": ",",
            "schema": "metric_id STRING, sales_date DATE, region STRING, total_sales DOUBLE, total_orders INT, avg_order_value DOUBLE"
        },
        primary_key="metric_id"
    ),
]

# Create DataFrame and insert
config_df = spark.createDataFrame(config_entries)
config_df.write.mode("append").saveAsTable(INGESTION_CONFIG_TABLE)
print(f"✓ Inserted {len(config_entries)} entries into {INGESTION_CONFIG_TABLE}")

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_metadata

import os

def get_all_data_files(path):
    """
    Get ALL data files from a directory (not just the first one!)
    This properly tests the framework's ability to handle multiple files per batch.
    """
    try:
        file_info = dbutils.fs.ls(path)
        data_files = []
        for f in file_info:
            # Skip Spark metadata files and empty files
            if not f.name.startswith('_') and f.size > 0:
                data_files.append((f.path, f.modificationTime, f.size))
        return data_files
    except Exception as e:
        print(f"Warning: Could not list files in {path}: {e}")
        return []

# Create metadata entries - ONE ENTRY PER FILE (not per table!)
metadata_entries = []

file_paths = [
    (orders_path, "orders"),
    (inventory_path, "inventory"),
    (customers_path, "customers"),
    (transactions_path, "transactions"),
    (employees_path, "employees"),
    (sales_metrics_path, "sales_metrics")
]

for dir_path, table_name in file_paths:
    # Get ALL files from this directory
    all_files = get_all_data_files(dir_path)
    
    print(f"  {table_name}: Found {len(all_files)} file(s)")
    
    # Create one metadata entry per file
    for source_file, mod_time, file_size in all_files:
        metadata_entries.append({
            "table_name": f"{TEST_CATALOG}.{TEST_SCHEMA}.{table_name}",
            "batch_load_id": BATCH_LOAD_ID,
            "source_file_path": source_file,
            "file_modification_time": datetime.fromtimestamp(mod_time / 1000),
            "file_size": file_size,
            "is_processed": "Y",
            "insert_ts": datetime.now(),
            "last_update_ts": datetime.now()
        })

# Create DataFrame and insert
metadata_df = spark.createDataFrame(metadata_entries)
metadata_df.write.mode("append").saveAsTable(INGESTION_METADATA_TABLE)
print(f"✓ Inserted {len(metadata_entries)} file entries into {INGESTION_METADATA_TABLE}")
print(f"  This tests the framework's ability to read multiple files per batch!")

# COMMAND ----------

# DBTITLE 1,Insert into serving_ingestion_audit

# Define schema for ingestion_audit (required because of None values)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType

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

# Create audit entries for each table
audit_entries = []

table_info = [
    ("TEST_E2E_001", "orders", 6),
    ("TEST_E2E_002", "inventory", 5),
    ("TEST_E2E_003", "customers", 5),
    ("TEST_E2E_004", "transactions", 5),
    ("TEST_E2E_005", "employees", 5),
    ("TEST_E2E_006", "sales_metrics", 5),
]

run_id = f"RUN_{BATCH_LOAD_ID}"

for config_id, table_name, row_count in table_info:
    audit_entries.append({
        "run_id": run_id,
        "config_id": config_id,
        "batch_load_id": BATCH_LOAD_ID,
        "group_name": TEST_GROUP_NAME,
        "target_table_name": f"{TEST_CATALOG}.{TEST_SCHEMA}.{table_name}",
        "operation_type": "LOAD",
        "load_type": "BATCH",
        "status": "COMPLETED",
        "start_ts": datetime.now() - timedelta(minutes=5),
        "end_ts": datetime.now(),
        "log_message": f"Test data loaded successfully for {table_name}",
        "microbatch_id": None,
        "row_count": row_count
    })

# Create DataFrame with explicit schema (required for None values)
audit_df = spark.createDataFrame(audit_entries, schema=ingestion_audit_schema)
audit_df.write.mode("append").saveAsTable(INGESTION_AUDIT_TABLE)
print(f"✓ Inserted {len(audit_entries)} entries into {INGESTION_AUDIT_TABLE}")

# COMMAND ----------

# DBTITLE 1,Verify Setup - Show Created Files

print("\n" + "="*80)
print("DBFS FILES CREATED")
print("="*80)

try:
    files = dbutils.fs.ls(DBFS_BASE_PATH)
    for f in files:
        print(f"  {f.path}")
        try:
            subfiles = dbutils.fs.ls(f.path)
            for sf in subfiles:
                print(f"    {sf.name}")
        except:
            pass
except Exception as e:
    print(f"Could not list files: {e}")

# COMMAND ----------

# DBTITLE 1,Verify Setup - Show Table Counts

print("\n" + "="*80)
print("TARGET DELTA TABLES")
print("="*80)

tables = [
    "orders", "inventory", "customers", 
    "transactions", "employees", "sales_metrics"
]

for table in tables:
    count = spark.table(f"{TEST_CATALOG}.{TEST_SCHEMA}.{table}").count()
    print(f"  {TEST_CATALOG}.{TEST_SCHEMA}.{table}: {count} rows")

# COMMAND ----------

# DBTITLE 1,Verify Setup - Show Metadata Entries

print("\n" + "="*80)
print("INGESTION CONFIG ENTRIES")
print("="*80)

config_check = spark.sql(f"""
    SELECT config_id, target_table, source_file_format, primary_key, partition_column
    FROM {INGESTION_CONFIG_TABLE}
    WHERE group_name = '{TEST_GROUP_NAME}'
    ORDER BY config_id
""")
config_check.show(truncate=False)

print("\n" + "="*80)
print("INGESTION METADATA ENTRIES")
print("="*80)

metadata_check = spark.sql(f"""
    SELECT table_name, batch_load_id, is_processed, file_size
    FROM {INGESTION_METADATA_TABLE}
    WHERE batch_load_id = '{BATCH_LOAD_ID}'
    ORDER BY table_name
""")
metadata_check.show(truncate=False)

print("\n" + "="*80)
print("INGESTION AUDIT ENTRIES")
print("="*80)

audit_check = spark.sql(f"""
    SELECT target_table_name, status, row_count, load_type
    FROM {INGESTION_AUDIT_TABLE}
    WHERE batch_load_id = '{BATCH_LOAD_ID}'
    ORDER BY target_table_name
""")
audit_check.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Summary and Next Steps

print("\n" + "="*80)
print("✅ END-TO-END TEST SETUP COMPLETE")
print("="*80)

print(f"""
Test Environment Summary:
------------------------
• Batch Load ID: {BATCH_LOAD_ID}
• Test Group: {TEST_GROUP_NAME}
• Test Schema: {TEST_CATALOG}.{TEST_SCHEMA}

Created Resources:
-----------------
✓ 6 Source Files in DBFS:
  - 2 ORC files (orders, inventory)
  - 2 TEXT files (customers with tab delimiter, transactions with pipe delimiter)
  - 2 CSV files (employees, sales_metrics with headers)

✓ 6 Target Delta Tables:
  - {TEST_CATALOG}.{TEST_SCHEMA}.orders (partitioned by partition_date)
  - {TEST_CATALOG}.{TEST_SCHEMA}.inventory
  - {TEST_CATALOG}.{TEST_SCHEMA}.customers
  - {TEST_CATALOG}.{TEST_SCHEMA}.transactions
  - {TEST_CATALOG}.{TEST_SCHEMA}.employees
  - {TEST_CATALOG}.{TEST_SCHEMA}.sales_metrics

✓ Metadata Entries:
  - 6 entries in {INGESTION_CONFIG_TABLE}
  - 6 entries in {INGESTION_METADATA_TABLE}
  - 6 entries in {INGESTION_AUDIT_TABLE}

Next Steps:
-----------
1. Run notebook: notebooks/setup/02_setup_validation_mapping.py
   This will create validation_mapping entries based on ingestion_config

2. Verify validation_mapping:
   SELECT * FROM cat_ril_nayeem_03.validation_v2.validation_mapping 
   WHERE table_group = '{TEST_GROUP_NAME}'

3. Run the validation framework:
   python deltarecon/runner.py --table-group {TEST_GROUP_NAME}

4. Check validation results:
   SELECT * FROM cat_ril_nayeem_03.validation_v2.validation_log
   WHERE table_family LIKE '%{TEST_SCHEMA}%'
   
   SELECT * FROM cat_ril_nayeem_03.validation_v2.validation_summary
   WHERE table_family LIKE '%{TEST_SCHEMA}%'

File Format Options Used:
------------------------
• ORC: No special options (native format)
• TEXT (tab): {{"header": "false", "sep": "\\t", "schema": "..."}}
• TEXT (pipe): {{"header": "false", "sep": "|", "schema": "..."}}
• CSV: {{"header": "true", "inferSchema": "false", "sep": ",", "schema": "..."}}

All source_file_options are stored as JSON strings in ingestion_config.
The framework will parse these and apply them when reading source files.
""")

print("\n✅ Ready for validation testing!")

# COMMAND ----------

# DBTITLE 1,Verification - Ensure No Duplicates

print("\n" + "=" * 80)
print("VERIFICATION: Checking for Duplicate Entries")
print("=" * 80)

all_good = True

# Check 1: Ingestion Config (should be exactly 6)
config_count = spark.sql(f"""
    SELECT COUNT(*) as count 
    FROM {INGESTION_CONFIG_TABLE}
    WHERE group_name = '{TEST_GROUP_NAME}'
""").collect()[0]['count']

if config_count == 6:
    print(f"\n✓ Ingestion Config: {config_count} entries (expected: 6)")
else:
    print(f"\n❌ Ingestion Config: {config_count} entries (expected: 6) - DUPLICATES DETECTED!")
    all_good = False

# Check 2: Ingestion Metadata (should be >= 6, one entry per file)
metadata_count = spark.sql(f"""
    SELECT COUNT(*) as count 
    FROM {INGESTION_METADATA_TABLE}
    WHERE table_name LIKE '{TEST_CATALOG}.{TEST_SCHEMA}.%'
""").collect()[0]['count']

# Show breakdown by table
metadata_breakdown = spark.sql(f"""
    SELECT table_name, COUNT(*) as file_count
    FROM {INGESTION_METADATA_TABLE}
    WHERE table_name LIKE '{TEST_CATALOG}.{TEST_SCHEMA}.%'
    GROUP BY table_name
    ORDER BY table_name
""").collect()

if metadata_count >= 6:
    print(f"✓ Ingestion Metadata: {metadata_count} file entries (expected: >=6)")
    print(f"  Breakdown by table:")
    for row in metadata_breakdown:
        table_short = row['table_name'].split('.')[-1]
        print(f"    {table_short}: {row['file_count']} file(s)")
else:
    print(f"❌ Ingestion Metadata: {metadata_count} entries (expected: >=6)")
    all_good = False

# Check 3: Ingestion Audit (should be exactly 6)
audit_count = spark.sql(f"""
    SELECT COUNT(*) as count 
    FROM {INGESTION_AUDIT_TABLE}
    WHERE group_name = '{TEST_GROUP_NAME}'
""").collect()[0]['count']

if audit_count == 6:
    print(f"✓ Ingestion Audit: {audit_count} entries (expected: 6)")
else:
    print(f"❌ Ingestion Audit: {audit_count} entries (expected: 6) - DUPLICATES DETECTED!")
    all_good = False

# Check 4: Target tables have _aud_batch_load_id column
print(f"\n✓ Checking target tables for _aud_batch_load_id column...")

tables_to_check = ["orders", "inventory", "customers", "transactions", "employees", "sales_metrics"]
missing_audit_col = []

for table in tables_to_check:
    try:
        columns = spark.table(f"{TEST_CATALOG}.{TEST_SCHEMA}.{table}").columns
        if "_aud_batch_load_id" in columns:
            print(f"  ✓ {table}: Has _aud_batch_load_id column")
        else:
            print(f"  ❌ {table}: Missing _aud_batch_load_id column")
            missing_audit_col.append(table)
            all_good = False
    except Exception as e:
        print(f"  ❌ {table}: Table not found or error: {e}")
        all_good = False

# Check 5: Row counts match expected
print(f"\n✓ Checking target table row counts...")

expected_counts = {
    "orders": 6,
    "inventory": 5,
    "customers": 5,
    "transactions": 5,
    "employees": 5,
    "sales_metrics": 5
}

for table, expected in expected_counts.items():
    try:
        actual = spark.table(f"{TEST_CATALOG}.{TEST_SCHEMA}.{table}").count()
        if actual == expected:
            print(f"  ✓ {table}: {actual} rows (expected: {expected})")
        else:
            print(f"  ❌ {table}: {actual} rows (expected: {expected})")
            all_good = False
    except Exception as e:
        print(f"  ❌ {table}: Error counting rows: {e}")
        all_good = False

# Check 6: All target rows have the correct batch_load_id
print(f"\n✓ Checking batch_load_id values in target tables...")

for table in tables_to_check:
    try:
        distinct_batches = spark.sql(f"""
            SELECT DISTINCT _aud_batch_load_id 
            FROM {TEST_CATALOG}.{TEST_SCHEMA}.{table}
        """).collect()
        
        if len(distinct_batches) == 1 and distinct_batches[0]['_aud_batch_load_id'] == BATCH_LOAD_ID:
            print(f"  ✓ {table}: All rows have batch_load_id = {BATCH_LOAD_ID}")
        else:
            print(f"  ❌ {table}: Multiple or incorrect batch_load_ids found")
            for batch in distinct_batches:
                print(f"     Found: {batch['_aud_batch_load_id']}")
            all_good = False
    except Exception as e:
        print(f"  ⚠ {table}: Could not check batch_load_id: {e}")

# Final verdict
print("\n" + "=" * 80)
if all_good:
    print("✅ ALL VERIFICATION CHECKS PASSED!")
    print("\nTest environment is ready:")
    print("  • No duplicate metadata entries")
    print("  • All target tables have _aud_batch_load_id column")
    print("  • Row counts are correct")
    print("  • Batch IDs are consistent")
    print("\n✓ You can now proceed to run validation!")
else:
    print("❌ SOME VERIFICATION CHECKS FAILED!")
    print("\nPlease review the errors above.")
    print("Suggested actions:")
    print("  1. Set CLEAN_ALL = True in the cleanup cell")
    print("  2. Re-run the entire notebook from the beginning")
    print("  3. Check for any errors during table creation")
print("=" * 80)

