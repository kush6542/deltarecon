# Databricks notebook source
"""
Additional End-to-End Testing Setup for Delta Recon Framework (Test Set 2)

This notebook creates a SECOND test environment with different scenarios:
- 6 more source tables (2 ORC, 2 TEXT, 2 CSV)
- Different data volumes and complexity
- Tests framework with multiple test groups
- All files properly registered in metadata

Run this AFTER 00_end_to_end_testing_setup.py to have 2 test groups.
"""

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, LongType, BooleanType, DecimalType
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime, timedelta
import json

# COMMAND ----------

# DBTITLE 1,Configuration
# Test configuration - DIFFERENT from test set 1
TEST_CATALOG = "ts42_demo"
TEST_SCHEMA = "test_e2e_validation_v2"  # Different schema
TEST_GROUP_NAME = "test_e2e_group_v2"   # Different group
BATCH_LOAD_ID = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# DBFS paths for source files - DIFFERENT path
DBFS_BASE_PATH = "/FileStore/delta_recon_test_v2"

# Ingestion metadata tables (same as before)
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
print("CLEANING UP EXISTING TEST DATA - VERSION 2")
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
# 1. ORC Table 1: User Accounts (10 rows)
# ========================================
accounts_schema = StructType([
    StructField("account_id", IntegerType(), False),
    StructField("username", StringType(), True),
    StructField("email", StringType(), True),
    StructField("account_status", StringType(), True),
    StructField("created_date", DateType(), True),
    StructField("last_login", TimestampType(), True),
    StructField("account_balance", DoubleType(), True)
])

accounts_data = [
    (10001, "alice_smith", "alice@example.com", "ACTIVE", datetime(2024, 1, 15).date(), datetime(2024, 11, 5, 14, 30, 0), 5000.00),
    (10002, "bob_jones", "bob@example.com", "ACTIVE", datetime(2024, 2, 20).date(), datetime(2024, 11, 5, 10, 15, 0), 3250.50),
    (10003, "carol_white", "carol@example.com", "SUSPENDED", datetime(2024, 3, 10).date(), datetime(2024, 10, 28, 9, 0, 0), 0.00),
    (10004, "david_brown", "david@example.com", "ACTIVE", datetime(2024, 4, 5).date(), datetime(2024, 11, 5, 16, 45, 0), 12500.75),
    (10005, "emma_davis", "emma@example.com", "ACTIVE", datetime(2024, 5, 12).date(), datetime(2024, 11, 5, 11, 20, 0), 7800.25),
    (10006, "frank_miller", "frank@example.com", "CLOSED", datetime(2024, 6, 18).date(), datetime(2024, 9, 15, 13, 10, 0), 0.00),
    (10007, "grace_wilson", "grace@example.com", "ACTIVE", datetime(2024, 7, 22).date(), datetime(2024, 11, 5, 15, 5, 0), 4200.00),
    (10008, "henry_moore", "henry@example.com", "ACTIVE", datetime(2024, 8, 30).date(), datetime(2024, 11, 5, 12, 30, 0), 9150.50),
    (10009, "iris_taylor", "iris@example.com", "ACTIVE", datetime(2024, 9, 14).date(), datetime(2024, 11, 5, 17, 0, 0), 6300.00),
    (10010, "jack_anderson", "jack@example.com", "ACTIVE", datetime(2024, 10, 8).date(), datetime(2024, 11, 5, 8, 45, 0), 11000.00),
]

# ========================================
# 2. ORC Table 2: Product Catalog (8 rows)
# ========================================
products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("supplier_id", IntegerType(), True),
    StructField("is_available", StringType(), True)
])

products_data = [
    ("PROD_A001", "Wireless Mouse", "ELECTRONICS", 29.99, 150, 5001, "Y"),
    ("PROD_A002", "Mechanical Keyboard", "ELECTRONICS", 89.99, 75, 5001, "Y"),
    ("PROD_B001", "Office Chair", "FURNITURE", 249.99, 30, 5002, "Y"),
    ("PROD_B002", "Standing Desk", "FURNITURE", 599.99, 12, 5002, "Y"),
    ("PROD_C001", "Coffee Maker", "APPLIANCES", 79.99, 45, 5003, "Y"),
    ("PROD_C002", "Blender", "APPLIANCES", 49.99, 60, 5003, "Y"),
    ("PROD_D001", "LED Monitor 27\"", "ELECTRONICS", 299.99, 25, 5001, "N"),
    ("PROD_D002", "USB-C Hub", "ELECTRONICS", 39.99, 100, 5001, "Y"),
]

# ========================================
# 3. TEXT Table 1: Shipping Records (comma-delimited, no header)
# ========================================
shipping_schema = StructType([
    StructField("shipment_id", StringType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("tracking_number", StringType(), True),
    StructField("carrier", StringType(), True),
    StructField("ship_date", DateType(), True),
    StructField("delivery_date", DateType(), True),
    StructField("shipping_cost", DoubleType(), True)
])

shipping_data = [
    ("SHIP_001", 20001, "TRK123456789", "FedEx", datetime(2024, 11, 1).date(), datetime(2024, 11, 5).date(), 15.99),
    ("SHIP_002", 20002, "TRK987654321", "UPS", datetime(2024, 11, 2).date(), datetime(2024, 11, 6).date(), 12.50),
    ("SHIP_003", 20003, "TRK456789123", "USPS", datetime(2024, 11, 2).date(), datetime(2024, 11, 7).date(), 8.75),
    ("SHIP_004", 20004, "TRK321654987", "FedEx", datetime(2024, 11, 3).date(), datetime(2024, 11, 8).date(), 18.25),
    ("SHIP_005", 20005, "TRK789123456", "DHL", datetime(2024, 11, 3).date(), datetime(2024, 11, 9).date(), 22.00),
    ("SHIP_006", 20006, "TRK654987321", "UPS", datetime(2024, 11, 4).date(), datetime(2024, 11, 9).date(), 14.99),
    ("SHIP_007", 20007, "TRK147258369", "FedEx", datetime(2024, 11, 4).date(), datetime(2024, 11, 10).date(), 16.50),
]

# ========================================
# 4. TEXT Table 2: Payment Methods (pipe-delimited, no header)
# ========================================
payment_methods_schema = StructType([
    StructField("payment_method_id", StringType(), False),
    StructField("account_id", IntegerType(), False),
    StructField("method_type", StringType(), True),
    StructField("card_last_four", StringType(), True),
    StructField("expiry_date", StringType(), True),
    StructField("is_default", StringType(), True),
    StructField("added_date", DateType(), True)
])

payment_methods_data = [
    ("PM_001", 10001, "CREDIT_CARD", "4532", "12/2026", "Y", datetime(2024, 1, 15).date()),
    ("PM_002", 10002, "DEBIT_CARD", "5421", "08/2025", "Y", datetime(2024, 2, 20).date()),
    ("PM_003", 10003, "PAYPAL", "N/A", "N/A", "Y", datetime(2024, 3, 10).date()),
    ("PM_004", 10004, "CREDIT_CARD", "3782", "03/2027", "Y", datetime(2024, 4, 5).date()),
    ("PM_005", 10005, "DEBIT_CARD", "6011", "11/2025", "Y", datetime(2024, 5, 12).date()),
    ("PM_006", 10007, "CREDIT_CARD", "4111", "06/2026", "Y", datetime(2024, 7, 22).date()),
    ("PM_007", 10008, "BANK_TRANSFER", "N/A", "N/A", "Y", datetime(2024, 8, 30).date()),
    ("PM_008", 10009, "CREDIT_CARD", "5105", "09/2026", "Y", datetime(2024, 9, 14).date()),
    ("PM_009", 10010, "DEBIT_CARD", "4012", "01/2027", "Y", datetime(2024, 10, 8).date()),
]

# ========================================
# 5. CSV Table 1: Reviews (with header)
# ========================================
reviews_schema = StructType([
    StructField("review_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("account_id", IntegerType(), False),
    StructField("rating", IntegerType(), True),
    StructField("review_text", StringType(), True),
    StructField("review_date", DateType(), True),
    StructField("verified_purchase", StringType(), True)
])

reviews_data = [
    ("REV_001", "PROD_A001", 10001, 5, "Excellent mouse, very responsive!", datetime(2024, 10, 15).date(), "Y"),
    ("REV_002", "PROD_A002", 10002, 4, "Good keyboard but a bit loud", datetime(2024, 10, 18).date(), "Y"),
    ("REV_003", "PROD_B001", 10004, 5, "Very comfortable office chair", datetime(2024, 10, 20).date(), "Y"),
    ("REV_004", "PROD_C001", 10005, 3, "Works fine but nothing special", datetime(2024, 10, 22).date(), "Y"),
    ("REV_005", "PROD_A001", 10007, 5, "Great value for money", datetime(2024, 10, 25).date(), "Y"),
    ("REV_006", "PROD_D002", 10008, 4, "Good hub with many ports", datetime(2024, 10, 28).date(), "Y"),
    ("REV_007", "PROD_B002", 10009, 5, "Best standing desk I've used", datetime(2024, 11, 1).date(), "Y"),
    ("REV_008", "PROD_C002", 10010, 4, "Solid blender for the price", datetime(2024, 11, 3).date(), "Y"),
]

# ========================================
# 6. CSV Table 2: Audit Logs (with header, more rows - 12)
# ========================================
audit_logs_schema = StructType([
    StructField("log_id", StringType(), False),
    StructField("account_id", IntegerType(), False),
    StructField("action_type", StringType(), True),
    StructField("action_timestamp", TimestampType(), True),
    StructField("ip_address", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("status_code", IntegerType(), True)
])

audit_logs_data = [
    ("LOG_001", 10001, "LOGIN", datetime(2024, 11, 5, 8, 30, 0), "192.168.1.10", "Mozilla/5.0", 200),
    ("LOG_002", 10002, "LOGIN", datetime(2024, 11, 5, 9, 15, 0), "192.168.1.11", "Chrome/118.0", 200),
    ("LOG_003", 10001, "VIEW_PRODUCT", datetime(2024, 11, 5, 10, 0, 0), "192.168.1.10", "Mozilla/5.0", 200),
    ("LOG_004", 10004, "LOGIN", datetime(2024, 11, 5, 10, 30, 0), "192.168.1.15", "Safari/17.0", 200),
    ("LOG_005", 10003, "LOGIN_FAILED", datetime(2024, 11, 5, 11, 0, 0), "192.168.1.20", "Edge/119.0", 401),
    ("LOG_006", 10005, "ADD_TO_CART", datetime(2024, 11, 5, 11, 30, 0), "192.168.1.25", "Firefox/120.0", 200),
    ("LOG_007", 10007, "CHECKOUT", datetime(2024, 11, 5, 12, 0, 0), "192.168.1.30", "Chrome/118.0", 200),
    ("LOG_008", 10008, "LOGIN", datetime(2024, 11, 5, 13, 15, 0), "192.168.1.35", "Safari/17.0", 200),
    ("LOG_009", 10009, "VIEW_PRODUCT", datetime(2024, 11, 5, 14, 0, 0), "192.168.1.40", "Mozilla/5.0", 200),
    ("LOG_010", 10010, "LOGIN", datetime(2024, 11, 5, 15, 30, 0), "192.168.1.45", "Chrome/118.0", 200),
    ("LOG_011", 10001, "LOGOUT", datetime(2024, 11, 5, 16, 45, 0), "192.168.1.10", "Mozilla/5.0", 200),
    ("LOG_012", 10002, "UPDATE_PROFILE", datetime(2024, 11, 5, 17, 0, 0), "192.168.1.11", "Chrome/118.0", 200),
]

print("✓ Sample data and schemas defined")

# COMMAND ----------

# DBTITLE 1,Create and Store ORC Source Files

# Table 1: User Accounts (ORC)
accounts_df = spark.createDataFrame(accounts_data, accounts_schema)
accounts_path = f"{DBFS_BASE_PATH}/orc/accounts/{BATCH_LOAD_ID}"
accounts_df.write.mode("overwrite").format("orc").save(accounts_path)
print(f"✓ Created ORC file: {accounts_path}")

# Table 2: Product Catalog (ORC)
products_df = spark.createDataFrame(products_data, products_schema)
products_path = f"{DBFS_BASE_PATH}/orc/products/{BATCH_LOAD_ID}"
products_df.write.mode("overwrite").format("orc").save(products_path)
print(f"✓ Created ORC file: {products_path}")

# COMMAND ----------

# DBTITLE 1,Create and Store TEXT Source Files

# Table 3: Shipping Records (TEXT - comma delimited, no header)
shipping_df = spark.createDataFrame(shipping_data, shipping_schema)
shipping_path = f"{DBFS_BASE_PATH}/text/shipping/{BATCH_LOAD_ID}"
shipping_df.write.mode("overwrite").option("sep", ",").option("header", "false").format("csv").save(shipping_path)
print(f"✓ Created TEXT file (comma-delimited): {shipping_path}")

# Table 4: Payment Methods (TEXT - pipe delimited, no header)
payment_methods_df = spark.createDataFrame(payment_methods_data, payment_methods_schema)
payment_methods_path = f"{DBFS_BASE_PATH}/text/payment_methods/{BATCH_LOAD_ID}"
payment_methods_df.write.mode("overwrite").option("sep", "|").option("header", "false").format("csv").save(payment_methods_path)
print(f"✓ Created TEXT file (pipe-delimited): {payment_methods_path}")

# COMMAND ----------

# DBTITLE 1,Create and Store CSV Source Files

# Table 5: Reviews (CSV with header)
reviews_df = spark.createDataFrame(reviews_data, reviews_schema)
reviews_path = f"{DBFS_BASE_PATH}/csv/reviews/{BATCH_LOAD_ID}"
reviews_df.write.mode("overwrite").option("header", "true").format("csv").save(reviews_path)
print(f"✓ Created CSV file (with header): {reviews_path}")

# Table 6: Audit Logs (CSV with header)
audit_logs_df = spark.createDataFrame(audit_logs_data, audit_logs_schema)
audit_logs_path = f"{DBFS_BASE_PATH}/csv/audit_logs/{BATCH_LOAD_ID}"
audit_logs_df.write.mode("overwrite").option("header", "true").format("csv").save(audit_logs_path)
print(f"✓ Created CSV file (with header): {audit_logs_path}")

# COMMAND ----------

# DBTITLE 1,Create Target Delta Tables

# IMPORTANT: Target tables must have _aud_batch_load_id column for validation to work

# Table 1: Accounts
accounts_target = f"{TEST_CATALOG}.{TEST_SCHEMA}.accounts"
accounts_df_with_audit = accounts_df.withColumn("_aud_batch_load_id", lit(BATCH_LOAD_ID))
accounts_df_with_audit.write.mode("overwrite").format("delta").saveAsTable(accounts_target)
print(f"✓ Created target table: {accounts_target}")

# Table 2: Products
products_target = f"{TEST_CATALOG}.{TEST_SCHEMA}.products"
products_df_with_audit = products_df.withColumn("_aud_batch_load_id", lit(BATCH_LOAD_ID))
products_df_with_audit.write.mode("overwrite").format("delta").saveAsTable(products_target)
print(f"✓ Created target table: {products_target}")

# Table 3: Shipping
shipping_target = f"{TEST_CATALOG}.{TEST_SCHEMA}.shipping"
shipping_df_with_audit = shipping_df.withColumn("_aud_batch_load_id", lit(BATCH_LOAD_ID))
shipping_df_with_audit.write.mode("overwrite").format("delta").saveAsTable(shipping_target)
print(f"✓ Created target table: {shipping_target}")

# Table 4: Payment Methods
payment_methods_target = f"{TEST_CATALOG}.{TEST_SCHEMA}.payment_methods"
payment_methods_df_with_audit = payment_methods_df.withColumn("_aud_batch_load_id", lit(BATCH_LOAD_ID))
payment_methods_df_with_audit.write.mode("overwrite").format("delta").saveAsTable(payment_methods_target)
print(f"✓ Created target table: {payment_methods_target}")

# Table 5: Reviews
reviews_target = f"{TEST_CATALOG}.{TEST_SCHEMA}.reviews"
reviews_df_with_audit = reviews_df.withColumn("_aud_batch_load_id", lit(BATCH_LOAD_ID))
reviews_df_with_audit.write.mode("overwrite").format("delta").saveAsTable(reviews_target)
print(f"✓ Created target table: {reviews_target}")

# Table 6: Audit Logs
audit_logs_target = f"{TEST_CATALOG}.{TEST_SCHEMA}.audit_logs"
audit_logs_df_with_audit = audit_logs_df.withColumn("_aud_batch_load_id", lit(BATCH_LOAD_ID))
audit_logs_df_with_audit.write.mode("overwrite").format("delta").saveAsTable(audit_logs_target)
print(f"✓ Created target table: {audit_logs_target}")

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
        "source_schema": "source_system_v2",
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
config_entries = [
    # 1. Accounts (ORC)
    create_config_entry(
        config_id="TEST_E2E_V2_001",
        table_name="accounts",
        source_file_path=f"{DBFS_BASE_PATH}/orc/accounts/{BATCH_LOAD_ID}",
        source_file_format="orc",
        source_file_options={},
        primary_key="account_id"
    ),
    
    # 2. Products (ORC)
    create_config_entry(
        config_id="TEST_E2E_V2_002",
        table_name="products",
        source_file_path=f"{DBFS_BASE_PATH}/orc/products/{BATCH_LOAD_ID}",
        source_file_format="orc",
        source_file_options={},
        primary_key="product_id"
    ),
    
    # 3. Shipping (TEXT, comma-delimited)
    create_config_entry(
        config_id="TEST_E2E_V2_003",
        table_name="shipping",
        source_file_path=f"{DBFS_BASE_PATH}/text/shipping/{BATCH_LOAD_ID}",
        source_file_format="text",
        source_file_options={
            "header": "false",
            "sep": ",",
            "schema": "shipment_id STRING, order_id INT, tracking_number STRING, carrier STRING, ship_date DATE, delivery_date DATE, shipping_cost DOUBLE"
        },
        primary_key="shipment_id"
    ),
    
    # 4. Payment Methods (TEXT, pipe-delimited)
    create_config_entry(
        config_id="TEST_E2E_V2_004",
        table_name="payment_methods",
        source_file_path=f"{DBFS_BASE_PATH}/text/payment_methods/{BATCH_LOAD_ID}",
        source_file_format="text",
        source_file_options={
            "header": "false",
            "sep": "|",
            "schema": "payment_method_id STRING, account_id INT, method_type STRING, card_last_four STRING, expiry_date STRING, is_default STRING, added_date DATE"
        },
        primary_key="payment_method_id"
    ),
    
    # 5. Reviews (CSV with header)
    create_config_entry(
        config_id="TEST_E2E_V2_005",
        table_name="reviews",
        source_file_path=f"{DBFS_BASE_PATH}/csv/reviews/{BATCH_LOAD_ID}",
        source_file_format="csv",
        source_file_options={
            "header": "true",
            "inferSchema": "false",
            "sep": ",",
            "schema": "review_id STRING, product_id STRING, account_id INT, rating INT, review_text STRING, review_date DATE, verified_purchase STRING"
        },
        primary_key="review_id"
    ),
    
    # 6. Audit Logs (CSV with header)
    create_config_entry(
        config_id="TEST_E2E_V2_006",
        table_name="audit_logs",
        source_file_path=f"{DBFS_BASE_PATH}/csv/audit_logs/{BATCH_LOAD_ID}",
        source_file_format="csv",
        source_file_options={
            "header": "true",
            "inferSchema": "false",
            "sep": ",",
            "schema": "log_id STRING, account_id INT, action_type STRING, action_timestamp TIMESTAMP, ip_address STRING, user_agent STRING, status_code INT"
        },
        primary_key="log_id"
    ),
]

# Define schema for ingestion_config (required because of None values)
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

# Create DataFrame with explicit schema and insert
config_df = spark.createDataFrame(config_entries, schema=ingestion_config_schema)
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
    (accounts_path, "accounts"),
    (products_path, "products"),
    (shipping_path, "shipping"),
    (payment_methods_path, "payment_methods"),
    (reviews_path, "reviews"),
    (audit_logs_path, "audit_logs")
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
    ("TEST_E2E_V2_001", "accounts", 10),
    ("TEST_E2E_V2_002", "products", 8),
    ("TEST_E2E_V2_003", "shipping", 7),
    ("TEST_E2E_V2_004", "payment_methods", 9),
    ("TEST_E2E_V2_005", "reviews", 8),
    ("TEST_E2E_V2_006", "audit_logs", 12),
]

for config_id, table_name, row_count in table_info:
    run_id = f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    audit_entries.append({
        "run_id": run_id,
        "config_id": config_id,
        "batch_load_id": BATCH_LOAD_ID,
        "group_name": TEST_GROUP_NAME,
        "target_table_name": f"{TEST_CATALOG}.{TEST_SCHEMA}.{table_name}",
        "operation_type": "LOAD",
        "load_type": "incremental",
        "status": "COMPLETED",
        "start_ts": datetime.now() - timedelta(minutes=5),
        "end_ts": datetime.now(),
        "log_message": f"Successfully loaded {row_count} rows",
        "microbatch_id": None,
        "row_count": row_count
    })

# Create DataFrame with explicit schema and insert
audit_df = spark.createDataFrame(audit_entries, schema=ingestion_audit_schema)
audit_df.write.mode("append").saveAsTable(INGESTION_AUDIT_TABLE)
print(f"✓ Inserted {len(audit_entries)} entries into {INGESTION_AUDIT_TABLE}")

# COMMAND ----------

# DBTITLE 1,Verify Setup - Show Source Files

print("\n" + "="*80)
print("SOURCE FILES IN DBFS")
print("="*80)

tables = [
    ("accounts", accounts_path),
    ("products", products_path),
    ("shipping", shipping_path),
    ("payment_methods", payment_methods_path),
    ("reviews", reviews_path),
    ("audit_logs", audit_logs_path)
]

for table_name, path in tables:
    try:
        files = dbutils.fs.ls(path)
        data_files = [f for f in files if not f.name.startswith('_') and f.size > 0]
        print(f"\n{table_name}: {len(data_files)} file(s)")
        for f in data_files[:3]:  # Show first 3 files
            print(f"  - {f.name} ({f.size} bytes)")
    except Exception as e:
        print(f"\n{table_name}: ERROR - {e}")

# COMMAND ----------

# DBTITLE 1,Verify Setup - Show Target Tables

print("\n" + "="*80)
print("TARGET DELTA TABLES")
print("="*80)

tables = [
    "accounts", "products", "shipping", 
    "payment_methods", "reviews", "audit_logs"
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
    SELECT config_id, target_table, source_file_format, primary_key
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
print("✅ END-TO-END TEST SETUP V2 COMPLETE")
print("="*80)

print(f"""
Setup Summary:
--------------
✓ Test Group: {TEST_GROUP_NAME}
✓ Batch ID: {BATCH_LOAD_ID}
✓ Schema: {TEST_CATALOG}.{TEST_SCHEMA}

Tables Created:
--------------
1. accounts (ORC) - 10 rows
2. products (ORC) - 8 rows
3. shipping (TEXT, comma) - 7 rows
4. payment_methods (TEXT, pipe) - 9 rows
5. reviews (CSV) - 8 rows
6. audit_logs (CSV) - 12 rows

Total: 54 rows across 6 tables

Metadata Entries:
----------------
✓ {len(config_entries)} config entries
✓ {len(metadata_entries)} metadata file entries (all files registered)
✓ {len(audit_entries)} audit entries

Next Steps:
-----------
1. Run validation mapping setup:
   %run ../setup/02_setup_validation_mapping

2. Run validation:
   from deltarecon.runner import ValidationRunner
   runner = ValidationRunner(
       table_group="{TEST_GROUP_NAME}",
       iteration="e2e_test_v2_{{timestamp}}",
       isFullValidation=True
   )
   runner.run()

3. Check results in validation_summary table

This test set has DIFFERENT characteristics from V1:
- Different row counts (7-12 vs 5-6)
- Different table names and domains
- Same multi-file handling test
- Tests framework with multiple groups simultaneously
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

tables_to_check = ["accounts", "products", "shipping", "payment_methods", "reviews", "audit_logs"]
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
print(f"\n✓ Checking row counts...")

expected_counts = {
    "accounts": 10,
    "products": 8,
    "shipping": 7,
    "payment_methods": 9,
    "reviews": 8,
    "audit_logs": 12
}

for table, expected_count in expected_counts.items():
    actual_count = spark.table(f"{TEST_CATALOG}.{TEST_SCHEMA}.{table}").count()
    if actual_count == expected_count:
        print(f"  ✓ {table}: {actual_count} rows (expected: {expected_count})")
    else:
        print(f"  ❌ {table}: {actual_count} rows (expected: {expected_count})")
        all_good = False

# Check 6: Batch Load ID consistency
print(f"\n✓ Checking batch_load_id consistency in target tables...")

for table in tables_to_check:
    batch_ids = spark.sql(f"""
        SELECT DISTINCT _aud_batch_load_id 
        FROM {TEST_CATALOG}.{TEST_SCHEMA}.{table}
    """).collect()
    
    if len(batch_ids) == 1 and batch_ids[0]['_aud_batch_load_id'] == BATCH_LOAD_ID:
        print(f"  ✓ {table}: Correct batch_load_id ({BATCH_LOAD_ID})")
    else:
        print(f"  ❌ {table}: Incorrect or multiple batch_load_ids")
        all_good = False

# Final verdict
print("\n" + "=" * 80)
if all_good:
    print("✅ VERIFICATION PASSED - All checks successful!")
    print("=" * 80)
    print("\nYou can now run the validation framework.")
else:
    print("❌ VERIFICATION FAILED - Please review errors above")
    print("=" * 80)
    print("\nFix the issues before running validation.")

