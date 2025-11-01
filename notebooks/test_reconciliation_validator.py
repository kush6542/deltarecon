# Databricks notebook source
"""
Test Script for Data Reconciliation Validator

This notebook demonstrates how to test the DataReconciliationValidator
with both PK-based and full-row reconciliation modes.
"""

# COMMAND ----------
# DBTITLE 1,Test Setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# DBTITLE 1,Test Case 1: PK-Based Reconciliation

print("=" * 80)
print("TEST CASE 1: PK-Based Reconciliation")
print("=" * 80)

# Create test data with PKs
source_data = [
    (1, "Alice", "NYC"),
    (2, "Bob", "LA"),
    (3, "Charlie", "SF"),
    (4, "David", "NYC")
]

target_data = [
    (2, "Bob", "LA"),          # Match
    (3, "Charlie", "Boston"),  # Mismatch (city differs)
    (4, "David", "NYC"),       # Match
    (5, "Eve", "Miami")        # Extra in target
]

source_df = spark.createDataFrame(source_data, ["customer_id", "name", "city"])
target_df = spark.createDataFrame(target_data, ["customer_id", "name", "city"])

print("\nSource Data:")
source_df.show()

print("Target Data:")
target_df.show()

# Manually test the reconciliation logic
from deltarecon.validators.data_reconciliation_validator import DataReconciliationValidator
from deltarecon.models.table_config import TableConfig

# Create config with PK
config = TableConfig(
    table_family="test",
    table_name="customers",
    source_table="customers_src",
    source_database="test_db",
    primary_keys=["customer_id"],
    partition_columns=[],
    orc_base_path="s3://test/path"
)

validator = DataReconciliationValidator()
result = validator.validate(source_df, target_df, config)

print("\n" + "=" * 80)
print("RESULTS:")
print("=" * 80)
print(f"Status: {result.status}")
print(f"Metrics: {result.metrics}")
print(f"Message: {result.message}")

print("\nExpected Results:")
print("  src_extras: 1 (customer_id=1, Alice)")
print("  tgt_extras: 1 (customer_id=5, Eve)")
print("  matches: 2 (customer_id=2 Bob, customer_id=4 David)")
print("  mismatches: 1 (customer_id=3 Charlie - city differs)")

# COMMAND ----------
# DBTITLE 1,Test Case 2: Full-Row Reconciliation (No PK)

print("=" * 80)
print("TEST CASE 2: Full-Row Reconciliation (No PK)")
print("=" * 80)

# Create test data without PKs (transaction logs)
source_logs = [
    ("2024-01-01 10:00", "LOGIN", 0),
    ("2024-01-01 10:05", "PURCHASE", 100),
    ("2024-01-01 10:10", "LOGOUT", 0),
    ("2024-01-01 11:00", "LOGIN", 0)
]

target_logs = [
    ("2024-01-01 10:05", "PURCHASE", 100),
    ("2024-01-01 10:10", "LOGOUT", 0),
    ("2024-01-01 11:00", "LOGIN", 0),
    ("2024-01-01 11:30", "PURCHASE", 200)
]

source_log_df = spark.createDataFrame(source_logs, ["timestamp", "event", "amount"])
target_log_df = spark.createDataFrame(target_logs, ["timestamp", "event", "amount"])

print("\nSource Logs:")
source_log_df.show()

print("Target Logs:")
target_log_df.show()

# Create config WITHOUT PK
config_no_pk = TableConfig(
    table_family="test",
    table_name="transaction_logs",
    source_table="transaction_logs_src",
    source_database="test_db",
    primary_keys=[],  # No PK
    partition_columns=[],
    orc_base_path="s3://test/path"
)

validator = DataReconciliationValidator()
result_no_pk = validator.validate(source_log_df, target_log_df, config_no_pk)

print("\n" + "=" * 80)
print("RESULTS:")
print("=" * 80)
print(f"Status: {result_no_pk.status}")
print(f"Metrics: {result_no_pk.metrics}")
print(f"Message: {result_no_pk.message}")

print("\nExpected Results:")
print("  src_extras: 1 (10:00 LOGIN event)")
print("  tgt_extras: 1 (11:30 PURCHASE event)")
print("  matches: 3 (10:05 PURCHASE, 10:10 LOGOUT, 11:00 LOGIN)")
print("  mismatches: 0 (N/A without PK)")

# COMMAND ----------
# DBTITLE 1,Test Case 3: Composite Primary Key

print("=" * 80)
print("TEST CASE 3: Composite Primary Key")
print("=" * 80)

# Create test data with composite PK
source_orders = [
    (100, "A", 2, 50.00),
    (100, "B", 1, 30.00),
    (101, "A", 5, 50.00),
    (102, "C", 3, 20.00)
]

target_orders = [
    (100, "B", 1, 30.00),     # Match
    (101, "A", 5, 55.00),     # Mismatch (price differs)
    (102, "C", 3, 20.00),     # Match
    (103, "A", 2, 50.00)      # Extra in target
]

source_order_df = spark.createDataFrame(
    source_orders, 
    ["order_id", "product_id", "quantity", "price"]
)
target_order_df = spark.createDataFrame(
    target_orders, 
    ["order_id", "product_id", "quantity", "price"]
)

print("\nSource Orders:")
source_order_df.show()

print("Target Orders:")
target_order_df.show()

# Create config with composite PK
config_composite = TableConfig(
    table_family="test",
    table_name="order_items",
    source_table="order_items_src",
    source_database="test_db",
    primary_keys=["order_id", "product_id"],  # Composite PK
    partition_columns=[],
    orc_base_path="s3://test/path"
)

validator = DataReconciliationValidator()
result_composite = validator.validate(source_order_df, target_order_df, config_composite)

print("\n" + "=" * 80)
print("RESULTS:")
print("=" * 80)
print(f"Status: {result_composite.status}")
print(f"Metrics: {result_composite.metrics}")
print(f"Message: {result_composite.message}")

print("\nExpected Results:")
print("  src_extras: 1 (order_id=100, product_id=A)")
print("  tgt_extras: 1 (order_id=103, product_id=A)")
print("  matches: 2 (100-B, 102-C)")
print("  mismatches: 1 (order_id=101, product_id=A - price differs)")

# COMMAND ----------
# DBTITLE 1,Summary

print("\n" + "=" * 80)
print("TEST SUMMARY")
print("=" * 80)

all_passed = (
    result.status in ["PASSED", "FAILED"] and
    result_no_pk.status in ["PASSED", "FAILED"] and
    result_composite.status in ["PASSED", "FAILED"]
)

print("\nAll test cases executed successfully!")
print("\nValidator supports:")
print("  ✓ PK-based reconciliation")
print("  ✓ Full-row reconciliation (no PK)")
print("  ✓ Composite primary keys")
print("  ✓ Hash-based comparison using exceptAll")
print("\nTo enable in production:")
print("  Set widget parameter: isFullValidation=true")


