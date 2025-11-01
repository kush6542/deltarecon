# Databricks notebook source
"""
DeltaRecon - Entry Point

This is a thin wrapper that calls the DeltaRecon validation framework.
All business logic is in the deltarecon package.

Usage:
  This notebook is called by Databricks jobs with parameters:
  - table_group: Name of the table group to validate
  - iteration_suffix: Suffix for iteration name (e.g., "daily", "hourly")
"""

from pyspark.sql import SparkSession
from deltarecon import ValidationRunner

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Get Parameters
dbutils.widgets.text("table_group", "")
dbutils.widgets.text("iteration_suffix", "default")
dbutils.widgets.text("isFullValidation", "false")

table_group = dbutils.widgets.get("table_group")
iteration_suffix = dbutils.widgets.get("iteration_suffix")

# Parse isFullValidation parameter
is_full_validation_str = dbutils.widgets.get("isFullValidation").strip().lower()
if is_full_validation_str in ["true", "false"]:
    is_full_validation = (is_full_validation_str == "true")
else:
    print(f"WARNING: Invalid parameter value for isFullValidation: '{is_full_validation_str}'. Valid values are 'true' or 'false'. Proceeding with False.")
    is_full_validation = False

print(f"Full Validation Mode: {'ENABLED' if is_full_validation else 'DISABLED'}")

# COMMAND ----------

# DBTITLE 1,Run Validation
# Create runner and execute
runner = ValidationRunner(
    spark=spark,
    table_group=table_group,
    iteration_suffix=iteration_suffix,
    is_full_validation=is_full_validation
)

# Run validation - all orchestration happens inside the runner
result = runner.run()

# COMMAND ----------

# DBTITLE 1,Display Summary
print("VALIDATION COMPLETE")
print(f"Total tables: {result['total']}")
print(f"Success: {result['success']}")
print(f"Failed: {result['failed']}")
print(f"No new batches: {result['no_batches']}")

# Exit with appropriate status
if result['failed'] > 0:
    print(f"\n{result['failed']} table(s) failed validation")
else:
    print("\nAll validations passed!")
