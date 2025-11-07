# Databricks notebook source
# MAGIC %md
# MAGIC # Manual Batch Validation Debugger
# MAGIC 
# MAGIC **Purpose:** Debug validation failures for a specific table and batch in production or test environments
# MAGIC 
# MAGIC **Features:**
# MAGIC - Supports ORC, CSV, and Text file formats
# MAGIC - Uses framework's file selection logic
# MAGIC - Works with production and test environments
# MAGIC - Prints all files being read for transparency
# MAGIC 
# MAGIC **How to use:**
# MAGIC 1. Fill in the widgets at the top
# MAGIC 2. Run all cells
# MAGIC 3. Review each validation check output

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, md5, concat_ws, count, lit, regexp_extract, when
import json

# Import framework modules for file selection and loading
from deltarecon.core.batch_processor import BatchProcessor
from deltarecon.core.source_target_loader import SourceTargetLoader
from deltarecon.models.table_config import TableConfig

print("Setup complete - Framework modules imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input Parameters (Widgets)

# COMMAND ----------

# Create widgets
dbutils.widgets.removeAll()

dbutils.widgets.text("ingestion_ops_schema", "ts42_demo.migration_operations", "1. Ingestion Ops Schema (catalog.schema)")
dbutils.widgets.text("validation_schema", "cat_ril_nayeem_03.validation_v2", "2. Validation Schema (catalog.schema)")
dbutils.widgets.text("target_table", "ts42_demo.test_validation.customers", "3. Target Table")
dbutils.widgets.text("batch_load_id", "BATCH_20251102_000001", "4. Batch Load ID")
dbutils.widgets.dropdown("run_reconciliation", "N", ["Y", "N"], "5. Run Data Reconciliation?")

# Get values
INGESTION_OPS_SCHEMA = dbutils.widgets.get("ingestion_ops_schema")
VALIDATION_SCHEMA = dbutils.widgets.get("validation_schema")
TARGET_TABLE = dbutils.widgets.get("target_table")
BATCH_LOAD_ID = dbutils.widgets.get("batch_load_id")
RUN_RECONCILIATION = dbutils.widgets.get("run_reconciliation") == "Y"

# Define table names (matching constants.py)
INGESTION_CONFIG_TABLE = f"{INGESTION_OPS_SCHEMA}.serving_ingestion_config"
VALIDATION_MAPPING_TABLE = f"{VALIDATION_SCHEMA}.validation_mapping"

print("="*70)
print("INPUT PARAMETERS")
print("="*70)
print(f"Ingestion Ops Schema: {INGESTION_OPS_SCHEMA}")
print(f"Validation Schema: {VALIDATION_SCHEMA}")
print(f"Ingestion Config Table: {INGESTION_CONFIG_TABLE}")
print(f"Validation Mapping Table: {VALIDATION_MAPPING_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Batch Load ID: {BATCH_LOAD_ID}")
print(f"Run Reconciliation: {RUN_RECONCILIATION}")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Configuration from Metadata Tables

# COMMAND ----------

print("Fetching configuration from metadata tables...")

# Query serving_ingestion_config table
ingestion_query = f"""
    SELECT 
        target_catalog,
        target_schema,
        target_table,
        source_file_path,
        source_file_format,
        source_file_options,
        write_mode,
        partition_column
    FROM {INGESTION_CONFIG_TABLE}
    WHERE concat_ws('.', target_catalog, target_schema, target_table) = '{TARGET_TABLE}'
"""

ingestion_config = spark.sql(ingestion_query).collect()

if not ingestion_config:
    raise ValueError(f"Table '{TARGET_TABLE}' not found in {INGESTION_CONFIG_TABLE}")

ingestion_config = ingestion_config[0]

# Query validation_mapping table
validation_query = f"""
    SELECT 
        tgt_primary_keys,
        mismatch_exclude_fields
    FROM {VALIDATION_MAPPING_TABLE}
    WHERE tgt_table = '{TARGET_TABLE}'
"""

validation_config = spark.sql(validation_query).collect()

if not validation_config:
    print("WARNING: Table not found in validation_mapping, using defaults")
    primary_keys = []
    exclude_fields = []
else:
    validation_config = validation_config[0]
    # Parse primary keys (pipe-separated)
    primary_keys = [pk.strip() for pk in validation_config.tgt_primary_keys.split('|')] if validation_config.tgt_primary_keys else []
    # Parse exclude fields (comma-separated)
    exclude_fields = [f.strip() for f in validation_config.mismatch_exclude_fields.split(',') if validation_config.mismatch_exclude_fields] if validation_config.mismatch_exclude_fields else []

# Parse partition columns (comma-separated)
partition_columns = [pc.strip() for pc in ingestion_config.partition_column.split(',')] if ingestion_config.partition_column else []

# Parse source file options (JSON string to dict)
source_file_options = {}
if ingestion_config.source_file_options:
    try:
        source_file_options = json.loads(ingestion_config.source_file_options)
    except json.JSONDecodeError as e:
        print(f"WARNING: Failed to parse source_file_options: {e}. Will use empty options.")
        source_file_options = {}

print("\nConfiguration loaded successfully")
print("\n" + "="*70)
print("TABLE CONFIGURATION")
print("="*70)
print(f"Target Table: {TARGET_TABLE}")
print(f"Source File Path: {ingestion_config.source_file_path}")
print(f"Write Mode: {ingestion_config.write_mode}")
print(f"File Format: {ingestion_config.source_file_format}")
print(f"File Options: {source_file_options}")
print(f"Primary Keys: {primary_keys}")
print(f"Partition Columns: {partition_columns}")
print(f"Exclude Fields: {exclude_fields}")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find Source Files for Batch (Using Framework Logic)

# COMMAND ----------

print("Locating source files for batch using framework's file selection logic...")
print(f"File format: {ingestion_config.source_file_format}")

# Query the ingestion_metadata table to find files for this batch
# This matches how the framework (batch_processor) identifies files
from deltarecon.config import constants

file_query = f"""
    SELECT 
        source_file_path,
        batch_load_id
    FROM {constants.INGESTION_METADATA_TABLE}
    WHERE table_name = '{TARGET_TABLE}'
        AND batch_load_id = '{BATCH_LOAD_ID}'
"""

file_result = spark.sql(file_query)
file_rows = file_result.collect()

if not file_rows:
    raise ValueError(f"No source files found for batch {BATCH_LOAD_ID} in ingestion_metadata table")

# Extract source file paths
source_file_paths = [row.source_file_path for row in file_rows]

print(f"\nFound {len(source_file_paths)} source file(s) for batch {BATCH_LOAD_ID}:")
print("="*70)
for i, path in enumerate(source_file_paths, 1):
    print(f"  {i}. {path}")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Data (ORC/CSV/Text Files)

# COMMAND ----------

print(f"Loading source data files (format: {ingestion_config.source_file_format})...")

try:
    file_format = ingestion_config.source_file_format.lower()
    
    # Load based on format
    if file_format == "orc":
        print("  Loading ORC files...")
        source_df = spark.read.format("orc").load(source_file_paths)
        
    elif file_format == "csv":
        print("  Loading CSV files...")
        reader = spark.read.format("csv")
        
        # Apply CSV options
        if "header" in source_file_options:
            reader = reader.option("header", source_file_options["header"])
        if "sep" in source_file_options:
            reader = reader.option("sep", source_file_options["sep"])
        if "quote" in source_file_options:
            reader = reader.option("quote", source_file_options["quote"])
        if "escape" in source_file_options:
            reader = reader.option("escape", source_file_options["escape"])
        if "multiline" in source_file_options:
            reader = reader.option("multiline", source_file_options["multiline"])
        
        # Apply schema if provided
        if "schema" in source_file_options:
            from pyspark.sql.types import StructType
            try:
                schema = StructType.fromDDL(source_file_options["schema"])
                reader = reader.schema(schema)
                print(f"    Applied explicit schema from options")
            except Exception as e:
                print(f"    WARNING: Failed to parse schema: {e}. Will infer schema.")
        
        source_df = reader.load(source_file_paths)
        
    elif file_format == "text":
        print("  Loading Text files...")
        # Text files are just CSV with different separators
        reader = spark.read.format("csv")
        
        # Apply text/CSV options
        if "header" in source_file_options:
            reader = reader.option("header", source_file_options["header"])
        if "sep" in source_file_options:
            reader = reader.option("sep", source_file_options["sep"])
        if "quote" in source_file_options:
            reader = reader.option("quote", source_file_options["quote"])
        if "escape" in source_file_options:
            reader = reader.option("escape", source_file_options["escape"])
        if "multiline" in source_file_options:
            reader = reader.option("multiline", source_file_options["multiline"])
        
        # Apply schema if provided
        if "schema" in source_file_options:
            from pyspark.sql.types import StructType
            try:
                schema = StructType.fromDDL(source_file_options["schema"])
                reader = reader.schema(schema)
                print(f"    Applied explicit schema from options")
            except Exception as e:
                print(f"    WARNING: Failed to parse schema: {e}. Will infer schema.")
        
        source_df = reader.load(source_file_paths)
        
    else:
        raise ValueError(f"Unsupported file format: {file_format}. Supported: orc, csv, text")
    
    original_count = source_df.count()
    
    print(f"\nOriginal data loaded successfully")
    print(f"  Rows: {original_count:,}")
    print(f"  Columns: {len(source_df.columns)}")
    
    # Extract partition columns from file path if they don't exist in data
    if partition_columns:
        print(f"\n  Extracting partition columns: {partition_columns}")
        for part_col in partition_columns:
            if part_col not in source_df.columns:
                print(f"    Extracting partition column: {part_col}")
                # Extract from _metadata.file_path
                pattern = f"{part_col}=([^/]+)"
                source_df = source_df.withColumn(
                    part_col,
                    regexp_extract(col("_metadata.file_path"), pattern, 1)
                )
                
                # Try to cast to appropriate type (date, int, etc.)
                # Default to date for partition_date, string for others
                if "date" in part_col.lower():
                    source_df = source_df.withColumn(part_col, col(part_col).cast("date"))
        
        # Verify partition extraction
        final_count = source_df.count()
        if final_count != original_count:
            print(f"  WARNING: Partition extraction changed row count! Before: {original_count:,}, After: {final_count:,}")
        else:
            print(f"  Partition extraction verified: row count unchanged")
    
    source_df.cache()
    src_count = source_df.count()
    
    print(f"\nFinal source data:")
    print(f"  Rows: {src_count:,}")
    print(f"  Columns: {len(source_df.columns)}")
    
except Exception as e:
    print(f"ERROR loading source data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Target Data (Delta Table)

# COMMAND ----------

print("Loading target Delta table...")

try:
    # Read Delta table and filter by batch_load_id
    target_df = spark.read.format("delta").table(TARGET_TABLE)
    target_df = target_df.filter(col("_aud_batch_load_id") == BATCH_LOAD_ID)
    
    target_df.cache()
    tgt_count = target_df.count()
    
    print(f"\nTarget data loaded successfully")
    print(f"  Rows: {tgt_count:,}")
    print(f"  Columns: {len(target_df.columns)}")
    
except Exception as e:
    print(f"ERROR loading target data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Preview

# COMMAND ----------

print("="*70)
print("SOURCE DATA PREVIEW (first 5 rows)")
print("="*70)
# display(source_df.limit(5))

print("\n" + "="*70)
print("TARGET DATA PREVIEW (first 5 rows)")
print("="*70)
# display(target_df.limit(5))

# COMMAND ----------

print("SOURCE SCHEMA:")
print("-"*70)
for field in source_df.schema.fields:
    print(f"  {field.name:<40} {field.dataType.simpleString()}")

print("\n" + "="*70)
print("TARGET SCHEMA:")
print("-"*70)
for field in target_df.schema.fields:
    print(f"  {field.name:<40} {field.dataType.simpleString()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 1: Row Count Validation

# COMMAND ----------

print("="*70)
print("CHECK 1: ROW COUNT VALIDATION")
print("="*70)

src_count = source_df.count()
tgt_count = target_df.count()
difference = abs(src_count - tgt_count)
match = src_count == tgt_count

print(f"Source rows: {src_count:,}")
print(f"Target rows: {tgt_count:,}")
print(f"Difference: {difference:,}")
print(f"\nStatus: {'PASSED' if match else 'FAILED'}")

if not match:
    pct_diff = (difference / max(src_count, tgt_count)) * 100
    print(f"Percentage difference: {pct_diff:.2f}%")
    if src_count > tgt_count:
        print(f"Source has {difference:,} MORE rows than target")
    else:
        print(f"Target has {difference:,} MORE rows than source")

print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 2: Schema Validation

# COMMAND ----------

print("="*70)
print("CHECK 2: SCHEMA VALIDATION")
print("="*70)

# Build schema dictionaries
# Exclude partition columns from source (they were extracted from path)
# Exclude audit columns (_aud_*) from target
partition_cols_set = set(partition_columns)

source_schema = {}
for field in source_df.schema.fields:
    if field.name not in partition_cols_set:
        source_schema[field.name] = field.dataType.simpleString()

target_schema = {}
for field in target_df.schema.fields:
    if not field.name.startswith('_aud_') and field.name not in partition_cols_set:
        target_schema[field.name] = field.dataType.simpleString()

# Compare
source_cols = set(source_schema.keys())
target_cols = set(target_schema.keys())
common_cols = source_cols & target_cols
missing_in_target = source_cols - target_cols
extra_in_target = target_cols - source_cols

# Type mismatches
type_mismatches = []
for col_name in sorted(common_cols):
    if source_schema[col_name] != target_schema[col_name]:
        type_mismatches.append({
            'column': col_name,
            'source_type': source_schema[col_name],
            'target_type': target_schema[col_name]
        })

print(f"Source columns: {len(source_schema)} (excluding partition columns)")
print(f"Target columns: {len(target_schema)} (excluding audit/partition columns)")
print(f"Common columns: {len(common_cols)}")
print(f"Missing in target: {len(missing_in_target)}")
print(f"Extra in target: {len(extra_in_target)}")
print(f"Type mismatches: {len(type_mismatches)}")

has_issues = bool(missing_in_target or extra_in_target or type_mismatches)

if missing_in_target:
    print(f"\nMISSING IN TARGET ({len(missing_in_target)} columns):")
    for col_name in sorted(missing_in_target):
        print(f"  - {col_name} ({source_schema[col_name]})")

if extra_in_target:
    print(f"\nEXTRA IN TARGET ({len(extra_in_target)} columns):")
    for col_name in sorted(extra_in_target):
        print(f"  - {col_name} ({target_schema[col_name]})")

if type_mismatches:
    print(f"\nTYPE MISMATCHES ({len(type_mismatches)} columns):")
    for mismatch in type_mismatches:
        print(f"  - {mismatch['column']}:")
        print(f"      Source: {mismatch['source_type']}")
        print(f"      Target: {mismatch['target_type']}")

print(f"\nStatus: {'FAILED' if has_issues else 'PASSED'}")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 3: Primary Key Duplicates

# COMMAND ----------

print("="*70)
print("CHECK 3: PRIMARY KEY DUPLICATE CHECK")
print("="*70)

if not primary_keys:
    print("No primary keys configured - SKIPPED")
    src_duplicates = 0
    tgt_duplicates = 0
    print("="*70)
else:
    print(f"Primary key columns: {primary_keys}")
    
    # Verify PK columns exist in both dataframes
    src_missing_pks = set(primary_keys) - set(source_df.columns)
    tgt_missing_pks = set(primary_keys) - set(target_df.columns)
    
    if src_missing_pks or tgt_missing_pks:
        print(f"\nERROR: Primary key columns missing!")
        if src_missing_pks:
            print(f"  Missing in source: {src_missing_pks}")
        if tgt_missing_pks:
            print(f"  Missing in target: {tgt_missing_pks}")
        print("\nStatus: FAILED (configuration error)")
        src_duplicates = -1
        tgt_duplicates = -1
    else:
        # Check SOURCE for duplicates
        print(f"\nChecking SOURCE for duplicates...")
        src_total = source_df.count()
        src_distinct = source_df.select(primary_keys).distinct().count()
        src_duplicates = src_total - src_distinct
        
        print(f"  Total rows: {src_total:,}")
        print(f"  Distinct keys: {src_distinct:,}")
        print(f"  Duplicate rows: {src_duplicates:,}")
        print(f"  Status: {'PASSED' if src_duplicates == 0 else 'FAILED'}")
        
        if src_duplicates > 0:
            print(f"\n  Top 10 duplicate keys in SOURCE:")
            duplicate_keys = (
                source_df
                .groupBy(primary_keys)
                .agg(count("*").alias("duplicate_count"))
                .filter(col("duplicate_count") > 1)
                .orderBy(col("duplicate_count").desc())
                .limit(10)
            )
            display(duplicate_keys)
        
        # Check TARGET for duplicates
        print(f"\nChecking TARGET for duplicates...")
        tgt_total = target_df.count()
        tgt_distinct = target_df.select(primary_keys).distinct().count()
        tgt_duplicates = tgt_total - tgt_distinct
        
        print(f"  Total rows: {tgt_total:,}")
        print(f"  Distinct keys: {tgt_distinct:,}")
        print(f"  Duplicate rows: {tgt_duplicates:,}")
        print(f"  Status: {'PASSED' if tgt_duplicates == 0 else 'FAILED'}")
        
        if tgt_duplicates > 0:
            print(f"\n  Top 10 duplicate keys in TARGET:")
            duplicate_keys = (
                target_df
                .groupBy(primary_keys)
                .agg(count("*").alias("duplicate_count"))
                .filter(col("duplicate_count") > 1)
                .orderBy(col("duplicate_count").desc())
                .limit(10)
            )
            display(duplicate_keys)
        
        overall_pk_status = 'PASSED' if (src_duplicates == 0 and tgt_duplicates == 0) else 'FAILED'
        print(f"\nOverall PK Validation Status: {overall_pk_status}")

print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 4: Data Reconciliation (Optional)

# COMMAND ----------

if not RUN_RECONCILIATION:
    print("="*70)
    print("CHECK 4: DATA RECONCILIATION")
    print("="*70)
    print("SKIPPED (user selected N)")
    print("="*70)
    src_extras = 0
    tgt_extras = 0
    matches = 0
else:
    print("="*70)
    print("CHECK 4: DATA RECONCILIATION (Full Row Hash)")
    print("="*70)
    
    # Determine columns to include in hash
    # Exclude: partition columns (extracted from path) and audit columns
    partition_cols_set = set(partition_columns)
    
    src_cols_to_hash = [c for c in source_df.columns if c not in partition_cols_set]
    tgt_cols_to_hash = [c for c in target_df.columns if not c.startswith('_aud_') and c not in partition_cols_set]
    
    print(f"Columns in hash (source): {len(src_cols_to_hash)}")
    print(f"Columns in hash (target): {len(tgt_cols_to_hash)}")
    print(f"Excluded from hash: partition columns {partition_columns}, audit columns (_aud_*)")
    
    # Sort columns for consistent hashing
    src_cols_sorted = sorted(src_cols_to_hash)
    tgt_cols_sorted = sorted(tgt_cols_to_hash)
    
    print(f"\nComputing row hashes...")
    
    # Create hash for each row
    # Convert all columns to string and concatenate with separator, then hash
    source_with_hash = source_df.withColumn(
        "row_hash",
        md5(concat_ws("||", *[col(c).cast("string") for c in src_cols_sorted]))
    ).select("row_hash")
    
    target_with_hash = target_df.withColumn(
        "row_hash",
        md5(concat_ws("||", *[col(c).cast("string") for c in tgt_cols_sorted]))
    ).select("row_hash")
    
    # Get distinct hashes
    src_hashes = source_with_hash.distinct()
    tgt_hashes = target_with_hash.distinct()
    
    src_distinct = src_hashes.count()
    tgt_distinct = tgt_hashes.count()
    
    print(f"Source distinct rows: {src_distinct:,}")
    print(f"Target distinct rows: {tgt_distinct:,}")
    
    # Compute set differences
    print(f"\nComputing set differences...")
    src_extras = src_hashes.subtract(tgt_hashes).count()
    tgt_extras = tgt_hashes.subtract(src_hashes).count()
    matches = src_distinct - src_extras
    
    print(f"\nResults:")
    print(f"  Matches: {matches:,}")
    print(f"  Source extras (not in target): {src_extras:,}")
    print(f"  Target extras (not in source): {tgt_extras:,}")
    
    if matches > 0:
        match_pct = (matches / max(src_distinct, tgt_distinct)) * 100
        print(f"  Match rate: {match_pct:.2f}%")
    
    recon_status = 'PASSED' if (src_extras == 0 and tgt_extras == 0) else 'FAILED'
    print(f"\nStatus: {recon_status}")
    print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

# Calculate overall status
checks_run = []
checks_passed = []

# Row count
checks_run.append("Row Count")
if src_count == tgt_count:
    checks_passed.append("Row Count")
row_count_status = "PASSED" if src_count == tgt_count else "FAILED"

# Schema
checks_run.append("Schema")
schema_passed = not (missing_in_target or extra_in_target or type_mismatches)
if schema_passed:
    checks_passed.append("Schema")
schema_status = "PASSED" if schema_passed else "FAILED"

# PK duplicates
pk_status = "SKIPPED"
if primary_keys:
    checks_run.append("PK Duplicates")
    if src_duplicates >= 0 and tgt_duplicates >= 0:
        pk_passed = (src_duplicates == 0 and tgt_duplicates == 0)
        if pk_passed:
            checks_passed.append("PK Duplicates")
        pk_status = "PASSED" if pk_passed else "FAILED"
    else:
        pk_status = "FAILED"

# Data reconciliation
recon_status = "SKIPPED"
if RUN_RECONCILIATION:
    checks_run.append("Data Reconciliation")
    if src_extras == 0 and tgt_extras == 0:
        checks_passed.append("Data Reconciliation")
        recon_status = "PASSED"
    else:
        recon_status = "FAILED"

print("\n" + "="*70)
print("VALIDATION SUMMARY REPORT")
print("="*70)
print(f"Table: {TARGET_TABLE}")
print(f"Batch: {BATCH_LOAD_ID}")
print(f"Date: {spark.sql('SELECT current_timestamp()').collect()[0][0]}")
print("="*70)

print("\nCHECK RESULTS:")
print(f"  1. Row Count:           {row_count_status}")
print(f"  2. Schema:              {schema_status}")
print(f"  3. PK Duplicates:       {pk_status}")
print(f"  4. Data Reconciliation: {recon_status}")

print(f"\nOVERALL: {len(checks_passed)} of {len(checks_run)} checks passed")

print("\nKEY METRICS:")
print(f"  Source rows: {src_count:,}")
print(f"  Target rows: {tgt_count:,}")
print(f"  Row difference: {abs(src_count - tgt_count):,}")

if primary_keys and src_duplicates >= 0:
    print(f"  Source PK duplicates: {src_duplicates:,}")
    print(f"  Target PK duplicates: {tgt_duplicates:,}")

if RUN_RECONCILIATION:
    print(f"  Matching rows (hash): {matches:,}")
    print(f"  Source extras: {src_extras:,}")
    print(f"  Target extras: {tgt_extras:,}")

print("="*70)

# Cleanup cache
source_df.unpersist()
target_df.unpersist()
print("\nCache cleared")
