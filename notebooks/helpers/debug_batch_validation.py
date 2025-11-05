# Databricks notebook source
# MAGIC %md
# MAGIC # Manual Batch Validation Debugger
# MAGIC 
# MAGIC **Purpose:** Debug validation failures for a specific table and batch
# MAGIC 
# MAGIC **How to use:**
# MAGIC 1. Fill in the widgets at the top
# MAGIC 2. Run all cells
# MAGIC 3. Review each validation check output

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup & Imports

# COMMAND ----------

import sys
sys.path.append('/Workspace/Shared/serving_gold_ingestion/dbx_validation_framework')

from pyspark.sql import functions as F
from pyspark.sql.functions import col, md5, concat_ws, count, lit, regexp_extract
from deltarecon.config import constants
from deltarecon.core.ingestion_reader import IngestionConfigReader
from deltarecon.core.source_target_loader import SourceTargetLoader
from deltarecon.core.batch_processor import BatchProcessor
from deltarecon.utils.logger import get_logger

logger = get_logger(__name__)

print("Imports successful")
print("Framework location: /Workspace/Shared/serving_gold_ingestion/dbx_validation_framework")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input Parameters (Widgets)

# COMMAND ----------

# Create widgets
dbutils.widgets.removeAll()

dbutils.widgets.text("target_table", "prd_connectivity.home_gold.home_btas_error_kpi_po", "1. Target Table")
dbutils.widgets.text("batch_load_id", "202510210435", "2. Batch Load ID")
dbutils.widgets.text("table_group", "home_gold_10am_daily_job", "3. Table Group")
dbutils.widgets.dropdown("run_reconciliation", "Y", ["Y", "N"], "4. Run Data Reconciliation?")

# Get values
TARGET_TABLE = dbutils.widgets.get("target_table")
BATCH_LOAD_ID = dbutils.widgets.get("batch_load_id")
TABLE_GROUP = dbutils.widgets.get("table_group")
RUN_RECONCILIATION = dbutils.widgets.get("run_reconciliation") == "Y"

print("="*70)
print("INPUT PARAMETERS")
print("="*70)
print(f"Target Table: {TARGET_TABLE}")
print(f"Batch Load ID: {BATCH_LOAD_ID}")
print(f"Table Group: {TABLE_GROUP}")
print(f"Run Reconciliation: {RUN_RECONCILIATION}")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Table Configuration

# COMMAND ----------

print("Fetching table configuration...")

# Get all configs for the table group
reader = IngestionConfigReader(spark, TABLE_GROUP)
all_configs = reader.get_tables_in_group()

# Find the specific table
config = None
for cfg in all_configs:
    if cfg.table_name == TARGET_TABLE:
        config = cfg
        break

if config is None:
    raise ValueError(f"Table '{TARGET_TABLE}' not found in group '{TABLE_GROUP}'")

print("\nConfiguration loaded successfully")
print("\n" + "="*70)
print("TABLE CONFIGURATION")
print("="*70)
print(f"Table Name: {config.table_name}")
print(f"Source Table: {config.source_table}")
print(f"Write Mode: {config.write_mode}")
print(f"Primary Keys: {config.primary_keys}")
print(f"Partition Columns: {config.partition_columns}")
print(f"Partition Types: {config.partition_datatypes}")
print(f"Exclude Fields: {config.mismatch_exclude_fields}")
print(f"Is Active: {config.is_active}")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Batch Exists in Source

# COMMAND ----------

print("Checking if batch exists in source...")

batch_processor = BatchProcessor(spark)
unprocessed_batches = batch_processor.get_unprocessed_batches(config)

if BATCH_LOAD_ID not in unprocessed_batches:
    print(f"\nWARNING: Batch '{BATCH_LOAD_ID}' not found in unprocessed batches")
    print(f"This may be expected if the batch was already processed")
    print(f"\nAvailable unprocessed batches ({len(unprocessed_batches)}):")
    for batch_id in sorted(unprocessed_batches.keys())[:10]:
        print(f"  - {batch_id}")
    if len(unprocessed_batches) > 10:
        print(f"  ... and {len(unprocessed_batches) - 10} more")
else:
    batch_info = unprocessed_batches[BATCH_LOAD_ID]
    print(f"\nBatch found in source")
    print(f"  ORC Files: {len(batch_info['orc_files'])}")
    print(f"  Sample path: {batch_info['orc_files'][0] if batch_info['orc_files'] else 'N/A'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source & Target Data

# COMMAND ----------

print("Loading source and target data...")

loader = SourceTargetLoader(spark)

try:
    source_df, target_df = loader.load_batch(BATCH_LOAD_ID, config)
    
    # Cache for performance
    source_df.cache()
    target_df.cache()
    
    src_count = source_df.count()
    tgt_count = target_df.count()
    
    print("\nData loaded and cached")
    print(f"  Source rows: {src_count:,}")
    print(f"  Target rows: {tgt_count:,}")
    print(f"  Source columns: {len(source_df.columns)}")
    print(f"  Target columns: {len(target_df.columns)}")
    
except Exception as e:
    print(f"\nERROR loading data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Preview

# COMMAND ----------

print("="*70)
print("SOURCE DATA PREVIEW (first 5 rows)")
print("="*70)
display(source_df.limit(5))

print("\n" + "="*70)
print("TARGET DATA PREVIEW (first 5 rows)")
print("="*70)
display(target_df.limit(5))

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

# Exclude partition columns from source (they're extracted, not in original data)
# Exclude audit columns from target
partition_cols = set(config.partition_columns) if config.partition_columns else set()

source_schema = {
    f.name: f.dataType.simpleString() 
    for f in source_df.schema.fields 
    if f.name not in partition_cols
}

target_schema = {
    f.name: f.dataType.simpleString() 
    for f in target_df.schema.fields 
    if not f.name.startswith('_aud_') and f.name not in partition_cols
}

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

if not config.primary_keys:
    print("No primary keys defined - SKIPPED")
    print("="*70)
else:
    pk_cols = config.primary_keys
    print(f"Primary key columns: {pk_cols}")
    
    # Verify PK columns exist
    src_missing_pks = set(pk_cols) - set(source_df.columns)
    tgt_missing_pks = set(pk_cols) - set(target_df.columns)
    
    if src_missing_pks or tgt_missing_pks:
        print(f"\nERROR: Primary key columns missing!")
        if src_missing_pks:
            print(f"  Missing in source: {src_missing_pks}")
        if tgt_missing_pks:
            print(f"  Missing in target: {tgt_missing_pks}")
        print("\nStatus: FAILED (configuration error)")
    else:
        # Check source
        print(f"\nChecking SOURCE for duplicates...")
        src_total = source_df.count()
        src_distinct = source_df.select(pk_cols).distinct().count()
        src_duplicates = src_total - src_distinct
        
        print(f"  Total rows: {src_total:,}")
        print(f"  Distinct keys: {src_distinct:,}")
        print(f"  Duplicate rows: {src_duplicates:,}")
        print(f"  Status: {'PASSED' if src_duplicates == 0 else 'FAILED'}")
        
        if src_duplicates > 0:
            print(f"\n  Top 10 duplicate keys in SOURCE:")
            duplicate_keys = (
                source_df
                .groupBy(pk_cols)
                .agg(count("*").alias("duplicate_count"))
                .filter(col("duplicate_count") > 1)
                .orderBy(col("duplicate_count").desc())
                .limit(10)
            )
            display(duplicate_keys)
        
        # Check target
        print(f"\nChecking TARGET for duplicates...")
        tgt_total = target_df.count()
        tgt_distinct = target_df.select(pk_cols).distinct().count()
        tgt_duplicates = tgt_total - tgt_distinct
        
        print(f"  Total rows: {tgt_total:,}")
        print(f"  Distinct keys: {tgt_distinct:,}")
        print(f"  Duplicate rows: {tgt_duplicates:,}")
        print(f"  Status: {'PASSED' if tgt_duplicates == 0 else 'FAILED'}")
        
        if tgt_duplicates > 0:
            print(f"\n  Top 10 duplicate keys in TARGET:")
            duplicate_keys = (
                target_df
                .groupBy(pk_cols)
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
else:
    print("="*70)
    print("CHECK 4: DATA RECONCILIATION (Full Row Hash)")
    print("="*70)
    
    # Determine columns to hash
    partition_cols = set(config.partition_columns) if config.partition_columns else set()
    exclude_from_hash = partition_cols | {"_aud_batch_load_id"}
    
    src_cols_to_hash = [c for c in source_df.columns if c not in exclude_from_hash]
    tgt_cols_to_hash = [c for c in target_df.columns if not c.startswith('_aud_') and c not in partition_cols]
    
    print(f"Columns in hash (source): {len(src_cols_to_hash)}")
    print(f"Columns in hash (target): {len(tgt_cols_to_hash)}")
    print(f"Excluded from hash: {exclude_from_hash}")
    
    # Sort columns for consistent hashing
    src_cols_sorted = sorted(src_cols_to_hash)
    tgt_cols_sorted = sorted(tgt_cols_to_hash)
    
    print(f"\nComputing row hashes...")
    
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
    
    # Compute differences
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
# MAGIC ## Deep Dive: Sample Mismatches

# COMMAND ----------

if not RUN_RECONCILIATION:
    print("Data reconciliation was not run - skipping mismatch analysis")
elif src_extras == 0 and tgt_extras == 0:
    print("No mismatches found - all rows match perfectly")
else:
    print("="*70)
    print("DEEP DIVE: SAMPLE MISMATCHES")
    print("="*70)
    
    # Recreate full hashes
    source_full_hash = source_df.withColumn(
        "row_hash",
        md5(concat_ws("||", *[col(c).cast("string") for c in src_cols_sorted]))
    )
    
    target_full_hash = target_df.withColumn(
        "row_hash",
        md5(concat_ws("||", *[col(c).cast("string") for c in tgt_cols_sorted]))
    )
    
    if src_extras > 0:
        print(f"\nSample rows in SOURCE but NOT in TARGET (showing 10 of {src_extras:,}):")
        print("-"*70)
        src_extra_hashes = src_hashes.subtract(tgt_hashes).limit(10)
        src_extra_rows = source_full_hash.join(src_extra_hashes, "row_hash", "inner").drop("row_hash")
        display(src_extra_rows)
    
    if tgt_extras > 0:
        print(f"\nSample rows in TARGET but NOT in SOURCE (showing 10 of {tgt_extras:,}):")
        print("-"*70)
        tgt_extra_hashes = tgt_hashes.subtract(src_hashes).limit(10)
        tgt_extra_rows = target_full_hash.join(tgt_extra_hashes, "row_hash", "inner").drop("row_hash")
        display(tgt_extra_rows)
    
    # If primary keys exist, find matching keys with different values
    if config.primary_keys and src_extras > 0 and tgt_extras > 0:
        print(f"\nSearching for rows with SAME primary key but DIFFERENT values...")
        print("-"*70)
        pk_cols = config.primary_keys
        
        # Get PKs from extras
        src_pks = source_full_hash.join(
            src_hashes.subtract(tgt_hashes), "row_hash", "inner"
        ).select(pk_cols).distinct()
        
        tgt_pks = target_full_hash.join(
            tgt_hashes.subtract(src_hashes), "row_hash", "inner"
        ).select(pk_cols).distinct()
        
        common_pks = src_pks.intersect(tgt_pks).limit(5)
        common_pk_count = common_pks.count()
        
        if common_pk_count > 0:
            print(f"Found {common_pk_count} primary keys with value differences")
            print(f"Showing first 5 examples:\n")
            
            for idx, pk_row in enumerate(common_pks.collect(), 1):
                pk_dict = pk_row.asDict()
                print(f"\nExample {idx}:")
                print(f"Primary Key: {pk_dict}")
                print("-"*70)
                
                # Build filter condition
                filter_conditions = [col(k) == pk_dict[k] for k in pk_cols]
                combined_filter = filter_conditions[0]
                for condition in filter_conditions[1:]:
                    combined_filter = combined_filter & condition
                
                print("SOURCE row:")
                display(source_df.filter(combined_filter))
                
                print("TARGET row:")
                display(target_df.filter(combined_filter))
        else:
            print("No common primary keys found")
            print("The extras are completely different rows (different PKs)")
    
    print("\n" + "="*70)

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
if config.primary_keys:
    checks_run.append("PK Duplicates")
    if not (src_missing_pks or tgt_missing_pks):
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
print(f"  1. Row Count:          {row_count_status}")
print(f"  2. Schema:             {schema_status}")
print(f"  3. PK Duplicates:      {pk_status}")
print(f"  4. Data Reconciliation: {recon_status}")

print(f"\nOVERALL: {len(checks_passed)} of {len(checks_run)} checks passed")

print("\nKEY METRICS:")
print(f"  Source rows: {src_count:,}")
print(f"  Target rows: {tgt_count:,}")
print(f"  Row difference: {abs(src_count - tgt_count):,}")

if config.primary_keys and not (src_missing_pks or tgt_missing_pks):
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

