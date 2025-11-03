# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Validation Helper
# MAGIC 
# MAGIC This notebook provides a **simple, direct way** to manually validate a specific batch:
# MAGIC 
# MAGIC ### Inputs:
# MAGIC - `batch_load_id`: The batch to validate
# MAGIC - `target_table`: Target table in format `catalog.schema.table`
# MAGIC 
# MAGIC ### Validations Performed:
# MAGIC 1. **Row Count**: Source vs Target row counts
# MAGIC 2. **Schema**: Column names and data types comparison
# MAGIC 3. **Primary Key**: Duplicate detection in source and target
# MAGIC 4. **Data Reconciliation**: Full row hash comparison
# MAGIC 
# MAGIC Each validation runs in its own cell with clear pass/fail indicators.

# COMMAND ----------

# DBTITLE 1,Import Libraries and Configuration
import sys
import logging
from pyspark.sql.functions import col, count, regexp_extract, md5, concat_ws, coalesce, lit
from pyspark.sql.types import StructType

# Add project root to Python path for imports
import os
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
project_root = os.path.abspath(os.path.join(os.path.dirname(notebook_path), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import constants from deltarecon config
# NOTE: Modify the import below to use your constants file (constants.py or constants.py)
try:
    from deltarecon.config.constants import (
        INGESTION_METADATA_TABLE,
        INGESTION_AUDIT_TABLE,
        INGESTION_CONFIG_TABLE,
        VALIDATION_MAPPING_TABLE
    )
    print("✅ Loaded constants from: deltarecon.config.constants")
except ImportError:
    # Fallback to default constants.py
    from deltarecon.config.constants import (
        INGESTION_METADATA_TABLE,
        INGESTION_AUDIT_TABLE,
        INGESTION_CONFIG_TABLE,
        VALIDATION_MAPPING_TABLE
    )
    print("✅ Loaded constants from: deltarecon.config.constants")

# Setup logging
logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

print(f"\n📋 Configuration loaded:")
print(f"   INGESTION_METADATA_TABLE: {INGESTION_METADATA_TABLE}")
print(f"   INGESTION_AUDIT_TABLE:    {INGESTION_AUDIT_TABLE}")
print(f"   INGESTION_CONFIG_TABLE:   {INGESTION_CONFIG_TABLE}")
print(f"   VALIDATION_MAPPING_TABLE: {VALIDATION_MAPPING_TABLE}")

# COMMAND ----------

# DBTITLE 1,Input Parameters
# Create widgets for batch_load_id and target_table
dbutils.widgets.text("batch_load_id", "")
dbutils.widgets.text("target_table", "")

# Get values
batch_load_id = dbutils.widgets.get("batch_load_id").strip()
target_table = dbutils.widgets.get("target_table").strip()

print("="*80)
print("MANUAL VALIDATION PARAMETERS")
print("="*80)
print(f"Batch Load ID: {batch_load_id}")
print(f"Target Table:  {target_table}")
print("="*80)

# Validate inputs
if not batch_load_id:
    raise ValueError("❌ batch_load_id is required!")
if not target_table:
    raise ValueError("❌ target_table is required!")

# COMMAND ----------

# DBTITLE 1,Fetch Batch Metadata
print("\n" + "="*80)
print("FETCHING BATCH METADATA")
print("="*80)

# Get source file paths for this batch
source_paths_query = f"""
    SELECT DISTINCT
        t1.source_file_path,
        t1.batch_load_id,
        t1.table_name,
        t2.status as ingestion_status
    FROM {INGESTION_METADATA_TABLE} t1
    INNER JOIN {INGESTION_AUDIT_TABLE} t2
        ON (t1.table_name = t2.target_table_name AND t1.batch_load_id = t2.batch_load_id)
    WHERE t1.batch_load_id = '{batch_load_id}'
        AND t1.table_name = '{target_table}'
        AND t2.status = 'COMPLETED'
    ORDER BY t1.source_file_path
"""

source_paths_df = spark.sql(source_paths_query)
source_paths_count = source_paths_df.count()

if source_paths_count == 0:
    raise ValueError(f"❌ No source files found for batch_load_id='{batch_load_id}' and target_table='{target_table}'")

source_paths_list = [row.source_file_path for row in source_paths_df.collect()]

print(f"\n✅ Found {source_paths_count} source ORC file(s)")
print(f"\nFirst 3 files:")
for idx, path in enumerate(source_paths_list[:3], 1):
    print(f"  {idx}. {path}")

if len(source_paths_list) > 3:
    print(f"  ... and {len(source_paths_list) - 3} more files")

# Display full list (commented out by default)
# print("\nFull source file list:")
# display(source_paths_df)

# COMMAND ----------

# DBTITLE 1,Get Partition Configuration
print("\n" + "="*80)
print("PARTITION CONFIGURATION")
print("="*80)

# Get partition columns from ingestion config
partition_info_query = f"""
    SELECT 
        partition_column,
        target_catalog,
        target_schema,
        target_table,
        write_mode
    FROM {INGESTION_CONFIG_TABLE}
    WHERE concat_ws('.', target_catalog, target_schema, target_table) = '{target_table}'
    LIMIT 1
"""

partition_info_df = spark.sql(partition_info_query)

if partition_info_df.count() > 0:
    partition_info = partition_info_df.first()
    partition_columns = partition_info.partition_column.split(',') if partition_info.partition_column else []
    write_mode = partition_info.write_mode
    
    print(f"Partition Columns: {partition_columns if partition_columns else 'None'}")
    print(f"Write Mode: {write_mode}")
else:
    partition_columns = []
    write_mode = "unknown"
    print("⚠️  No partition information found")

# COMMAND ----------

# DBTITLE 1,Read Source ORC Files
print("\n" + "="*80)
print("READING SOURCE ORC FILES")
print("="*80)

# Read ORC files
print(f"\nReading {len(source_paths_list)} ORC file(s)...")
df_source_orc = spark.read.format("orc").load(source_paths_list)

# Add partition columns if needed (extracted from file paths)
if partition_columns and len(partition_columns) > 0:
    print(f"Extracting partition columns: {partition_columns}")
    
    df_source = df_source_orc
    
    for partition_col in partition_columns:
        if partition_col not in df_source.columns:
            # Extract partition value from file path
            # Pattern: /path/partition_col=value/file.orc
            pattern = f"{partition_col}=([^/]+)"
            df_source = df_source.withColumn(
                partition_col,
                regexp_extract(col("_metadata.file_path"), pattern, 1)
            )
else:
    df_source = df_source_orc

source_row_count = df_source.count()

print(f"\n✅ Successfully read source ORC files")
print(f"   Total rows:    {source_row_count:,}")
print(f"   Total columns: {len(df_source.columns)}")

print("\nSource Schema:")
df_source.printSchema()

# Display sample data (commented out by default)
# print("\nSample data (first 5 rows):")
# display(df_source.limit(5))

# COMMAND ----------

# DBTITLE 1,Read Target Delta Table
print("\n" + "="*80)
print("READING TARGET DELTA TABLE")
print("="*80)

# Read target table with batch_load_id filter
print(f"\nReading target table: {target_table}")
print(f"Filtering by: _aud_batch_load_id = '{batch_load_id}'")

df_target = spark.table(target_table).filter(f"_aud_batch_load_id = '{batch_load_id}'")

target_row_count = df_target.count()

print(f"\n✅ Successfully read target Delta table")
print(f"   Total rows:    {target_row_count:,}")
print(f"   Total columns: {len(df_target.columns)}")

print("\nTarget Schema:")
df_target.printSchema()

# Display sample data (commented out by default)
# print("\nSample data (first 5 rows):")
# display(df_target.limit(5))

# COMMAND ----------

# DBTITLE 1,Validation 1: Row Count
print("\n" + "="*80)
print("VALIDATION 1: ROW COUNT")
print("="*80)

print(f"\nSource rows: {source_row_count:,}")
print(f"Target rows: {target_row_count:,}")

if source_row_count == target_row_count:
    print(f"\n✅ PASSED - Row counts match!")
else:
    difference = abs(source_row_count - target_row_count)
    percentage_diff = (difference / max(source_row_count, target_row_count) * 100) if max(source_row_count, target_row_count) > 0 else 0
    print(f"\n❌ FAILED - Row count mismatch!")
    print(f"   Difference: {difference:,} rows ({percentage_diff:.2f}%)")

print("\n" + "="*80)

# COMMAND ----------

# DBTITLE 1,Validation 2: Schema Comparison
print("\n" + "="*80)
print("VALIDATION 2: SCHEMA COMPARISON")
print("="*80)

# Get column names (exclude partition columns from source, audit columns from target)
partition_cols_set = set(partition_columns) if partition_columns else set()
source_cols = set([f.name for f in df_source.schema.fields if f.name not in partition_cols_set])
target_cols = set([f.name for f in df_target.schema.fields if not f.name.startswith('_aud_')])

print(f"\nSource columns: {len(source_cols)} (excluding partition columns)")
print(f"Target columns: {len(target_cols)} (excluding _aud_* columns)")

# Compare column names
common_cols = source_cols & target_cols
missing_in_target = source_cols - target_cols
extra_in_target = target_cols - source_cols

print(f"Common columns: {len(common_cols)}")

schema_issues = []

if missing_in_target:
    print(f"\n❌ Missing in target ({len(missing_in_target)}): {sorted(missing_in_target)}")
    schema_issues.append("missing_columns")

if extra_in_target:
    print(f"\n⚠️  Extra in target ({len(extra_in_target)}): {sorted(extra_in_target)}")
    schema_issues.append("extra_columns")

# Compare data types for common columns
print(f"\n🔍 Checking data types for {len(common_cols)} common columns...")

source_schema_map = {f.name: f.dataType.simpleString() for f in df_source.schema.fields}
target_schema_map = {f.name: f.dataType.simpleString() for f in df_target.schema.fields}

type_mismatches = []
for col_name in sorted(common_cols):
    src_type = source_schema_map[col_name]
    tgt_type = target_schema_map[col_name]
    
    if src_type != tgt_type:
        type_mismatches.append({
            'column': col_name,
            'source_type': src_type,
            'target_type': tgt_type
        })

if type_mismatches:
    print(f"\n❌ Type mismatches found ({len(type_mismatches)}):")
    for mismatch in type_mismatches[:10]:
        print(f"   • {mismatch['column']}: {mismatch['source_type']} (source) vs {mismatch['target_type']} (target)")
    
    if len(type_mismatches) > 10:
        print(f"   ... and {len(type_mismatches) - 10} more mismatches")
    
    schema_issues.append("type_mismatches")
else:
    print("   ✅ All data types match")

# Summary
print("\n" + "-"*80)
if len(schema_issues) == 0:
    print("✅ PASSED - Schema matches perfectly")
else:
    print(f"❌ FAILED - {len(schema_issues)} schema issue(s) found")
print("-"*80)

print("\n" + "="*80)

# Display detailed type mismatches (commented out by default)
# if type_mismatches:
#     print("\nDetailed type mismatches:")
#     display(spark.createDataFrame(type_mismatches))

# COMMAND ----------

# DBTITLE 1,Validation 3: Primary Key Uniqueness
print("\n" + "="*80)
print("VALIDATION 3: PRIMARY KEY UNIQUENESS")
print("="*80)

# Get primary keys from validation mapping table
# Note: We need to derive workflow_name and table_family from target_table
# For simplicity, try to find any matching configuration

pk_query = f"""
    SELECT DISTINCT tgt_primary_keys, workflow_name, table_family
    FROM {VALIDATION_MAPPING_TABLE}
    WHERE concat_ws('.', tgt_warehouse_name, tgt_database_name, tgt_table_name) = '{target_table}'
    LIMIT 1
"""

pk_result_df = spark.sql(pk_query)

if pk_result_df.count() == 0:
    print("\n⚠️  No primary key configuration found")
    print("   Skipping primary key validation")
    primary_keys = []
else:
    pk_result = pk_result_df.first()
    
    if pk_result.tgt_primary_keys:
        # Primary keys are pipe-separated
        primary_keys = [pk.strip() for pk in pk_result.tgt_primary_keys.split('|')]
        
        print(f"\nPrimary Keys: {primary_keys}")
        print(f"(from workflow: {pk_result.workflow_name}, table_family: {pk_result.table_family})")
        
        # Check if PK columns exist in both dataframes
        missing_in_source = set(primary_keys) - set(df_source.columns)
        missing_in_target = set(primary_keys) - set(df_target.columns)
        
        if missing_in_source or missing_in_target:
            print(f"\n❌ Primary key columns missing!")
            if missing_in_source:
                print(f"   Missing in source: {missing_in_source}")
            if missing_in_target:
                print(f"   Missing in target: {missing_in_target}")
        else:
            # Check for duplicates in source
            print(f"\n🔍 Checking SOURCE for duplicate primary keys...")
            src_distinct_keys = df_source.select(primary_keys).distinct().count()
            src_duplicates = source_row_count - src_distinct_keys
            
            print(f"   Total rows:          {source_row_count:,}")
            print(f"   Distinct key combos: {src_distinct_keys:,}")
            print(f"   Duplicate rows:      {src_duplicates:,}")
            
            if src_duplicates == 0:
                print("   Status: ✅ NO DUPLICATES")
            else:
                print("   Status: ❌ DUPLICATES FOUND")
            
            # Check for duplicates in target
            print(f"\n🔍 Checking TARGET for duplicate primary keys...")
            tgt_distinct_keys = df_target.select(primary_keys).distinct().count()
            tgt_duplicates = target_row_count - tgt_distinct_keys
            
            print(f"   Total rows:          {target_row_count:,}")
            print(f"   Distinct key combos: {tgt_distinct_keys:,}")
            print(f"   Duplicate rows:      {tgt_duplicates:,}")
            
            if tgt_duplicates == 0:
                print("   Status: ✅ NO DUPLICATES")
            else:
                print("   Status: ❌ DUPLICATES FOUND")
            
            # Display duplicate examples (commented out by default)
            # if src_duplicates > 0:
            #     print("\nTop 10 duplicate keys in SOURCE:")
            #     duplicate_keys_source = df_source.groupBy(primary_keys).agg(
            #         count("*").alias("count")
            #     ).filter("count > 1").orderBy(col("count").desc())
            #     display(duplicate_keys_source.limit(10))
            
            # if tgt_duplicates > 0:
            #     print("\nTop 10 duplicate keys in TARGET:")
            #     duplicate_keys_target = df_target.groupBy(primary_keys).agg(
            #         count("*").alias("count")
            #     ).filter("count > 1").orderBy(col("count").desc())
            #     display(duplicate_keys_target.limit(10))
            
            # Summary
            print("\n" + "-"*80)
            if src_duplicates == 0 and tgt_duplicates == 0:
                print("✅ PASSED - No primary key violations")
            else:
                print("❌ FAILED - Primary key violations detected")
                if src_duplicates > 0:
                    print(f"   • Source has {src_duplicates:,} duplicate rows")
                if tgt_duplicates > 0:
                    print(f"   • Target has {tgt_duplicates:,} duplicate rows")
            print("-"*80)
    else:
        print("\n⚠️  No primary keys defined for this table")
        print("   Skipping primary key validation")
        primary_keys = []

print("\n" + "="*80)

# COMMAND ----------

# DBTITLE 1,Validation 4: Data Reconciliation (Full Row Hash)
print("\n" + "="*80)
print("VALIDATION 4: DATA RECONCILIATION")
print("="*80)

print("\nUsing full-row hash comparison...")
print("(Excludes partition and audit columns from hash)")

# Determine columns to include in hash
partition_cols_set = set(partition_columns) if partition_columns else set()
audit_cols = set([c for c in df_target.columns if c.startswith('_aud_')])
exclude_cols = partition_cols_set | audit_cols

# Get common data columns only (exclude partition and audit columns)
source_data_cols = sorted([c for c in df_source.columns if c not in partition_cols_set])
target_data_cols = sorted([c for c in df_target.columns if c not in audit_cols])

# Use only common columns for hash
common_data_cols = sorted(set(source_data_cols) & set(target_data_cols))

print(f"\nColumns in hash calculation: {len(common_data_cols)}")

# Create row hash for source
print("\n🔍 Creating source row hashes...")
source_hash_expr = md5(concat_ws("||", *[
    coalesce(col(c).cast("string"), lit("__NULL__")) 
    for c in common_data_cols
]))
df_source_hashed = df_source.select(source_hash_expr.alias("_row_hash")).distinct()
df_source_hashed.cache()
src_distinct_rows = df_source_hashed.count()

# Create row hash for target
print("🔍 Creating target row hashes...")
target_hash_expr = md5(concat_ws("||", *[
    coalesce(col(c).cast("string"), lit("__NULL__")) 
    for c in common_data_cols
]))
df_target_hashed = df_target.select(target_hash_expr.alias("_row_hash")).distinct()
df_target_hashed.cache()
tgt_distinct_rows = df_target_hashed.count()

print(f"\nSource distinct rows: {src_distinct_rows:,}")
print(f"Target distinct rows: {tgt_distinct_rows:,}")

# Compute set differences
print("\n🔍 Computing differences...")

# Rows in source but NOT in target
src_minus_tgt = df_source_hashed.exceptAll(df_target_hashed)
src_extras = src_minus_tgt.count()

# Rows in target but NOT in source
tgt_minus_src = df_target_hashed.exceptAll(df_source_hashed)
tgt_extras = tgt_minus_src.count()

# Matches: rows that exist in both
matches = src_distinct_rows - src_extras

# Clean up
df_source_hashed.unpersist()
df_target_hashed.unpersist()

print(f"\n📊 Reconciliation Results:")
print(f"   Matches (in both):     {matches:,}")
print(f"   Source extras:         {src_extras:,}")
print(f"   Target extras:         {tgt_extras:,}")

# Summary
print("\n" + "-"*80)
if src_extras == 0 and tgt_extras == 0:
    print("✅ PASSED - All rows match perfectly")
else:
    print("❌ FAILED - Data discrepancies found")
    if src_extras > 0:
        match_pct = (matches / src_distinct_rows * 100) if src_distinct_rows > 0 else 0
        print(f"   • {src_extras:,} row(s) only in source ({100-match_pct:.2f}% different)")
    if tgt_extras > 0:
        match_pct = (matches / tgt_distinct_rows * 100) if tgt_distinct_rows > 0 else 0
        print(f"   • {tgt_extras:,} row(s) only in target ({100-match_pct:.2f}% different)")
print("-"*80)

print("\n" + "="*80)

# Display sample mismatches (commented out by default)
# if src_extras > 0:
#     print("\nSample rows only in SOURCE (first 10):")
#     display(src_minus_tgt.limit(10))

# if tgt_extras > 0:
#     print("\nSample rows only in TARGET (first 10):")
#     display(tgt_minus_src.limit(10))

# COMMAND ----------

# DBTITLE 1,Validation Summary
print("\n" + "="*80)
print("VALIDATION SUMMARY")
print("="*80)

print(f"\nBatch Load ID: {batch_load_id}")
print(f"Target Table:  {target_table}")

print("\n📊 Results:")
print(f"   1. Row Count:           {'✅ PASSED' if source_row_count == target_row_count else '❌ FAILED'}")
print(f"   2. Schema:              {'✅ PASSED' if len(schema_issues) == 0 else '❌ FAILED'}")

# PK validation summary
if primary_keys and 'src_duplicates' in locals() and 'tgt_duplicates' in locals():
    pk_status = "✅ PASSED" if (src_duplicates == 0 and tgt_duplicates == 0) else "❌ FAILED"
else:
    pk_status = "⚠️  SKIPPED"
print(f"   3. Primary Key:         {pk_status}")

print(f"   4. Data Reconciliation: {'✅ PASSED' if (src_extras == 0 and tgt_extras == 0) else '❌ FAILED'}")

print("\n" + "="*80)
print("\n✅ Manual validation complete!")
print("\nThe dataframes are available for further analysis:")
print("   • df_source - Source ORC files")
print("   • df_target - Target Delta table")
print("\nYou can add custom analysis cells below.")
print("="*80)

# COMMAND ----------

