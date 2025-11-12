# Databricks notebook source
# MAGIC %md
# MAGIC # Quick Schema-Only Validation
# MAGIC 
# MAGIC **Purpose:** Fast schema validation for a specific table and batch - NO full data loads!
# MAGIC 
# MAGIC **Features:**
# MAGIC - ⚡ **FAST** - Reads only 10 rows from source and target
# MAGIC - 🔍 Schema validation (column names and data types)
# MAGIC - 📊 Configuration validation
# MAGIC - ⏱️ Runs in seconds, not minutes
# MAGIC 
# MAGIC **What this checks:**
# MAGIC - ✅ Column names match
# MAGIC - ✅ Data types match
# MAGIC - ✅ Configuration is valid
# MAGIC 
# MAGIC **What this DOES NOT check:**
# MAGIC - ❌ Row count (requires full scan)
# MAGIC - ❌ PK duplicates (requires full scan)
# MAGIC - ❌ Data reconciliation (requires full scan)
# MAGIC 
# MAGIC **Use this when:**
# MAGIC - You want to quickly verify schema compatibility
# MAGIC - You're debugging schema mismatches
# MAGIC - You don't need full data validation yet
# MAGIC 
# MAGIC **For full validation, use:** `debug_batch_validation` notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, regexp_extract
import json

# Import framework modules
from deltarecon.core.source_target_loader import SourceTargetLoader
from deltarecon.models.table_config import TableConfig
from deltarecon.validators.schema_validator import SchemaValidator

print("✓ Setup complete - Framework modules imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input Parameters (Widgets)

# COMMAND ----------

# Create widgets
dbutils.widgets.removeAll()

dbutils.widgets.text("ingestion_ops_schema", "jio_home_prod.operations", "1. Ingestion Ops Schema (catalog.schema)")
dbutils.widgets.text("validation_schema", "jio_home_prod.operations", "2. Validation Schema (catalog.schema)")
dbutils.widgets.text("target_table", "", "3. Target Table")
dbutils.widgets.text("batch_load_id", "", "4. Batch Load ID")

# Get values
INGESTION_OPS_SCHEMA = dbutils.widgets.get("ingestion_ops_schema")
VALIDATION_SCHEMA = dbutils.widgets.get("validation_schema")
TARGET_TABLE = dbutils.widgets.get("target_table")
BATCH_LOAD_ID = dbutils.widgets.get("batch_load_id")

# Define table names
INGESTION_CONFIG_TABLE = f"{INGESTION_OPS_SCHEMA}.serving_ingestion_config"
VALIDATION_MAPPING_TABLE = f"{VALIDATION_SCHEMA}.validation_mapping"

# Sample size for fast loading
SAMPLE_SIZE = 10

print("="*70)
print("INPUT PARAMETERS")
print("="*70)
print(f"Ingestion Ops Schema: {INGESTION_OPS_SCHEMA}")
print(f"Validation Schema: {VALIDATION_SCHEMA}")
print(f"Ingestion Config Table: {INGESTION_CONFIG_TABLE}")
print(f"Validation Mapping Table: {VALIDATION_MAPPING_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Batch Load ID: {BATCH_LOAD_ID}")
print(f"Sample Size: {SAMPLE_SIZE} rows (for speed)")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Configuration from Metadata Tables

# COMMAND ----------

print("⏳ Fetching configuration from metadata tables...")

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
    print("⚠️  WARNING: Table not found in validation_mapping, using defaults")
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
        print(f"⚠️  WARNING: Failed to parse source_file_options: {e}. Will use empty options.")
        source_file_options = {}

print("\n✓ Configuration loaded successfully")
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
# MAGIC ## Find Source Files for Batch

# COMMAND ----------

print("\n" + "="*70)
print("STEP 1: LOCATE SOURCE FILES")
print("="*70)
print(f"⏳ Querying ingestion_metadata table for batch '{BATCH_LOAD_ID}'...")

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

print(f"\n✓ Found {len(source_file_paths)} source file(s):")
for i, path in enumerate(source_file_paths[:5], 1):
    print(f"  {i}. {path}")
if len(source_file_paths) > 5:
    print(f"  ... and {len(source_file_paths) - 5} more")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Data Sample (Fast!)

# COMMAND ----------

print("\n" + "="*70)
print(f"STEP 2: SOURCE DATA LOADING (SAMPLE OF {SAMPLE_SIZE} ROWS)")
print("="*70)
print(f"📋 Configuration:")
print(f"   File format: {ingestion_config.source_file_format}")
print(f"   Sample size: {SAMPLE_SIZE} rows")
if partition_columns:
    print(f"   Partition columns: {partition_columns}")
if source_file_options:
    print(f"   File options: {source_file_options}")

print(f"\n⏳ Loading source data sample (first {SAMPLE_SIZE} rows only)...")

try:
    file_format = ingestion_config.source_file_format.lower()
    
    # Load based on format
    if file_format == "orc":
        print("  ✓ Loading ORC files...")
        source_df = spark.read.format("orc").load(source_file_paths)
        
    elif file_format == "csv":
        print("  ✓ Loading CSV files...")
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
            except Exception as e:
                print(f"    ⚠️  WARNING: Failed to parse schema: {e}. Will infer schema.")
        
        source_df = reader.load(source_file_paths)
        
    elif file_format == "text":
        print("  ✓ Loading Text files...")
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
            except Exception as e:
                print(f"    ⚠️  WARNING: Failed to parse schema: {e}. Will infer schema.")
        
        source_df = reader.load(source_file_paths)
        
    else:
        raise ValueError(f"Unsupported file format: {file_format}. Supported: orc, csv, text")
    
    # LIMIT to sample size for speed
    source_df = source_df.limit(SAMPLE_SIZE)
    
    # Extract partition columns from file path if they don't exist in data
    if partition_columns:
        print(f"  ✓ Extracting partition columns: {partition_columns}")
        for part_col in partition_columns:
            if part_col not in source_df.columns:
                # Extract from _metadata.file_path
                pattern = f"{part_col}=([^/]+)"
                source_df = source_df.withColumn(
                    part_col,
                    regexp_extract(col("_metadata.file_path"), pattern, 1)
                )
                
                # Try to cast to appropriate type (date, int, etc.)
                if "date" in part_col.lower():
                    source_df = source_df.withColumn(part_col, col(part_col).cast("date"))
    
    # Force execution to get actual count
    src_count = source_df.count()
    
    print(f"\n✓ Source sample loaded")
    print(f"  Rows: {src_count} (sample)")
    print(f"  Columns: {len(source_df.columns)}")
    print("="*70)
    
except Exception as e:
    print(f"❌ ERROR loading source data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Target Data Sample (Fast!)

# COMMAND ----------

print("\n" + "="*70)
print(f"STEP 3: TARGET DATA LOADING (SAMPLE OF {SAMPLE_SIZE} ROWS)")
print("="*70)
print(f"📋 Configuration:")
print(f"   Target table: {TARGET_TABLE}")
print(f"   Write mode: {ingestion_config.write_mode}")
print(f"   Sample size: {SAMPLE_SIZE} rows")

try:
    # Load target with appropriate filter based on write mode
    if ingestion_config.write_mode.lower() == "overwrite":
        print(f"\n⏳ Loading target sample (overwrite mode - no filter)...")
        target_df = spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT {SAMPLE_SIZE}")
    
    elif ingestion_config.write_mode.lower() == "partition_overwrite":
        print(f"\n⏳ Loading target sample (partition_overwrite mode)...")
        
        if not partition_columns:
            raise ValueError("partition_overwrite mode requires partition_columns to be configured")
        
        # Extract partition values from source sample
        partition_values = {}
        for part_col in partition_columns:
            if part_col not in source_df.columns:
                raise ValueError(f"Partition column '{part_col}' not found in source data")
            
            distinct_vals = [row[part_col] for row in source_df.select(part_col).distinct().collect()]
            partition_values[part_col] = distinct_vals
        
        # Build WHERE clause
        where_conditions = []
        for part_col, values in partition_values.items():
            if len(values) == 1:
                val = values[0]
                if isinstance(val, str):
                    where_conditions.append(f"{part_col} = '{val}'")
                else:
                    where_conditions.append(f"{part_col} = {val}")
            else:
                if isinstance(values[0], str):
                    val_list = ", ".join([f"'{v}'" for v in values])
                else:
                    val_list = ", ".join([str(v) for v in values])
                where_conditions.append(f"{part_col} IN ({val_list})")
        
        where_clause = " AND ".join(where_conditions)
        print(f"  Filter: {where_clause}")
        
        # Execute query with LIMIT for sample
        target_df = spark.sql(f"""
            SELECT * FROM {TARGET_TABLE}
            WHERE {where_clause}
            LIMIT {SAMPLE_SIZE}
        """)
    
    elif ingestion_config.write_mode.lower() in ["append", "merge"]:
        print(f"\n⏳ Loading target sample ({ingestion_config.write_mode} mode)...")
        print(f"  Filter: _aud_batch_load_id = '{BATCH_LOAD_ID}'")
        
        target_df = spark.sql(f"""
            SELECT * FROM {TARGET_TABLE}
            WHERE _aud_batch_load_id = '{BATCH_LOAD_ID}'
            LIMIT {SAMPLE_SIZE}
        """)
    
    else:
        raise ValueError(f"Unsupported write_mode: {ingestion_config.write_mode}")
    
    # Force execution to get actual count
    tgt_count = target_df.count()
    
    print(f"\n✓ Target sample loaded")
    print(f"  Rows: {tgt_count} (sample)")
    print(f"  Columns: {len(target_df.columns)}")
    
    if tgt_count == 0:
        print(f"\n⚠️  WARNING: No data found in target! Schema validation may fail.")
    
    print("="*70)
    
except Exception as e:
    print(f"❌ ERROR loading target data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Comparison

# COMMAND ----------

print("\n" + "="*70)
print("STEP 4: SCHEMA VALIDATION")
print("="*70)

# Build schema dictionaries (excluding partition columns from source, audit columns from target)
partition_cols_set = set(partition_columns)

print(f"\n📋 Building schema comparison...")
print(f"   Excluding from source: partition columns {partition_columns}")
print(f"   Excluding from target: _aud_* columns and partition columns")

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

print(f"\n📊 SCHEMA COMPARISON RESULTS:")
print(f"   Source columns: {len(source_schema)}")
print(f"   Target columns: {len(target_schema)}")
print(f"   Common columns: {len(common_cols)}")
print(f"   Missing in target: {len(missing_in_target)}")
print(f"   Extra in target: {len(extra_in_target)}")
print(f"   Type mismatches: {len(type_mismatches)}")

has_issues = bool(missing_in_target or extra_in_target or type_mismatches)

if missing_in_target:
    print(f"\n❌ MISSING IN TARGET ({len(missing_in_target)} columns):")
    for col_name in sorted(missing_in_target):
        print(f"     - {col_name} ({source_schema[col_name]})")

if extra_in_target:
    print(f"\n⚠️  EXTRA IN TARGET ({len(extra_in_target)} columns):")
    for col_name in sorted(extra_in_target):
        print(f"     - {col_name} ({target_schema[col_name]})")

if type_mismatches:
    print(f"\n❌ TYPE MISMATCHES ({len(type_mismatches)} columns):")
    for mismatch in type_mismatches:
        print(f"     - {mismatch['column']}:")
        print(f"         Source: {mismatch['source_type']}")
        print(f"         Target: {mismatch['target_type']}")

schema_status = "PASSED" if not has_issues else "FAILED"
print(f"\n{'✅' if schema_status == 'PASSED' else '❌'} Schema Validation: {schema_status}")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Schema Listing

# COMMAND ----------

print("\n" + "="*70)
print("SOURCE SCHEMA (excluding partition columns)")
print("="*70)
for field in source_df.schema.fields:
    if field.name not in partition_cols_set:
        print(f"  {field.name:<40} {field.dataType.simpleString()}")

print("\n" + "="*70)
print("TARGET SCHEMA (excluding _aud_* and partition columns)")
print("="*70)
for field in target_df.schema.fields:
    if not field.name.startswith('_aud_') and field.name not in partition_cols_set:
        print(f"  {field.name:<40} {field.dataType.simpleString()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Preview (Sample)

# COMMAND ----------

print("\n" + "="*70)
print(f"SOURCE DATA PREVIEW (first 5 of {src_count} sampled rows)")
# print("="*70)
display(source_df.limit(5))

# COMMAND ----------

print("\n" + "="*70)
print(f"TARGET DATA PREVIEW (first 5 of {tgt_count} sampled rows)")
# print("="*70)
display(target_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

print("\n" + "="*70)
print("VALIDATION SUMMARY REPORT")
print("="*70)
print(f"Table: {TARGET_TABLE}")
print(f"Batch: {BATCH_LOAD_ID}")
print(f"Validation Type: SCHEMA ONLY (Fast)")
print(f"Sample Size: {SAMPLE_SIZE} rows")
print(f"Date: {spark.sql('SELECT current_timestamp()').collect()[0][0]}")
print("="*70)

print("\n📋 VALIDATION RESULTS:")
print(f"   Schema Validation: {schema_status}")
print(f"     - Column name match: {'PASSED' if not (missing_in_target or extra_in_target) else 'FAILED'}")
print(f"     - Data type match: {'PASSED' if not type_mismatches else 'FAILED'}")

print(f"\n📊 SCHEMA METRICS:")
print(f"   Source columns: {len(source_schema)}")
print(f"   Target columns: {len(target_schema)}")
print(f"   Common columns: {len(common_cols)}")
print(f"   Missing in target: {len(missing_in_target)}")
print(f"   Extra in target: {len(extra_in_target)}")
print(f"   Type mismatches: {len(type_mismatches)}")

print(f"\n⚡ PERFORMANCE:")
print(f"   This ran on {SAMPLE_SIZE} rows only (fast mode)")
print(f"   For full validation including:")
print(f"     - Row count comparison")
print(f"     - PK duplicate checks")
print(f"     - Data reconciliation")
print(f"   Use the 'debug_batch_validation' notebook")

if schema_status == "PASSED":
    print(f"\n✅ Schema validation PASSED! Ready for full validation.")
else:
    print(f"\n❌ Schema validation FAILED! Fix schema issues before running full validation.")

print("="*70)

