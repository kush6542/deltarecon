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
from deltarecon.core.ingestion_reader import IngestionConfigReader
from deltarecon.models.table_config import TableConfig
from deltarecon.validators.schema_validator import SchemaValidator

print("✓ Setup complete - Framework modules imported")
print("  • IngestionConfigReader (with partition mapping prioritization)")
print("  • SchemaValidator (with DESCRIBE EXTENDED support)")

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

print("📋 Fetching configuration using FRAMEWORK (IngestionConfigReader)...")
print("   This will automatically prioritize source_table_partition_mapping over config!")

# Use framework's IngestionConfigReader to get TableConfig
# This automatically handles partition mapping prioritization
# Note: table_group is required by constructor but not used by _row_to_table_config()
config_reader = IngestionConfigReader(spark, table_group="debug_notebook")

print(f"\n🔍 Step 1: Query validation_mapping + ingestion_config for table: {TARGET_TABLE}")
# Must join validation_mapping with ingestion_config to get ALL required fields
# This mirrors exactly what the framework does in get_tables_in_group()
validation_query = f"""
    SELECT 
        vm.table_group,
        vm.table_family,
        vm.src_table,
        vm.tgt_table,
        vm.tgt_primary_keys,
        vm.mismatch_exclude_fields,
        vm.validation_is_active,
        ic.write_mode,
        ic.partition_column,
        ic.source_file_format,
        ic.source_file_options
    FROM {VALIDATION_MAPPING_TABLE} vm
    INNER JOIN {INGESTION_CONFIG_TABLE} ic
        ON vm.tgt_table = concat_ws('.', ic.target_catalog, ic.target_schema, ic.target_table)
    WHERE vm.tgt_table = '{TARGET_TABLE}'
        AND vm.validation_is_active = TRUE
"""

mapping_rows = spark.sql(validation_query).collect()

if not mapping_rows:
    raise ValueError(f"Table '{TARGET_TABLE}' not found or inactive in {VALIDATION_MAPPING_TABLE}")

print(f"   ✓ Found configuration for {TARGET_TABLE}")
print(f"   ✓ Joined validation_mapping + ingestion_config")
print(f"   ✓ Query includes all fields required by _row_to_table_config()")

# Use framework's _row_to_table_config method to build TableConfig
# This includes partition mapping prioritization logic!
print(f"\n🔍 Step 2: Build TableConfig using framework logic")
print(f"   Framework will:")
print(f"   • Query serving_ingestion_config for table details")
print(f"   • Query source_table_partition_mapping for authoritative partitions")
print(f"   • Prioritize mapping table over config (if available)")
print(f"   • Fallback to config if mapping is empty")

table_config = config_reader._row_to_table_config(mapping_rows[0])

print(f"\n✅ TableConfig built successfully!")

print("\n" + "="*70)
print("TABLE CONFIGURATION (from framework)")
print("="*70)
print(f"Table Group: {table_config.table_group}")
print(f"Table Family: {table_config.table_family}")
print(f"Target Table: {table_config.table_name}")
print(f"Source Table: {table_config.source_table}")
print(f"Write Mode: {table_config.write_mode}")
print(f"File Format: {table_config.source_file_format}")
print(f"File Options: {table_config.source_file_options}")
print(f"Primary Keys: {table_config.primary_keys}")
print(f"Partition Columns: {table_config.partition_columns}")
if table_config.partition_datatypes:
    print(f"Partition Datatypes: {table_config.partition_datatypes}")
print(f"Exclude Fields: {table_config.mismatch_exclude_fields}")
print(f"Has Primary Keys: {table_config.has_primary_keys}")
print(f"Is Partitioned: {table_config.is_partitioned}")
print(f"Is Active: {table_config.is_active}")
print("="*70)

# Extract for convenience (to use in rest of notebook)
TARGET_TABLE = table_config.table_name
primary_keys = table_config.primary_keys or []
partition_columns = table_config.partition_columns or []
partition_datatypes = table_config.partition_datatypes or {}
exclude_fields = table_config.mismatch_exclude_fields or []
source_file_options = table_config.source_file_options or {}

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
print(f"   File format: {table_config.source_file_format}")
print(f"   Sample size: {SAMPLE_SIZE} rows")
if partition_columns:
    print(f"   Partition columns: {partition_columns}")
if source_file_options:
    print(f"   File options: {source_file_options}")

print(f"\n⏳ Loading source data sample (first {SAMPLE_SIZE} rows only)...")

try:
    file_format = table_config.source_file_format.lower()
    
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
print(f"   Write mode: {table_config.write_mode}")
print(f"   Sample size: {SAMPLE_SIZE} rows")

try:
    # Load target with appropriate filter based on write mode
    if table_config.write_mode.lower() == "overwrite":
        print(f"\n⏳ Loading target sample (overwrite mode - no filter)...")
        target_df = spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT {SAMPLE_SIZE}")
    
    elif table_config.write_mode.lower() == "partition_overwrite":
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
    
    elif table_config.write_mode.lower() in ["append", "merge"]:
        print(f"\n⏳ Loading target sample ({table_config.write_mode} mode)...")
        print(f"  Filter: _aud_batch_load_id = '{BATCH_LOAD_ID}'")
        
        target_df = spark.sql(f"""
            SELECT * FROM {TARGET_TABLE}
            WHERE _aud_batch_load_id = '{BATCH_LOAD_ID}'
            LIMIT {SAMPLE_SIZE}
        """)
    
    else:
        raise ValueError(f"Unsupported write_mode: {table_config.write_mode}")
    
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
print("STEP 4: SCHEMA VALIDATION (using FRAMEWORK)")
print("="*70)
print(f"Framework class: SchemaValidator (with DESCRIBE EXTENDED support)")
print(f"This will fetch original schema from target table, bypassing masked columns!")

# Initialize framework's SchemaValidator
schema_validator = SchemaValidator()

print(f"\n🔧 Framework will:")
print(f"   1. Get source schema from DataFrame")
print(f"   2. Execute DESCRIBE EXTENDED on target table to get original schema")
print(f"   3. Compare schemas (excluding audit/partition columns)")
print(f"   4. Report missing, extra, and mismatched columns")

# Run framework validation
try:
    result = schema_validator.validate(
        source_df=source_df,
        target_df=target_df,
        config=table_config
    )
    
    print(f"\n✅ Schema validation completed!")
    print(f"   Status: {result.status}")
    print(f"   Message: {result.message}")
    
    # Verbose debug output
    if result.details:
        print(f"\n📊 DETAILED RESULTS:")
        
        if 'source_columns' in result.details:
            print(f"   Source columns: {result.details['source_columns']} (excluding partition columns)")
        if 'target_columns' in result.details:
            print(f"   Target columns: {result.details['target_columns']} (excluding audit/partition columns)")
        if 'missing_in_target' in result.details:
            missing = result.details['missing_in_target']
            print(f"   Missing in target: {len(missing)}")
            if missing:
                print(f"\n   ❌ MISSING IN TARGET:")
                for col in missing:
                    print(f"      - {col}")
        
        if 'extra_in_target' in result.details:
            extra = result.details['extra_in_target']
            print(f"   Extra in target: {len(extra)}")
            if extra:
                print(f"\n   ⚠️  EXTRA IN TARGET:")
                for col in extra:
                    print(f"      - {col}")
        
        if 'type_mismatches' in result.details:
            mismatches = result.details['type_mismatches']
            print(f"   Type mismatches: {len(mismatches)}")
            if mismatches:
                print(f"\n   ❌ TYPE MISMATCHES:")
                for mismatch in mismatches:
                    print(f"      - {mismatch['column']}:")
                    print(f"          Source: {mismatch['source_type']}")
                    print(f"          Target: {mismatch['target_type']}")
    
    schema_status = result.status
    has_issues = result.status == "FAILED"
    
except Exception as e:
    print(f"\n❌ ERROR during schema validation: {str(e)}")
    print(f"   This may indicate a framework issue or configuration problem")
    schema_status = "FAILED"
    has_issues = True
    import traceback
    traceback.print_exc()

print(f"\n{'✅' if schema_status == 'PASSED' else '❌'} Schema Validation: {schema_status}")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Schema Listing

# COMMAND ----------

# Define partition columns set for filtering
partition_cols_set = set(partition_columns) if partition_columns else set()

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
# display(source_df.limit(5))

# COMMAND ----------

print("\n" + "="*70)
print(f"TARGET DATA PREVIEW (first 5 of {tgt_count} sampled rows)")
# print("="*70)
# display(target_df.limit(5))

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

# Extract metrics from framework validation result
if 'result' in locals() and result:
    missing_count = result.metrics.get('missing_in_target_count', 0)
    extra_count = result.metrics.get('extra_in_target_count', 0)
    type_mismatch_count = result.metrics.get('type_mismatches_count', 0)
    source_col_count = result.metrics.get('source_column_count', 0)
    target_col_count = result.metrics.get('target_column_count', 0)
    common_col_count = result.metrics.get('common_columns', 0)
    col_name_status = result.metrics.get('col_name_status', 'UNKNOWN')
    data_type_status = result.metrics.get('data_type_status', 'UNKNOWN')
else:
    # Fallback if result not available
    missing_count = 0
    extra_count = 0
    type_mismatch_count = 0
    source_col_count = 0
    target_col_count = 0
    common_col_count = 0
    col_name_status = 'UNKNOWN'
    data_type_status = 'UNKNOWN'

print("\n📋 VALIDATION RESULTS:")
print(f"   Schema Validation: {schema_status}")
print(f"     - Column name match: {col_name_status}")
print(f"     - Data type match: {data_type_status}")

print(f"\n📊 SCHEMA METRICS:")
print(f"   Source columns: {source_col_count}")
print(f"   Target columns: {target_col_count}")
print(f"   Common columns: {common_col_count}")
print(f"   Missing in target: {missing_count}")
print(f"   Extra in target: {extra_count}")
print(f"   Type mismatches: {type_mismatch_count}")

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

