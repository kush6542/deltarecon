# Databricks notebook source
# MAGIC %md
# MAGIC # Manual Batch Validation Debugger
# MAGIC 
# MAGIC **Purpose:** Debug validation failures for a specific table and batch in production or test environments
# MAGIC 
# MAGIC **Features:**
# MAGIC - Supports ORC, CSV, and Text file formats
# MAGIC - Uses framework's file selection logic
# MAGIC - **Detailed file reading information** - shows exactly which files are being read from source and target
# MAGIC - **Framework-based target filtering** - uses write_mode logic (overwrite vs append/merge/partition_overwrite)
# MAGIC - **Partition extraction visualization** - for partition_overwrite mode, shows extracted partition values
# MAGIC - **Clear target loading strategy** - explains HOW target data is filtered based on write mode
# MAGIC - Works with production and test environments
# MAGIC - Prints all files being read for transparency
# MAGIC - Shows write_mode-specific behavior with detailed explanations
# MAGIC 
# MAGIC **How to use:**
# MAGIC 1. Fill in the widgets at the top
# MAGIC 2. Run all cells
# MAGIC 3. Review each validation check output
# MAGIC 4. Look for detailed logging sections marked with 📋 and ✓ symbols

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
from deltarecon.core.ingestion_reader import IngestionConfigReader
from deltarecon.models.table_config import TableConfig
from deltarecon.validators.schema_validator import SchemaValidator

print("Setup complete - Framework modules imported")
print("  ✓ BatchProcessor")
print("  ✓ SourceTargetLoader")
print("  ✓ IngestionConfigReader (with partition mapping prioritization)")
print("  ✓ SchemaValidator (with DESCRIBE EXTENDED support)")
print("  ✓ TableConfig")

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
print(f"\n{'='*70}")
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
# MAGIC ## Execution Plan Summary

# COMMAND ----------

print("\n" + "="*70)
print("EXECUTION PLAN - HOW THIS VALIDATION WILL WORK")
print("="*70)
print(f"\n1️⃣  IDENTIFY SOURCE FILES:")
print(f"   - Query ingestion_metadata table for batch '{BATCH_LOAD_ID}'")
print(f"   - Extract source file paths that were ingested")

print(f"\n2️⃣  LOAD SOURCE DATA (manual - for debugging):")
print(f"   - Format: {table_config.source_file_format}")
if partition_columns:
    print(f"   - Extract partition columns from file paths: {partition_columns}")
    if partition_datatypes:
        print(f"   - Partition datatypes (from mapping table): {partition_datatypes}")
print(f"   - Apply file options: {source_file_options if source_file_options else 'None'}")

print(f"\n3️⃣  LOAD TARGET DATA (using FRAMEWORK SourceTargetLoader):")
print(f"   - Table: {TARGET_TABLE}")
print(f"   - Write mode: {table_config.write_mode}")
print(f"   - Framework class: SourceTargetLoader")

if table_config.write_mode.lower() == "overwrite":
    print(f"   - Strategy: Load ENTIRE table (no filtering)")
    print(f"   - Reason: Overwrite mode = entire table is the current batch")
elif table_config.write_mode.lower() == "partition_overwrite":
    print(f"   - Strategy: Filter by PARTITION VALUES + batch_id")
    print(f"   - Partitions: {partition_columns}")
    print(f"   - Reason: partition_overwrite overwrites old batch_ids")
    print(f"   - Framework logic: Extracts values from source, filters target by partitions + batch_id")
    print(f"   - Note: This PREVENTS comparing against historical/stale data!")
elif table_config.write_mode.lower() in ["append", "merge"]:
    print(f"   - Strategy: Filter by _aud_batch_load_id = '{BATCH_LOAD_ID}'")
    print(f"   - Reason: {table_config.write_mode} mode preserves all batch_ids")

print(f"\n4️⃣  RUN VALIDATION CHECKS:")
print(f"   - Row Count: Compare source vs target row counts")
print(f"   - Schema: Verify column names and types match")
if primary_keys:
    print(f"   - PK Duplicates: Check for duplicates in {primary_keys}")
if RUN_RECONCILIATION:
    print(f"   - Data Reconciliation: Compare row-level data hashes")

print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find Source Files for Batch (Using Framework Logic)

# COMMAND ----------

print("\n" + "="*70)
print("STEP 1: LOCATE SOURCE FILES")
print("="*70)
print("Querying ingestion_metadata table for batch files...")
print(f"File format: {table_config.source_file_format}")

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

print("\n" + "="*70)
print("STEP 2: SOURCE DATA LOADING")
print("="*70)
print(f"📋 Configuration:")
print(f"   File format: {table_config.source_file_format}")
print(f"   Write mode: {table_config.write_mode}")
print(f"   Batch ID: {BATCH_LOAD_ID}")
if partition_columns:
    print(f"   Partition columns: {partition_columns}")
if source_file_options:
    print(f"   File options: {source_file_options}")

print(f"\n📂 Source files to read ({len(source_file_paths)}):")
for i, path in enumerate(source_file_paths, 1):
    print(f"   [{i}] {path}")

print("\n" + "-"*70)
print("⏳ Loading source data files...")
print("-"*70)

try:
    file_format = table_config.source_file_format.lower()
    
    # Load based on format
    if file_format == "orc":
        print("  ✓ Loading ORC files...")
        print("    Format: Apache ORC (Optimized Row Columnar)")
        print("    Options: None (ORC is self-describing)")
        source_df = spark.read.format("orc").load(source_file_paths)
        
    elif file_format == "csv":
        print("  ✓ Loading CSV files...")
        print("    Format: CSV (Comma-Separated Values)")
        reader = spark.read.format("csv")
        
        # Apply CSV options
        options_applied = []
        if "header" in source_file_options:
            reader = reader.option("header", source_file_options["header"])
            options_applied.append(f"header={source_file_options['header']}")
        if "sep" in source_file_options:
            reader = reader.option("sep", source_file_options["sep"])
            options_applied.append(f"sep='{source_file_options['sep']}'")
        if "quote" in source_file_options:
            reader = reader.option("quote", source_file_options["quote"])
            options_applied.append(f"quote='{source_file_options['quote']}'")
        if "escape" in source_file_options:
            reader = reader.option("escape", source_file_options["escape"])
            options_applied.append(f"escape='{source_file_options['escape']}'")
        if "multiline" in source_file_options:
            reader = reader.option("multiline", source_file_options["multiline"])
            options_applied.append(f"multiline={source_file_options['multiline']}")
        
        if options_applied:
            print(f"    Options applied: {', '.join(options_applied)}")
        else:
            print(f"    Options applied: None (using defaults)")
        
        # Apply schema if provided
        if "schema" in source_file_options:
            from pyspark.sql.types import StructType
            try:
                schema = StructType.fromDDL(source_file_options["schema"])
                reader = reader.schema(schema)
                print(f"    Schema: Explicit schema provided")
            except Exception as e:
                print(f"    ⚠️  WARNING: Failed to parse schema: {e}. Will infer schema.")
        else:
            print(f"    Schema: Will be inferred from data")
        
        source_df = reader.load(source_file_paths)
        
    elif file_format == "text":
        print("  ✓ Loading Text files...")
        print("    Format: Text (CSV-based with custom separator)")
        # Text files are just CSV with different separators
        reader = spark.read.format("csv")
        
        # Apply text/CSV options
        options_applied = []
        if "header" in source_file_options:
            reader = reader.option("header", source_file_options["header"])
            options_applied.append(f"header={source_file_options['header']}")
        if "sep" in source_file_options:
            reader = reader.option("sep", source_file_options["sep"])
            options_applied.append(f"sep='{source_file_options['sep']}'")
        if "quote" in source_file_options:
            reader = reader.option("quote", source_file_options["quote"])
            options_applied.append(f"quote='{source_file_options['quote']}'")
        if "escape" in source_file_options:
            reader = reader.option("escape", source_file_options["escape"])
            options_applied.append(f"escape='{source_file_options['escape']}'")
        if "multiline" in source_file_options:
            reader = reader.option("multiline", source_file_options["multiline"])
            options_applied.append(f"multiline={source_file_options['multiline']}")
        
        if options_applied:
            print(f"    Options applied: {', '.join(options_applied)}")
        else:
            print(f"    Options applied: None (using defaults)")
        
        # Apply schema if provided
        if "schema" in source_file_options:
            from pyspark.sql.types import StructType
            try:
                schema = StructType.fromDDL(source_file_options["schema"])
                reader = reader.schema(schema)
                print(f"    Schema: Explicit schema provided")
            except Exception as e:
                print(f"    ⚠️  WARNING: Failed to parse schema: {e}. Will infer schema.")
        else:
            print(f"    Schema: Will be inferred from data")
        
        source_df = reader.load(source_file_paths)
        
    else:
        raise ValueError(f"Unsupported file format: {file_format}. Supported: orc, csv, text")
    
    original_count = source_df.count()
    
    print(f"\n✓ Original data loaded successfully")
    print(f"  Rows: {original_count:,}")
    print(f"  Columns: {len(source_df.columns)}")
    
    # Extract partition columns from file path if they don't exist in data
    if partition_columns:
        print(f"\n" + "-"*70)
        print(f"Extracting partition columns: {partition_columns}")
        if partition_datatypes:
            print(f"Using datatypes from source_table_partition_mapping:")
            for col_name, dtype in partition_datatypes.items():
                print(f"  • {col_name}: {dtype}")
        print("-"*70)
        for part_col in partition_columns:
            if part_col not in source_df.columns:
                print(f"  → Extracting partition column: {part_col}")
                # Extract from _metadata.file_path
                pattern = f"{part_col}=([^/]+)"
                source_df = source_df.withColumn(
                    part_col,
                    regexp_extract(col("_metadata.file_path"), pattern, 1)
                )
                
                # Cast to datatype from mapping table (or fallback logic)
                target_dtype = partition_datatypes.get(part_col, 'string') if partition_datatypes else 'string'
                
                if target_dtype and target_dtype.lower() != 'string':
                    source_df = source_df.withColumn(part_col, col(part_col).cast(target_dtype))
                    print(f"    ✓ Cast to {target_dtype} (from mapping table)")
                elif "date" in part_col.lower():
                    # Fallback for date columns if no mapping
                    source_df = source_df.withColumn(part_col, col(part_col).cast("date"))
                    print(f"    ✓ Cast to date type (fallback logic)")
        
        # Verify partition extraction
        final_count = source_df.count()
        if final_count != original_count:
            print(f"\n⚠️  WARNING: Partition extraction changed row count! Before: {original_count:,}, After: {final_count:,}")
        else:
            print(f"\n✓ Partition extraction verified: row count unchanged")
    
    source_df.cache()
    src_count = source_df.count()
    
    print(f"\n" + "="*70)
    print("SOURCE DATA LOADED")
    print("="*70)
    print(f"✓ Rows: {src_count:,}")
    print(f"✓ Columns: {len(source_df.columns)}")
    print("="*70)
    
except Exception as e:
    print(f"ERROR loading source data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Target Data (Delta Table)

# COMMAND ----------

print("\n" + "="*70)
print("STEP 3: TARGET DATA LOADING (using FRAMEWORK)")
print("="*70)
print(f"📋 Configuration:")
print(f"   Target table: {TARGET_TABLE}")
print(f"   Write mode: {table_config.write_mode}")
print(f"   Batch ID: {BATCH_LOAD_ID}")
print(f"   Framework class: SourceTargetLoader")

print(f"\n🔧 Using framework's _load_target_delta() method")
print(f"   This method includes ALL framework improvements:")
print(f"   ✓ Partition overwrite batch filtering")
print(f"   ✓ Batch verification")
print(f"   ✓ Historical data exclusion")

try:
    # Initialize framework's SourceTargetLoader
    loader = SourceTargetLoader(spark)
    
    # Initialize partition_values (used for debug output later)
    partition_values = {}
    
    print(f"\n📊 Framework will determine filtering strategy based on write_mode...")
    
    if table_config.write_mode.lower() == "overwrite":
        # OVERWRITE mode: Entire table is replaced each ingestion
        print("\n" + "-"*70)
        print("OVERWRITE MODE DETECTED")
        print("-"*70)
        print("📋 Framework strategy: Load ENTIRE table (no filtering)")
        print("   Reason: In overwrite mode, entire table = current batch")
        print("-"*70)
        
        # Use framework method
        target_df = loader._load_target_delta(table_config, BATCH_LOAD_ID, source_df=source_df)
        
        # Verbose debug output
        actual_batches = [row['_aud_batch_load_id'] for row in 
                        target_df.select("_aud_batch_load_id").distinct().limit(5).collect()]
        print(f"\n✓ Actual batch_ids in table: {actual_batches}")
        
        if BATCH_LOAD_ID not in actual_batches:
            print(f"\n⚠️  Note: Validating batch '{BATCH_LOAD_ID}' but table contains {actual_batches}")
            print(f"   This is EXPECTED for overwrite mode - entire current table represents this batch")
    
    elif table_config.write_mode.lower() == "partition_overwrite":
        # PARTITION_OVERWRITE: Uses partition-based filtering + batch_id
        print("\n" + "-"*70)
        print("PARTITION_OVERWRITE MODE DETECTED")
        print("-"*70)
        print("📋 Framework strategy: Filter by PARTITION VALUES + batch_id")
        print("   Reason: In partition_overwrite mode, old batch_ids are overwritten")
        print("   Framework logic: Extract partition values from source, filter target by partitions + batch_id")
        print("   This PREVENTS comparing against historical/stale data!")
        print("-"*70)
        
        if not partition_columns:
            raise ValueError("partition_overwrite mode requires partition_columns to be configured")
        
        # Show partition values extracted from source (verbose debug)
        print(f"\n🔍 DEBUG: Partition values in SOURCE data:")
        print(f"   Partition columns: {partition_columns}")
        
        partition_values = {}
        for part_col in partition_columns:
            if part_col not in source_df.columns:
                raise ValueError(f"Partition column '{part_col}' not found in source data")
            
            distinct_vals = [row[part_col] for row in source_df.select(part_col).distinct().collect()]
            partition_values[part_col] = distinct_vals
            
            print(f"   • {part_col}: {len(distinct_vals)} distinct value(s)")
            # Show first 5 values for debug
            for val in distinct_vals[:5]:
                print(f"      - {val}")
            if len(distinct_vals) > 5:
                print(f"      ... and {len(distinct_vals) - 5} more")
        
        print(f"\n🔧 Framework will now:")
        print(f"   1. Extract partition values from source (shown above)")
        print(f"   2. Build SQL WHERE clause: (partitions) AND (_aud_batch_load_id = '{BATCH_LOAD_ID}')")
        print(f"   3. Load target data using optimized query")
        print(f"   4. Verify batch consistency")
        
        # Use framework method - it handles all the logic!
        target_df = loader._load_target_delta(table_config, BATCH_LOAD_ID, source_df=source_df)
    
    elif table_config.write_mode.lower() in ["append", "merge"]:
        # APPEND/MERGE: Multiple batches coexist, filter by batch_id
        print("\n" + "-"*70)
        print(f"{table_config.write_mode.upper()} MODE DETECTED")
        print("-"*70)
        print(f"📋 Framework strategy: Filter by batch_id only")
        print(f"   Filter: _aud_batch_load_id = '{BATCH_LOAD_ID}'")
        print(f"   Reason: In {table_config.write_mode} mode, batches coexist with unique batch_ids")
        print(f"   Framework logic: Use WHERE clause for predicate pushdown")
        print("-"*70)
        
        # Use framework method
        target_df = loader._load_target_delta(table_config, BATCH_LOAD_ID, source_df=source_df)
    
    else:
        raise ValueError(f"Unsupported write_mode: {table_config.write_mode}")
    
    target_df.cache()
    tgt_count = target_df.count()
    
    print(f"\n" + "="*70)
    print("TARGET DATA LOADED")
    print("="*70)
    print(f"✓ Rows: {tgt_count:,}")
    print(f"✓ Columns: {len(target_df.columns)}")
    
    # Verbose debug output for partition_overwrite
    if table_config.write_mode.lower() == "partition_overwrite" and tgt_count > 0:
        actual_batches = [row['_aud_batch_load_id'] for row in 
                        target_df.select("_aud_batch_load_id").distinct().limit(10).collect()]
        print(f"\n📋 DEBUG: Batch IDs found in filtered target data:")
        for batch in actual_batches:
            if batch == BATCH_LOAD_ID:
                print(f"   • {batch} ← (this is the batch we're validating)")
            else:
                print(f"   • {batch} ← (UNEXPECTED! Framework should filter to batch_id '{BATCH_LOAD_ID}')")
        
        if BATCH_LOAD_ID not in actual_batches:
            print(f"\n⚠️  WARNING: Batch '{BATCH_LOAD_ID}' not found in target data!")
            print(f"   Framework filtering may have an issue or data was overwritten")
    
    # Safety check: warn if no data found
    if tgt_count == 0:
        print(f"\n⚠️  WARNING: No data found in target table!")
        print(f"   Batch: {BATCH_LOAD_ID}")
        if table_config.write_mode.lower() == "partition_overwrite":
            print(f"   Possible causes:")
            print(f"   1) Partitions were overwritten by a later batch")
            print(f"   2) Partition values in source don't match target")
            print(f"   3) Ingestion failed or incomplete")
        else:
            print(f"   Possible causes:")
            print(f"   1) Batch was overwritten (if overwrite mode)")
            print(f"   2) Ingestion failed")
            print(f"   3) Filter mismatch")
    
    print("="*70)
    
except Exception as e:
    print(f"ERROR loading target data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading Summary

# COMMAND ----------

print("\n" + "="*70)
print("DATA LOADING SUMMARY")
print("="*70)
print(f"\n📊 SOURCE DATA:")
print(f"   Format: {table_config.source_file_format}")
print(f"   Files read: {len(source_file_paths)}")
print(f"   Total rows: {src_count:,}")
print(f"   Total columns: {len(source_df.columns)}")
if partition_columns:
    print(f"   Partition columns extracted: {partition_columns}")

print(f"\n📊 TARGET DATA:")
print(f"   Table: {TARGET_TABLE}")
print(f"   Write mode: {table_config.write_mode}")

if table_config.write_mode.lower() == "overwrite":
    print(f"   Filter applied: None (entire table loaded)")
    print(f"   Reason: Overwrite mode - entire table = current batch")

elif table_config.write_mode.lower() == "partition_overwrite":
    print(f"   Filter applied: Partition values from source")
    if partition_columns:
        for part_col in partition_columns:
            if part_col in partition_values:
                vals = partition_values[part_col]
                if len(vals) <= 3:
                    print(f"      {part_col}: {vals}")
                else:
                    print(f"      {part_col}: {vals[:3]} ... ({len(vals)} total)")
    print(f"   Reason: partition_overwrite mode - filter by current partition state")
    print(f"   Note: Target may contain data from multiple batches")

elif table_config.write_mode.lower() in ["append", "merge"]:
    print(f"   Filter applied: _aud_batch_load_id = '{BATCH_LOAD_ID}'")
    print(f"   Reason: {table_config.write_mode} mode - multiple batches coexist")

print(f"   Total rows: {tgt_count:,}")
print(f"   Total columns: {len(target_df.columns)}")

print(f"\n📈 ROW COUNT COMPARISON:")
print(f"   Source: {src_count:,}")
print(f"   Target: {tgt_count:,}")
if src_count == tgt_count:
    print(f"   ✓ Match! (difference: 0)")
else:
    diff = abs(src_count - tgt_count)
    pct = (diff / max(src_count, tgt_count)) * 100
    print(f"   ⚠️  Mismatch! (difference: {diff:,}, {pct:.2f}%)")

print("="*70)

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

print("\n" + "="*70)
print("STEP 4.1: ROW COUNT VALIDATION")
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

print("\n" + "="*70)
print("STEP 4.2: SCHEMA VALIDATION (using FRAMEWORK)")
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
                print(f"\n   MISSING IN TARGET:")
                for col in missing:
                    print(f"      - {col}")
        
        if 'extra_in_target' in result.details:
            extra = result.details['extra_in_target']
            print(f"   Extra in target: {len(extra)}")
            if extra:
                print(f"\n   EXTRA IN TARGET:")
                for col in extra:
                    print(f"      - {col}")
        
        if 'type_mismatches' in result.details:
            mismatches = result.details['type_mismatches']
            print(f"   Type mismatches: {len(mismatches)}")
            if mismatches:
                print(f"\n   TYPE MISMATCHES:")
                for col, types in mismatches.items():
                    print(f"      - {col}:")
                    print(f"          Source: {types['source']}")
                    print(f"          Target: {types['target']}")
    
    has_issues = result.status == "FAILED"
    print(f"\nStatus: {result.status}")
    
except Exception as e:
    print(f"\n❌ ERROR during schema validation: {str(e)}")
    print(f"   This may indicate a framework issue or configuration problem")
    has_issues = True
    import traceback
    traceback.print_exc()

print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 3: Primary Key Duplicates

# COMMAND ----------

print("\n" + "="*70)
print("STEP 4.3: PRIMARY KEY DUPLICATE CHECK")
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
    print("\n" + "="*70)
    print("STEP 4.4: DATA RECONCILIATION")
    print("="*70)
    print("SKIPPED (user selected N)")
    print("="*70)
    src_extras = 0
    tgt_extras = 0
    matches = 0
else:
    print("\n" + "="*70)
    print("STEP 4.4: DATA RECONCILIATION (Full Row Hash)")
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
schema_passed = result.status == "PASSED" if 'result' in locals() else False
if schema_passed:
    checks_passed.append("Schema")
schema_status = result.status if 'result' in locals() else "FAILED"

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
print("STEP 5: VALIDATION SUMMARY REPORT")
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
