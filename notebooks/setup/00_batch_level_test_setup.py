# Databricks notebook source
"""
Batch-Level Auditing Test Setup

Creates a simple test scenario to verify batch-level auditing:
- 2 table groups: test_group1, test_group2
- 2 tables per group (4 tables total)
- 5 batches per table (20 batches total)

Focus: Test orchestration and batch-level tracking
"""

# COMMAND ----------

# DBTITLE 1,Load Constants and Imports
from deltarecon.config import constants
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col, lit
from datetime import datetime, timedelta

print(f"Framework Version: {constants.FRAMEWORK_VERSION}")
print(f"Batch-Level Auditing: {constants.BATCH_LEVEL_AUDITING}")
print(f"Ingestion Schema: {constants.INGESTION_OPS_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Setup: Create Catalogs and Schemas
# Create ingestion operations catalog and schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {constants.INGESTION_OPS_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {constants.INGESTION_OPS_SCHEMA}")

# Create target catalog and schema
target_catalog = "ts42_demo"
target_schema = "batch_test"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")

print(f"✓ Catalogs and schemas ready")
print(f"  Ingestion: {constants.INGESTION_OPS_SCHEMA}")
print(f"  Target: {target_catalog}.{target_schema}")

# COMMAND ----------

# DBTITLE 1,Setup: Base Paths for ORC Files
dbfs_base_path = "/FileStore/deltarecon_batch_test"
orc_base_path = f"{dbfs_base_path}/orc_files"

print(f"ORC base path: dbfs:{orc_base_path}")

# Clean up old test data
dbutils.fs.rm(f"dbfs:{dbfs_base_path}", True)
print(f"✓ Cleaned up old test data")

# COMMAND ----------

# DBTITLE 1,Test Configuration
# Define test tables
TEST_TABLES = {
    "test_group1": [
        {
            "name": "orders",
            "source_name": "source_orders",
            "columns": ["order_id", "customer_id", "amount", "status"],
            "pk": "order_id"
        },
        {
            "name": "customers", 
            "source_name": "source_customers",
            "columns": ["customer_id", "name", "email", "region"],
            "pk": "customer_id"
        }
    ],
    "test_group2": [
        {
            "name": "products",
            "source_name": "source_products", 
            "columns": ["product_id", "product_name", "price", "category"],
            "pk": "product_id"
        },
        {
            "name": "inventory",
            "source_name": "source_inventory",
            "columns": ["inventory_id", "product_id", "quantity", "warehouse"],
            "pk": "inventory_id"
        }
    ]
}

BATCHES_PER_TABLE = 5
ROWS_PER_BATCH = 100

print(f"Test Configuration:")
print(f"  Groups: {len(TEST_TABLES)}")
print(f"  Tables per group: 2")
print(f"  Batches per table: {BATCHES_PER_TABLE}")
print(f"  Rows per batch: {ROWS_PER_BATCH}")
print(f"  Total batches: {len(TEST_TABLES) * 2 * BATCHES_PER_TABLE}")

# COMMAND ----------

# DBTITLE 1,Helper Functions
def generate_batch_id(base_date, batch_num):
    """Generate batch ID with timestamp"""
    date = base_date + timedelta(hours=batch_num)
    return f"BATCH_{date.strftime('%Y%m%d_%H%M%S')}"

def generate_sample_data(table_config, batch_num, rows_per_batch):
    """Generate simple test data for a table"""
    data = []
    base_offset = batch_num * rows_per_batch
    
    for i in range(rows_per_batch):
        row_id = base_offset + i + 1
        
        if table_config["name"] == "orders":
            data.append((row_id, row_id % 20 + 1, row_id * 10.5, "ACTIVE"))
        elif table_config["name"] == "customers":
            data.append((row_id, f"Customer_{row_id}", f"cust{row_id}@test.com", f"Region_{row_id % 5}"))
        elif table_config["name"] == "products":
            data.append((row_id, f"Product_{row_id}", row_id * 5.99, f"Category_{row_id % 3}"))
        elif table_config["name"] == "inventory":
            data.append((row_id, row_id % 50 + 1, row_id * 10, f"Warehouse_{row_id % 3}"))
    
    return data

def create_schema(columns):
    """Create schema for table"""
    fields = []
    for col_name in columns:
        if col_name.endswith("_id"):
            fields.append(StructField(col_name, IntegerType(), False))
        elif col_name in ["amount", "price"]:
            fields.append(StructField(col_name, LongType(), True))
        elif col_name == "quantity":
            fields.append(StructField(col_name, IntegerType(), True))
        else:
            fields.append(StructField(col_name, StringType(), True))
    return StructType(fields)

print("✓ Helper functions defined")

# COMMAND ----------

# DBTITLE 1,Create Test Data for All Tables and Batches
base_date = datetime(2025, 1, 1, 0, 0, 0)
all_batches_created = []

for group_name, tables in TEST_TABLES.items():
    print(f"\n{'='*60}")
    print(f"GROUP: {group_name}")
    print(f"{'='*60}")
    
    for table_config in tables:
        table_name = table_config["name"]
        full_table_name = f"{target_catalog}.{target_schema}.{table_name}"
        
        print(f"\nTable: {table_name}")
        print(f"  Full name: {full_table_name}")
        
        # Drop and recreate target Delta table
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        
        # Create batches for this table
        for batch_num in range(BATCHES_PER_TABLE):
            batch_id = generate_batch_id(base_date, batch_num)
            
            # Generate sample data
            data = generate_sample_data(table_config, batch_num, ROWS_PER_BATCH)
            schema = create_schema(table_config["columns"])
            
            # Create source ORC file
            source_df = spark.createDataFrame(data, schema)
            orc_path = f"{orc_base_path}/{group_name}/{table_name}/{batch_id}/data.orc"
            source_df.write.mode("overwrite").format("orc").save(orc_path)
            
            # Create/append to target Delta table with audit column
            target_df = source_df.withColumn("_aud_batch_load_id", lit(batch_id))
            
            if batch_num == 0:
                target_df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
            else:
                target_df.write.format("delta").mode("append").saveAsTable(full_table_name)
            
            all_batches_created.append({
                "group": group_name,
                "table": table_name,
                "batch_id": batch_id,
                "rows": ROWS_PER_BATCH,
                "orc_path": orc_path
            })
            
            print(f"    ✓ Batch {batch_num + 1}/{BATCHES_PER_TABLE}: {batch_id} ({ROWS_PER_BATCH} rows)")

print(f"\n{'='*60}")
print(f"✓ Created {len(all_batches_created)} batches across {len(TEST_TABLES) * 2} tables")

# COMMAND ----------

# DBTITLE 1,Create Ingestion Metadata Tables (if not exist)
# Create ingestion_config table
config_ddl = f"""
CREATE TABLE IF NOT EXISTS {constants.INGESTION_CONFIG_TABLE} (
  group_name STRING,
  target_catalog STRING,
  target_schema STRING,
  target_table STRING,
  source_schema STRING,
  source_table STRING,
  primary_key STRING,
  partition_column STRING,
  write_mode STRING,
  is_active STRING
)
USING DELTA
"""
spark.sql(config_ddl)

# Create ingestion_audit table
audit_ddl = f"""
CREATE TABLE IF NOT EXISTS {constants.INGESTION_AUDIT_TABLE} (
  batch_load_id STRING,
  target_table_name STRING,
  status STRING,
  group_name STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP
)
USING DELTA
"""
spark.sql(audit_ddl)

# Create ingestion_metadata table
metadata_ddl = f"""
CREATE TABLE IF NOT EXISTS {constants.INGESTION_METADATA_TABLE} (
  table_name STRING,
  batch_load_id STRING,
  source_file_path STRING,
  record_count BIGINT
)
USING DELTA
"""
spark.sql(metadata_ddl)

print("✓ Ingestion metadata tables ready")

# COMMAND ----------

# DBTITLE 1,Populate Ingestion Config Table
print("Populating ingestion_config...")

# Clear existing test data
spark.sql(f"DELETE FROM {constants.INGESTION_CONFIG_TABLE} WHERE group_name LIKE 'test_group%'")

# Insert config for all test tables
config_records = []
for group_name, tables in TEST_TABLES.items():
    for table_config in tables:
        config_records.append((
            group_name,
            target_catalog,
            target_schema,
            table_config["name"],
            "source_system",
            table_config["source_name"],
            table_config["pk"],
            None,  # No partitions for simplicity
            "append",  # All append mode
            "Y"
        ))

config_schema = StructType([
    StructField("group_name", StringType()),
    StructField("target_catalog", StringType()),
    StructField("target_schema", StringType()),
    StructField("target_table", StringType()),
    StructField("source_schema", StringType()),
    StructField("source_table", StringType()),
    StructField("primary_key", StringType()),
    StructField("partition_column", StringType()),
    StructField("write_mode", StringType()),
    StructField("is_active", StringType())
])

config_df = spark.createDataFrame(config_records, config_schema)
config_df.write.format("delta").mode("append").saveAsTable(constants.INGESTION_CONFIG_TABLE)

print(f"✓ Inserted {len(config_records)} config records")

# COMMAND ----------

# DBTITLE 1,Populate Ingestion Audit Table
print("Populating ingestion_audit...")

# Clear existing test data
spark.sql(f"DELETE FROM {constants.INGESTION_AUDIT_TABLE} WHERE group_name LIKE 'test_group%'")

# Insert audit records for all batches
audit_records = []
for batch_info in all_batches_created:
    full_table_name = f"{target_catalog}.{target_schema}.{batch_info['table']}"
    audit_records.append((
        batch_info["batch_id"],
        full_table_name,
        "COMPLETED",
        batch_info["group"],
        datetime.now(),
        datetime.now()
    ))

audit_schema = StructType([
    StructField("batch_load_id", StringType()),
    StructField("target_table_name", StringType()),
    StructField("status", StringType()),
    StructField("group_name", StringType()),
    StructField("start_time", StringType()),
    StructField("end_time", StringType())
])

audit_df = spark.createDataFrame(audit_records, audit_schema)
audit_df.write.format("delta").mode("append").saveAsTable(constants.INGESTION_AUDIT_TABLE)

print(f"✓ Inserted {len(audit_records)} audit records")

# COMMAND ----------

# DBTITLE 1,Populate Ingestion Metadata Table
print("Populating ingestion_metadata...")

# Clear existing test data
spark.sql(f"DELETE FROM {constants.INGESTION_METADATA_TABLE} WHERE table_name LIKE '{target_catalog}.{target_schema}.%'")

# Insert metadata records for all ORC files
metadata_records = []
for batch_info in all_batches_created:
    full_table_name = f"{target_catalog}.{target_schema}.{batch_info['table']}"
    metadata_records.append((
        full_table_name,
        batch_info["batch_id"],
        batch_info["orc_path"],
        batch_info["rows"]
    ))

metadata_schema = StructType([
    StructField("table_name", StringType()),
    StructField("batch_load_id", StringType()),
    StructField("source_file_path", StringType()),
    StructField("record_count", LongType())
])

metadata_df = spark.createDataFrame(metadata_records, metadata_schema)
metadata_df.write.format("delta").mode("append").saveAsTable(constants.INGESTION_METADATA_TABLE)

print(f"✓ Inserted {len(metadata_records)} metadata records")

# COMMAND ----------

# DBTITLE 1,Verification Queries
print("\n" + "="*60)
print("VERIFICATION")
print("="*60)

# Check ingestion_config
config_count = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {constants.INGESTION_CONFIG_TABLE} 
    WHERE group_name LIKE 'test_group%'
""").first()['cnt']
print(f"\n✓ Ingestion Config: {config_count} tables configured")

# Check ingestion_audit
audit_summary = spark.sql(f"""
    SELECT 
        group_name,
        COUNT(DISTINCT target_table_name) as tables,
        COUNT(*) as batches
    FROM {constants.INGESTION_AUDIT_TABLE}
    WHERE group_name LIKE 'test_group%'
    GROUP BY group_name
    ORDER BY group_name
""")
print("\n✓ Ingestion Audit Summary:")
audit_summary.show()

# Check ingestion_metadata
metadata_summary = spark.sql(f"""
    SELECT 
        table_name,
        COUNT(*) as orc_files,
        SUM(record_count) as total_rows
    FROM {constants.INGESTION_METADATA_TABLE}
    WHERE table_name LIKE '{target_catalog}.{target_schema}.%'
    GROUP BY table_name
    ORDER BY table_name
""")
print("\n✓ Ingestion Metadata Summary:")
metadata_summary.show()

# Check target tables
print("\n✓ Target Delta Tables:")
for group_name, tables in TEST_TABLES.items():
    for table_config in tables:
        full_table_name = f"{target_catalog}.{target_schema}.{table_config['name']}"
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}").first()['cnt']
        batch_count = spark.sql(f"SELECT COUNT(DISTINCT _aud_batch_load_id) as cnt FROM {full_table_name}").first()['cnt']
        print(f"  {full_table_name}: {count} rows, {batch_count} batches")

# COMMAND ----------

# DBTITLE 1,Sample Data from Each Table
print("\n" + "="*60)
print("SAMPLE DATA")
print("="*60)

for group_name, tables in TEST_TABLES.items():
    print(f"\nGroup: {group_name}")
    for table_config in tables:
        full_table_name = f"{target_catalog}.{target_schema}.{table_config['name']}"
        print(f"\n  Table: {table_config['name']}")
        spark.sql(f"SELECT * FROM {full_table_name} LIMIT 3").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Test Summary
print("\n" + "="*60)
print("TEST SETUP COMPLETE!")
print("="*60)
print(f"""
Setup Summary:
  - Groups Created: {len(TEST_TABLES)}
  - Tables Created: {len(TEST_TABLES) * 2}
  - Batches per Table: {BATCHES_PER_TABLE}
  - Total Batches: {len(all_batches_created)}
  - Rows per Batch: {ROWS_PER_BATCH}
  - Total Rows: {len(all_batches_created) * ROWS_PER_BATCH}

Tables by Group:
""")

for group_name, tables in TEST_TABLES.items():
    print(f"\n  {group_name}:")
    for table_config in tables:
        full_table_name = f"{target_catalog}.{target_schema}.{table_config['name']}"
        print(f"    - {full_table_name}")

print(f"""
Next Steps:
  1. Run validation for test_group1:
     
     from deltarecon import ValidationRunner
     
     runner = ValidationRunner(
         spark=spark,
         table_group="test_group1",
         iteration_suffix="batch_test"
     )
     
     result = runner.run()
     print(result)
  
  2. Check validation_log table:
     
     SELECT batch_load_id, tgt_table, validation_run_status
     FROM {constants.VALIDATION_LOG_TABLE}
     WHERE iteration_name LIKE 'batch_test%'
     ORDER BY tgt_table, batch_load_id
  
  3. Check validation_summary table:
     
     SELECT batch_load_id, tgt_table, overall_status, metrics.*
     FROM {constants.VALIDATION_SUMMARY_TABLE}
     WHERE iteration_name LIKE 'batch_test%'
     ORDER BY tgt_table, batch_load_id
""")

# COMMAND ----------

# DBTITLE 1,Quick Test - Run Validation on test_group1
print("\n" + "="*60)
print("RUNNING VALIDATION TEST")
print("="*60)

from deltarecon import ValidationRunner

try:
    runner = ValidationRunner(
        spark=spark,
        table_group="test_group1",
        iteration_suffix="batch_test"
    )
    
    result = runner.run()
    
    print("\n" + "="*60)
    print("VALIDATION RESULTS")
    print("="*60)
    print(f"Total tables: {result['total']}")
    print(f"Success: {result['success']}")
    print(f"Failed: {result['failed']}")
    print(f"No batches: {result['no_batches']}")
    
    print("\nDetailed Results:")
    for r in result['results']:
        print(f"\n  Table: {r['table']}")
        print(f"    Status: {r['status']}")
        print(f"    Batches Processed: {r.get('batches_processed', 0)}")
        print(f"    Batches Succeeded: {r.get('batches_succeeded', 0)}")
        print(f"    Batches Failed: {r.get('batches_failed', 0)}")
    
    print("\n" + "="*60)
    print("TEST COMPLETED SUCCESSFULLY!")
    print("="*60)
    
except Exception as e:
    print(f"\n❌ VALIDATION FAILED: {str(e)}")
    import traceback
    traceback.print_exc()

