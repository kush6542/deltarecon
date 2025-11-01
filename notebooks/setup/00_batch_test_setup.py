# Databricks notebook source
"""
Fresh Batch-Level Auditing Test Setup

Purpose: Test batch-level auditing with clean, controlled test data
Schema Reference: docs/INGESTION_SCHEMAS.md

Test Configuration:
- 2 groups: batch_test_group1, batch_test_group2
- 2 tables per group (4 total)
- 5 batches per table (20 total batches)
- 100 rows per batch
"""

# COMMAND ----------

# DBTITLE 1,Imports and Constants
from deltarecon.config import constants
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import lit
from datetime import datetime, timedelta

print("="*80)
print("BATCH-LEVEL AUDITING TEST SETUP")
print("="*80)
print(f"Framework Version: {constants.FRAMEWORK_VERSION}")
print(f"Batch-Level Auditing: {constants.BATCH_LEVEL_AUDITING}")
print()

# COMMAND ----------

# DBTITLE 1,Test Configuration
# Unique DBFS path for this test
TEST_DBFS_PATH = "/FileStore/deltarecon_batch_test_v2"
ORC_BASE_PATH = f"{TEST_DBFS_PATH}/orc"

# Target catalog and schema
TARGET_CATALOG = "ts42_demo"
TARGET_SCHEMA = "batch_validation_test"

# Test groups and tables
TEST_CONFIG = {
    "batch_test_group1": {
        "tables": [
            {"name": "orders", "pk": "order_id"},
            {"name": "customers", "pk": "customer_id"}
        ]
    },
    "batch_test_group2": {
        "tables": [
            {"name": "products", "pk": "product_id"},
            {"name": "shipments", "pk": "shipment_id"}
        ]
    }
}

BATCHES_PER_TABLE = 5
ROWS_PER_BATCH = 100

print(f"DBFS Path: {TEST_DBFS_PATH}")
print(f"Target: {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Groups: {len(TEST_CONFIG)}")
print(f"Tables: {sum(len(g['tables']) for g in TEST_CONFIG.values())}")
print(f"Batches: {sum(len(g['tables']) for g in TEST_CONFIG.values()) * BATCHES_PER_TABLE}")
print(f"Total Rows: {sum(len(g['tables']) for g in TEST_CONFIG.values()) * BATCHES_PER_TABLE * ROWS_PER_BATCH}")

# COMMAND ----------

# DBTITLE 1,Cleanup Previous Test Data
print("Cleaning up previous test data...")

# Drop target schema
spark.sql(f"DROP SCHEMA IF EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA} CASCADE")

# Clean DBFS
dbutils.fs.rm(f"dbfs:{TEST_DBFS_PATH}", True)

# Clean ingestion metadata
spark.sql(f"""
    DELETE FROM {constants.INGESTION_CONFIG_TABLE} 
    WHERE group_name LIKE 'batch_test_group%'
""")

spark.sql(f"""
    DELETE FROM {constants.INGESTION_AUDIT_TABLE} 
    WHERE group_name LIKE 'batch_test_group%'
""")

spark.sql(f"""
    DELETE FROM {constants.INGESTION_METADATA_TABLE} 
    WHERE table_name LIKE '{TARGET_CATALOG}.{TARGET_SCHEMA}.%'
""")

# Clean validation metadata
spark.sql(f"""
    DELETE FROM {constants.VALIDATION_LOG_TABLE} 
    WHERE iteration_name LIKE 'batch_test%'
""")

spark.sql(f"""
    DELETE FROM {constants.VALIDATION_SUMMARY_TABLE} 
    WHERE iteration_name LIKE 'batch_test%'
""")

print("✓ Cleanup complete")

# COMMAND ----------

# DBTITLE 1,Create Target Schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"✓ Created schema: {TARGET_CATALOG}.{TARGET_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Helper Functions
def generate_batch_id(base_date, batch_num):
    """Generate batch ID: BATCH_YYYYMMDD_HHMMSS"""
    date = base_date + timedelta(hours=batch_num)
    return f"BATCH_{date.strftime('%Y%m%d_%H%M%S')}"

def generate_test_data(table_name, batch_num, rows_per_batch):
    """Generate simple test data"""
    data = []
    base_offset = batch_num * rows_per_batch
    
    for i in range(rows_per_batch):
        row_id = base_offset + i + 1
        
        if table_name == "orders":
            data.append((row_id, f"ORDER_{row_id:05d}", row_id * 10.5, "ACTIVE"))
        elif table_name == "customers":
            data.append((row_id, f"Customer_{row_id}", f"email_{row_id}@test.com", "US"))
        elif table_name == "products":
            data.append((row_id, f"Product_{row_id}", row_id * 5.99, "Electronics"))
        elif table_name == "shipments":
            data.append((row_id, f"SHIP_{row_id:05d}", row_id % 100 + 1, "DELIVERED"))
    
    return data

def get_schema(table_name):
    """Get schema for table"""
    schemas = {
        "orders": StructType([
            StructField("order_id", IntegerType(), False),
            StructField("order_number", StringType(), True),
            StructField("amount", LongType(), True),
            StructField("status", StringType(), True)
        ]),
        "customers": StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("country", StringType(), True)
        ]),
        "products": StructType([
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), True),
            StructField("price", LongType(), True),
            StructField("category", StringType(), True)
        ]),
        "shipments": StructType([
            StructField("shipment_id", IntegerType(), False),
            StructField("tracking_number", StringType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("status", StringType(), True)
        ])
    }
    return schemas[table_name]

print("✓ Helper functions defined")

# COMMAND ----------

# DBTITLE 1,Create Test Data (ORC + Delta)
base_date = datetime(2025, 1, 1, 0, 0, 0)
all_batches = []

print("\n" + "="*80)
print("CREATING TEST DATA")
print("="*80)

for group_name, group_config in TEST_CONFIG.items():
    print(f"\nGROUP: {group_name}")
    
    for table_info in group_config["tables"]:
        table_name = table_info["name"]
        full_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table_name}"
        
        print(f"  Table: {table_name}")
        
        # Drop table if exists
        spark.sql(f"DROP TABLE IF EXISTS {full_table}")
        
        for batch_num in range(BATCHES_PER_TABLE):
            batch_id = generate_batch_id(base_date, batch_num)
            
            # Generate data
            data = generate_test_data(table_name, batch_num, ROWS_PER_BATCH)
            schema = get_schema(table_name)
            
            # Write ORC file
            orc_path = f"{ORC_BASE_PATH}/{group_name}/{table_name}/{batch_id}/data.orc"
            df = spark.createDataFrame(data, schema)
            df.write.mode("overwrite").format("orc").save(orc_path)
            
            # Write Delta table (with audit column)
            df_with_audit = df.withColumn("_aud_batch_load_id", lit(batch_id))
            
            if batch_num == 0:
                df_with_audit.write.format("delta").mode("overwrite").saveAsTable(full_table)
            else:
                df_with_audit.write.format("delta").mode("append").saveAsTable(full_table)
            
            # Track batch info
            all_batches.append({
                "group": group_name,
                "table": table_name,
                "full_table": full_table,
                "batch_id": batch_id,
                "orc_path": orc_path,
                "rows": ROWS_PER_BATCH,
                "pk": table_info["pk"]
            })
            
            print(f"    ✓ {batch_id}: {ROWS_PER_BATCH} rows")

print(f"\n✓ Created {len(all_batches)} batches across {len([t for g in TEST_CONFIG.values() for t in g['tables']])} tables")

# COMMAND ----------

# DBTITLE 1,Populate ingestion_config
print("\nPopulating ingestion_config...")

config_inserts = 0
for group_name, group_config in TEST_CONFIG.items():
    for table_info in group_config["tables"]:
        table_name = table_info["name"]
        
        # Reference: docs/INGESTION_SCHEMAS.md - Section 1
        spark.sql(f"""
            INSERT INTO {constants.INGESTION_CONFIG_TABLE}
            (group_name, source_schema, source_table, target_catalog, target_schema,
             target_table, source_file_format, write_mode, primary_key, is_active)
            VALUES (
                '{group_name}',
                'source_system',
                'src_{table_name}',
                '{TARGET_CATALOG}',
                '{TARGET_SCHEMA}',
                '{table_name}',
                'orc',
                'append',
                '{table_info["pk"]}',
                'Y'
            )
        """)
        config_inserts += 1

print(f"✓ Inserted {config_inserts} config records")

# COMMAND ----------

# DBTITLE 1,Populate ingestion_audit
print("\nPopulating ingestion_audit...")

# Reference: docs/INGESTION_SCHEMAS.md - Section 2
for idx, batch_info in enumerate(all_batches):
    spark.sql(f"""
        INSERT INTO {constants.INGESTION_AUDIT_TABLE}
        (run_id, config_id, batch_load_id, group_name, target_table_name,
         operation_type, load_type, status, start_ts, end_ts, row_count)
        VALUES (
            'RUN_{batch_info["batch_id"]}',
            'CFG_{idx:05d}',
            '{batch_info["batch_id"]}',
            '{batch_info["group"]}',
            '{batch_info["full_table"]}',
            'LOAD',
            'BATCH',
            'COMPLETED',
            current_timestamp(),
            current_timestamp(),
            {batch_info["rows"]}
        )
    """)

print(f"✓ Inserted {len(all_batches)} audit records")

# COMMAND ----------

# DBTITLE 1,Populate ingestion_metadata
print("\nPopulating ingestion_metadata...")

# Reference: docs/INGESTION_SCHEMAS.md - Section 3
for batch_info in all_batches:
    spark.sql(f"""
        INSERT INTO {constants.INGESTION_METADATA_TABLE}
        (table_name, batch_load_id, source_file_path)
        VALUES (
            '{batch_info["full_table"]}',
            '{batch_info["batch_id"]}',
            '{batch_info["orc_path"]}'
        )
    """)

print(f"✓ Inserted {len(all_batches)} metadata records")

# COMMAND ----------

# DBTITLE 1,Verification
print("\n" + "="*80)
print("VERIFICATION")
print("="*80)

# Check config
config_count = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {constants.INGESTION_CONFIG_TABLE}
    WHERE group_name LIKE 'batch_test_group%'
""").first()['cnt']
print(f"\n✓ Config records: {config_count}")

# Check audit
print("\nAudit summary by group:")
spark.sql(f"""
    SELECT 
        group_name,
        COUNT(DISTINCT target_table_name) as tables,
        COUNT(*) as batches
    FROM {constants.INGESTION_AUDIT_TABLE}
    WHERE group_name LIKE 'batch_test_group%'
    GROUP BY group_name
    ORDER BY group_name
""").show()

# Check metadata
print("Metadata summary by table:")
spark.sql(f"""
    SELECT 
        table_name,
        COUNT(*) as orc_files,
        COUNT(DISTINCT batch_load_id) as batches
    FROM {constants.INGESTION_METADATA_TABLE}
    WHERE table_name LIKE '{TARGET_CATALOG}.{TARGET_SCHEMA}.%'
    GROUP BY table_name
    ORDER BY table_name
""").show(truncate=False)

# Check target tables
print("Target Delta tables:")
for group_name, group_config in TEST_CONFIG.items():
    for table_info in group_config["tables"]:
        full_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table_info['name']}"
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table}").first()['cnt']
        batches = spark.sql(f"SELECT COUNT(DISTINCT _aud_batch_load_id) as cnt FROM {full_table}").first()['cnt']
        print(f"  {full_table}: {count} rows, {batches} batches")

# COMMAND ----------

# DBTITLE 1,Sample Data
print("\n" + "="*80)
print("SAMPLE DATA")
print("="*80)

for group_name, group_config in TEST_CONFIG.items():
    print(f"\nGroup: {group_name}")
    for table_info in group_config["tables"]:
        full_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table_info['name']}"
        print(f"\n  {table_info['name']}:")
        spark.sql(f"SELECT * FROM {full_table} LIMIT 3").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Test Summary
print("\n" + "="*80)
print("TEST SETUP COMPLETE!")
print("="*80)

summary_stats = {
    "Groups": len(TEST_CONFIG),
    "Tables": sum(len(g['tables']) for g in TEST_CONFIG.values()),
    "Batches per Table": BATCHES_PER_TABLE,
    "Total Batches": len(all_batches),
    "Rows per Batch": ROWS_PER_BATCH,
    "Total Rows": len(all_batches) * ROWS_PER_BATCH
}

for key, value in summary_stats.items():
    print(f"  {key}: {value}")

print(f"\nTest Configuration:")
for group_name, group_config in TEST_CONFIG.items():
    print(f"\n  {group_name}:")
    for table_info in group_config["tables"]:
        print(f"    - {TARGET_CATALOG}.{TARGET_SCHEMA}.{table_info['name']} (PK: {table_info['pk']})")

print("\n" + "="*80)
print("NEXT STEPS")
print("="*80)

print("""
1. Run validation on batch_test_group1:

   from deltarecon import ValidationRunner
   
   runner = ValidationRunner(
       spark=spark,
       table_group="batch_test_group1",
       iteration_suffix="batch_test"
   )
   
   result = runner.run()
   print(f"Success: {result['success']}, Failed: {result['failed']}")

2. Check validation_log (expect 10 entries - 2 tables × 5 batches):

   SELECT batch_load_id, tgt_table, validation_run_status
   FROM cat_ril_nayeem_03.validation_v2.validation_log
   WHERE iteration_name LIKE 'batch_test%'
   ORDER BY tgt_table, batch_load_id

3. Check validation_summary (expect 10 entries):

   SELECT batch_load_id, tgt_table, overall_status, 
          metrics.src_records, metrics.tgt_records
   FROM cat_ril_nayeem_03.validation_v2.validation_summary
   WHERE iteration_name LIKE 'batch_test%'
   ORDER BY tgt_table, batch_load_id

4. Run validation on batch_test_group2:

   runner = ValidationRunner(
       spark=spark,
       table_group="batch_test_group2",
       iteration_suffix="batch_test"
   )
   result = runner.run()
""")

# COMMAND ----------

# DBTITLE 1,Quick Test - Run Validation
print("\n" + "="*80)
print("RUNNING VALIDATION TEST - batch_test_group1")
print("="*80)

from deltarecon import ValidationRunner

try:
    runner = ValidationRunner(
        spark=spark,
        table_group="batch_test_group1",
        iteration_suffix="batch_test"
    )
    
    result = runner.run()
    
    print("\n" + "="*80)
    print("VALIDATION RESULTS")
    print("="*80)
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
    
    # Show validation results
    print("\n" + "="*80)
    print("VALIDATION LOG ENTRIES")
    print("="*80)
    spark.sql(f"""
        SELECT batch_load_id, tgt_table, validation_run_status
        FROM {constants.VALIDATION_LOG_TABLE}
        WHERE iteration_name LIKE 'batch_test%'
        ORDER BY tgt_table, batch_load_id
    """).show(truncate=False)
    
    print("\n" + "="*80)
    print("TEST COMPLETED SUCCESSFULLY!")
    print("="*80)
    
except Exception as e:
    print(f"\n❌ VALIDATION FAILED: {str(e)}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# DBTITLE 1,Cleanup (Optional - Run manually if needed)
# Uncomment to clean up all test data

# print("Cleaning up test data...")
# 
# spark.sql(f"DROP SCHEMA IF EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA} CASCADE")
# dbutils.fs.rm(f"dbfs:{TEST_DBFS_PATH}", True)
# 
# spark.sql(f"DELETE FROM {constants.INGESTION_CONFIG_TABLE} WHERE group_name LIKE 'batch_test_group%'")
# spark.sql(f"DELETE FROM {constants.INGESTION_AUDIT_TABLE} WHERE group_name LIKE 'batch_test_group%'")
# spark.sql(f"DELETE FROM {constants.INGESTION_METADATA_TABLE} WHERE table_name LIKE '{TARGET_CATALOG}.{TARGET_SCHEMA}.%'")
# spark.sql(f"DELETE FROM {constants.VALIDATION_LOG_TABLE} WHERE iteration_name LIKE 'batch_test%'")
# spark.sql(f"DELETE FROM {constants.VALIDATION_SUMMARY_TABLE} WHERE iteration_name LIKE 'batch_test%'")
# 
# print("✓ Cleanup complete")

