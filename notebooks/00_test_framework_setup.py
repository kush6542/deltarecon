# Databricks notebook source
"""
DeltaRecon Framework Test Setup
================================

This notebook creates a complete test environment for the validation framework:
1. Creates test schemas and tables (source + target)
2. Generates sample data with controlled scenarios (matches, mismatches, extras)
3. Creates ORC files in DBFS
4. Populates ingestion metadata tables
5. Ready for validation testing

Test Scenarios:
- Table 1: orders (Perfect match scenario)
- Table 2: customers (Schema mismatch scenario)  
- Table 3: products (Data mismatch scenario)

After running this, manually execute:
1. 02_setup_validation_mapping.py - to populate validation_mapping
2. Create validation jobs
3. Run validations to test metrics collection
"""

# COMMAND ----------

# DBTITLE 1,Test Configuration
TEST_CONFIG = {
    # Test environment settings
    "test_catalog": "ts42_demo",
    "test_schema": "deltarecon_test",
    "source_schema": "deltarecon_test_source",
    "test_dbfs_path": "dbfs:/FileStore/deltarecon_test_data",
    "table_group": "test_group_validation",
    
    # Batch configuration
    "batches": [
        {
            "batch_id": "BATCH_20251102_000001",
            "run_id": "RUN_20251102_000001",
            "row_count": 100
        },
        {
            "batch_id": "BATCH_20251102_000002", 
            "run_id": "RUN_20251102_000002",
            "row_count": 50
        }
    ],
    
    # Tables to create with test scenarios
    "tables": [
        {
            "name": "orders",
            "primary_key": "order_id",
            "scenario": "perfect_match",
            "description": "All validations should PASS"
        },
        {
            "name": "customers",
            "primary_key": "customer_id",
            "scenario": "schema_mismatch",
            "description": "Schema validation should FAIL (missing column in target)"
        },
        {
            "name": "products",
            "primary_key": "product_id",
            "scenario": "data_mismatch",
            "description": "Data reconciliation should detect mismatches"
        }
    ]
}

print("=" * 80)
print("DELTARECON FRAMEWORK TEST SETUP")
print("=" * 80)
print(f"Test Catalog: {TEST_CONFIG['test_catalog']}")
print(f"Test Schema: {TEST_CONFIG['test_schema']}")
print(f"Source Schema: {TEST_CONFIG['source_schema']}")
print(f"DBFS Path: {TEST_CONFIG['test_dbfs_path']}")
print(f"Table Group: {TEST_CONFIG['table_group']}")
print(f"Number of Batches: {len(TEST_CONFIG['batches'])}")
print(f"Number of Tables: {len(TEST_CONFIG['tables'])}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import uuid

# Import framework constants
from deltarecon.config import constants

print(f"Framework Version: {constants.FRAMEWORK_VERSION}")
print(f"Ingestion Config Table: {constants.INGESTION_CONFIG_TABLE}")
print(f"Ingestion Audit Table: {constants.INGESTION_AUDIT_TABLE}")
print(f"Ingestion Metadata Table: {constants.INGESTION_METADATA_TABLE}")

# COMMAND ----------

# DBTITLE 1,Step 1: Create Schemas
print("\n" + "="*80)
print("STEP 1: Creating Test Schemas")
print("="*80)

# Create target schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_CONFIG['test_catalog']}.{TEST_CONFIG['test_schema']}")
print(f"✓ Created target schema: {TEST_CONFIG['test_catalog']}.{TEST_CONFIG['test_schema']}")

# Create source schema (simulating external system)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_CONFIG['test_catalog']}.{TEST_CONFIG['source_schema']}")
print(f"✓ Created source schema: {TEST_CONFIG['test_catalog']}.{TEST_CONFIG['source_schema']}")

# COMMAND ----------

# DBTITLE 1,Step 2: Create Test Tables - ORDERS (Perfect Match)

print("\n" + "="*80)
print("STEP 2: Creating Test Tables")
print("="*80)

# ============================================================================
# TABLE 1: ORDERS (Perfect Match Scenario)
# ============================================================================
print("\n--- Creating ORDERS Table (Perfect Match Scenario) ---")

# Source table DDL
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TEST_CONFIG['test_catalog']}.{TEST_CONFIG['source_schema']}.orders (
    order_id BIGINT COMMENT 'Unique order identifier',
    customer_id BIGINT COMMENT 'Customer who placed the order',
    order_date DATE COMMENT 'Date order was placed',
    order_amount DECIMAL(10,2) COMMENT 'Total order amount',
    order_status STRING COMMENT 'Order status',
    product_category STRING COMMENT 'Product category',
    shipping_city STRING COMMENT 'Shipping city',
    payment_method STRING COMMENT 'Payment method used',
    _aud_batch_load_id STRING COMMENT 'Audit batch load ID'
) USING DELTA
COMMENT 'Orders source table - perfect match test scenario'
""")

# Target table DDL (same structure)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TEST_CONFIG['test_catalog']}.{TEST_CONFIG['test_schema']}.orders (
    order_id BIGINT COMMENT 'Unique order identifier',
    customer_id BIGINT COMMENT 'Customer who placed the order',
    order_date DATE COMMENT 'Date order was placed',
    order_amount DECIMAL(10,2) COMMENT 'Total order amount',
    order_status STRING COMMENT 'Order status',
    product_category STRING COMMENT 'Product category',
    shipping_city STRING COMMENT 'Shipping city',
    payment_method STRING COMMENT 'Payment method used',
    _aud_batch_load_id STRING COMMENT 'Audit batch load ID'
) USING DELTA
COMMENT 'Orders target table - perfect match test scenario'
""")

print("✓ Created source and target tables for ORDERS")

# COMMAND ----------

# DBTITLE 1,Step 3: Create Test Tables - CUSTOMERS (Schema Mismatch)

# ============================================================================
# TABLE 2: CUSTOMERS (Schema Mismatch Scenario)
# ============================================================================
print("\n--- Creating CUSTOMERS Table (Schema Mismatch Scenario) ---")

# Source table has email column
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TEST_CONFIG['test_catalog']}.{TEST_CONFIG['source_schema']}.customers (
    customer_id BIGINT COMMENT 'Unique customer identifier',
    customer_name STRING COMMENT 'Customer full name',
    email STRING COMMENT 'Customer email address',
    phone STRING COMMENT 'Customer phone number',
    city STRING COMMENT 'Customer city',
    state STRING COMMENT 'Customer state',
    signup_date DATE COMMENT 'Customer signup date',
    customer_segment STRING COMMENT 'Customer segment (Gold/Silver/Bronze)',
    _aud_batch_load_id STRING COMMENT 'Audit batch load ID'
) USING DELTA
COMMENT 'Customers source table - schema mismatch test scenario'
""")

# Target table missing email column (schema mismatch)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TEST_CONFIG['test_catalog']}.{TEST_CONFIG['test_schema']}.customers (
    customer_id BIGINT COMMENT 'Unique customer identifier',
    customer_name STRING COMMENT 'Customer full name',
    phone STRING COMMENT 'Customer phone number',
    city STRING COMMENT 'Customer city',
    state STRING COMMENT 'Customer state',
    signup_date DATE COMMENT 'Customer signup date',
    customer_segment STRING COMMENT 'Customer segment (Gold/Silver/Bronze)',
    _aud_batch_load_id STRING COMMENT 'Audit batch load ID'
) USING DELTA
COMMENT 'Customers target table - missing email column for schema mismatch test'
""")

print("✓ Created source and target tables for CUSTOMERS (target missing 'email' column)")

# COMMAND ----------

# DBTITLE 1,Step 4: Create Test Tables - PRODUCTS (Data Mismatch)

# ============================================================================
# TABLE 3: PRODUCTS (Data Mismatch Scenario)
# ============================================================================
print("\n--- Creating PRODUCTS Table (Data Mismatch Scenario) ---")

# Source table DDL
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TEST_CONFIG['test_catalog']}.{TEST_CONFIG['source_schema']}.products (
    product_id BIGINT COMMENT 'Unique product identifier',
    product_name STRING COMMENT 'Product name',
    category STRING COMMENT 'Product category',
    price DECIMAL(10,2) COMMENT 'Product price',
    stock_quantity INT COMMENT 'Stock quantity',
    supplier_id BIGINT COMMENT 'Supplier identifier',
    is_active BOOLEAN COMMENT 'Whether product is active',
    last_updated TIMESTAMP COMMENT 'Last update timestamp',
    _aud_batch_load_id STRING COMMENT 'Audit batch load ID'
) USING DELTA
COMMENT 'Products source table - data mismatch test scenario'
""")

# Target table DDL (same structure but will have data mismatches)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TEST_CONFIG['test_catalog']}.{TEST_CONFIG['test_schema']}.products (
    product_id BIGINT COMMENT 'Unique product identifier',
    product_name STRING COMMENT 'Product name',
    category STRING COMMENT 'Product category',
    price DECIMAL(10,2) COMMENT 'Product price',
    stock_quantity INT COMMENT 'Stock quantity',
    supplier_id BIGINT COMMENT 'Supplier identifier',
    is_active BOOLEAN COMMENT 'Whether product is active',
    last_updated TIMESTAMP COMMENT 'Last update timestamp',
    _aud_batch_load_id STRING COMMENT 'Audit batch load ID'
) USING DELTA
COMMENT 'Products target table - will have data mismatches'
""")

print("✓ Created source and target tables for PRODUCTS")

print("\n" + "="*80)
print("All test tables created successfully!")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Step 5: Generate and Load Test Data - ORDERS

print("\n" + "="*80)
print("STEP 5: Generating and Loading Test Data")
print("="*80)

# ============================================================================
# Generate ORDERS data (Perfect Match)
# ============================================================================
print("\n--- Generating ORDERS data (Perfect Match) ---")

def generate_orders_data(batch_config):
    """Generate orders data for a batch"""
    batch_id = batch_config['batch_id']
    row_count = batch_config['row_count']
    
    # Generate deterministic data based on batch_id
    seed_value = int(batch_id.split('_')[-1])
    random.seed(seed_value)
    
    orders_data = []
    base_order_id = seed_value * 1000
    
    for i in range(row_count):
        order_id = base_order_id + i
        orders_data.append({
            'order_id': order_id,
            'customer_id': random.randint(1, 50),
            'order_date': (datetime.now() - timedelta(days=random.randint(0, 365))).date(),
            'order_amount': round(random.uniform(10.0, 1000.0), 2),
            'order_status': random.choice(['PENDING', 'COMPLETED', 'SHIPPED', 'DELIVERED', 'CANCELLED']),
            'product_category': random.choice(['Electronics', 'Clothing', 'Home', 'Sports', 'Books']),
            'shipping_city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Cash']),
            '_aud_batch_load_id': batch_id
        })
    
    return orders_data

# Process each batch
for batch_config in TEST_CONFIG['batches']:
    batch_id = batch_config['batch_id']
    print(f"\nProcessing batch: {batch_id}")
    
    # Generate data
    orders_data = generate_orders_data(batch_config)
    orders_df = spark.createDataFrame(orders_data)
    
    # Write to both source and target (perfect match)
    orders_df.write.mode("append").saveAsTable(
        f"{TEST_CONFIG['test_catalog']}.{TEST_CONFIG['source_schema']}.orders"
    )
    orders_df.write.mode("append").saveAsTable(
        f"{TEST_CONFIG['test_catalog']}.{TEST_CONFIG['test_schema']}.orders"
    )
    
    print(f"  ✓ Loaded {len(orders_data)} records to source and target")

print("\n✓ ORDERS data loaded successfully (perfect match)")

# COMMAND ----------

# DBTITLE 1,Step 6: Generate and Load Test Data - CUSTOMERS

# ============================================================================
# Generate CUSTOMERS data (Schema Mismatch)
# ============================================================================
print("\n--- Generating CUSTOMERS data (Schema Mismatch) ---")

def generate_customers_data(batch_config, include_email=True):
    """Generate customers data for a batch"""
    batch_id = batch_config['batch_id']
    row_count = batch_config['row_count']
    
    seed_value = int(batch_id.split('_')[-1])
    random.seed(seed_value)
    
    customers_data = []
    base_customer_id = seed_value * 1000
    
    for i in range(row_count):
        customer_id = base_customer_id + i
        customer = {
            'customer_id': customer_id,
            'customer_name': f'Customer_{customer_id}',
            'phone': f'+1-555-{random.randint(1000, 9999)}',
            'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
            'state': random.choice(['NY', 'CA', 'IL', 'TX', 'AZ']),
            'signup_date': (datetime.now() - timedelta(days=random.randint(0, 730))).date(),
            'customer_segment': random.choice(['Gold', 'Silver', 'Bronze']),
            '_aud_batch_load_id': batch_id
        }
        
        # Add email only for source (schema mismatch)
        if include_email:
            customer['email'] = f'customer{customer_id}@example.com'
        
        customers_data.append(customer)
    
    return customers_data

# Process each batch
for batch_config in TEST_CONFIG['batches']:
    batch_id = batch_config['batch_id']
    print(f"\nProcessing batch: {batch_id}")
    
    # Generate source data (with email)
    customers_data_source = generate_customers_data(batch_config, include_email=True)
    customers_df_source = spark.createDataFrame(customers_data_source)
    
    # Generate target data (without email - will be dropped automatically)
    customers_data_target = generate_customers_data(batch_config, include_email=False)
    customers_df_target = spark.createDataFrame(customers_data_target)
    
    # Write to source and target
    customers_df_source.write.mode("append").saveAsTable(
        f"{TEST_CONFIG['test_catalog']}.{TEST_CONFIG['source_schema']}.customers"
    )
    customers_df_target.write.mode("append").saveAsTable(
        f"{TEST_CONFIG['test_catalog']}.{TEST_CONFIG['test_schema']}.customers"
    )
    
    print(f"  ✓ Loaded {len(customers_data_source)} records (source has email, target doesn't)")

print("\n✓ CUSTOMERS data loaded successfully (schema mismatch)")

# COMMAND ----------

# DBTITLE 1,Step 7: Generate and Load Test Data - PRODUCTS

# ============================================================================
# Generate PRODUCTS data (Data Mismatch)
# ============================================================================
print("\n--- Generating PRODUCTS data (Data Mismatch) ---")

def generate_products_data(batch_config, introduce_mismatch=False):
    """Generate products data for a batch"""
    batch_id = batch_config['batch_id']
    row_count = batch_config['row_count']
    
    seed_value = int(batch_id.split('_')[-1])
    random.seed(seed_value)
    
    products_data = []
    base_product_id = seed_value * 1000
    
    for i in range(row_count):
        product_id = base_product_id + i
        
        # Base product data
        product = {
            'product_id': product_id,
            'product_name': f'Product_{product_id}',
            'category': random.choice(['Electronics', 'Clothing', 'Home', 'Sports', 'Books']),
            'price': round(random.uniform(5.0, 500.0), 2),
            'stock_quantity': random.randint(0, 1000),
            'supplier_id': random.randint(1, 20),
            'is_active': random.choice([True, False]),
            'last_updated': datetime.now() - timedelta(hours=random.randint(0, 720)),
            '_aud_batch_load_id': batch_id
        }
        
        # Introduce mismatches in ~10% of records for target
        if introduce_mismatch and i % 10 == 0:
            product['price'] = round(product['price'] * 1.1, 2)  # 10% price difference
            product['stock_quantity'] = product['stock_quantity'] + 10  # Stock difference
        
        products_data.append(product)
    
    return products_data

# Process each batch
for batch_config in TEST_CONFIG['batches']:
    batch_id = batch_config['batch_id']
    print(f"\nProcessing batch: {batch_id}")
    
    # Generate source data (no mismatches)
    products_data_source = generate_products_data(batch_config, introduce_mismatch=False)
    products_df_source = spark.createDataFrame(products_data_source)
    
    # Generate target data (with mismatches)
    products_data_target = generate_products_data(batch_config, introduce_mismatch=True)
    products_df_target = spark.createDataFrame(products_data_target)
    
    # Write to source and target
    products_df_source.write.mode("append").saveAsTable(
        f"{TEST_CONFIG['test_catalog']}.{TEST_CONFIG['source_schema']}.products"
    )
    products_df_target.write.mode("append").saveAsTable(
        f"{TEST_CONFIG['test_catalog']}.{TEST_CONFIG['test_schema']}.products"
    )
    
    expected_mismatches = batch_config['row_count'] // 10
    print(f"  ✓ Loaded {len(products_data_source)} records (~{expected_mismatches} intentional mismatches)")

print("\n✓ PRODUCTS data loaded successfully (data mismatches)")

# COMMAND ----------

# DBTITLE 1,Step 8: Create ORC Files for Source Data

print("\n" + "="*80)
print("STEP 8: Creating ORC Files in DBFS")
print("="*80)

import os

# Create base directory
dbutils.fs.mkdirs(TEST_CONFIG['test_dbfs_path'])
print(f"✓ Created base directory: {TEST_CONFIG['test_dbfs_path']}")

# Track ORC file paths for metadata
orc_file_metadata = []

for table_info in TEST_CONFIG['tables']:
    table_name = table_info['name']
    full_source_table = f"{TEST_CONFIG['test_catalog']}.{TEST_CONFIG['source_schema']}.{table_name}"
    
    print(f"\n--- Creating ORC files for {table_name.upper()} ---")
    
    for batch_config in TEST_CONFIG['batches']:
        batch_id = batch_config['batch_id']
        
        # Create batch-specific directory
        batch_path = f"{TEST_CONFIG['test_dbfs_path']}/{table_name}/{batch_id}"
        
        # Read data for this batch from source table
        batch_df = spark.table(full_source_table).filter(f"_aud_batch_load_id = '{batch_id}'")
        row_count = batch_df.count()
        
        # Write as ORC
        batch_df.write.mode("overwrite").format("orc").save(batch_path)
        
        # List ORC files created
        orc_files = [f.path for f in dbutils.fs.ls(batch_path) if f.path.endswith('.orc')]
        
        print(f"  ✓ Batch {batch_id}: {len(orc_files)} ORC file(s), {row_count} records")
        print(f"    Path: {batch_path}")
        
        # Store metadata for later
        for orc_file in orc_files:
            orc_file_metadata.append({
                'table_name': f"{TEST_CONFIG['test_catalog']}.{TEST_CONFIG['test_schema']}.{table_name}",
                'batch_load_id': batch_id,
                'source_file_path': orc_file,
                'row_count': row_count
            })

print(f"\n✓ Created {len(orc_file_metadata)} ORC file(s) total")
print(f"✓ Base path: {TEST_CONFIG['test_dbfs_path']}")

# COMMAND ----------

# DBTITLE 1,Step 9: Populate Ingestion Config Table

print("\n" + "="*80)
print("STEP 9: Populating Ingestion Metadata Tables")
print("="*80)

print("\n--- Populating serving_ingestion_config ---")

# Prepare ingestion config data
config_data = []
for table_info in TEST_CONFIG['tables']:
    table_name = table_info['name']
    config_data.append({
        'config_id': f"TEST_{table_name.upper()}_{uuid.uuid4().hex[:8]}",
        'group_name': TEST_CONFIG['table_group'],
        'source_schema': TEST_CONFIG['source_schema'],
        'source_table': table_name,
        'source_file_path': f"{TEST_CONFIG['test_dbfs_path']}/{table_name}",
        'target_catalog': TEST_CONFIG['test_catalog'],
        'target_schema': TEST_CONFIG['test_schema'],
        'target_table': table_name,
        'source_file_format': 'orc',
        'source_file_options': None,
        'load_type': 'incremental',
        'write_mode': 'append',
        'primary_key': table_info['primary_key'],
        'partition_column': None,
        'partitioning_strategy': None,
        'frequency': 'daily',
        'table_size_gb': '0.001',
        'column_datatype_mapping': None,
        'delta_properties': None,
        'clean_column_names': 'N',
        'deduplicate': 'N',
        'is_active': 'Y',
        'insert_ts': datetime.now(),
        'last_update_ts': datetime.now(),
        'timestamp_column': None,
        'schedule': 'daily',
        'job_tags': 'test'
    })

# Create DataFrame and write
config_df = spark.createDataFrame(config_data)
config_df.write.mode("append").saveAsTable(constants.INGESTION_CONFIG_TABLE)

print(f"✓ Inserted {len(config_data)} configuration record(s)")

# COMMAND ----------

# DBTITLE 1,Step 10: Populate Ingestion Audit Table

print("\n--- Populating serving_ingestion_audit ---")

# Prepare ingestion audit data
audit_data = []
for table_info in TEST_CONFIG['tables']:
    table_name = table_info['name']
    full_table_name = f"{TEST_CONFIG['test_catalog']}.{TEST_CONFIG['test_schema']}.{table_name}"
    
    for batch_config in TEST_CONFIG['batches']:
        audit_data.append({
            'run_id': batch_config['run_id'],
            'config_id': f"TEST_{table_name.upper()}",
            'batch_load_id': batch_config['batch_id'],
            'group_name': TEST_CONFIG['table_group'],
            'target_table_name': full_table_name,
            'operation_type': 'LOAD',
            'load_type': 'BATCH',
            'status': 'COMPLETED',
            'start_ts': datetime.now() - timedelta(minutes=10),
            'end_ts': datetime.now() - timedelta(minutes=5),
            'log_message': f"Test data loaded for {table_name}",
            'microbatch_id': None,
            'row_count': batch_config['row_count']
        })

# Create DataFrame and write
audit_df = spark.createDataFrame(audit_data)
audit_df.write.mode("append").saveAsTable(constants.INGESTION_AUDIT_TABLE)

print(f"✓ Inserted {len(audit_data)} audit record(s)")

# COMMAND ----------

# DBTITLE 1,Step 11: Populate Ingestion Metadata Table

print("\n--- Populating serving_ingestion_metadata ---")

# Prepare ingestion metadata data
metadata_records = []
for orc_meta in orc_file_metadata:
    # Get file size (approximate)
    try:
        file_info = dbutils.fs.ls(orc_meta['source_file_path'])[0]
        file_size = file_info.size
    except:
        file_size = 1000  # Default if can't get size
    
    metadata_records.append({
        'table_name': orc_meta['table_name'],
        'batch_load_id': orc_meta['batch_load_id'],
        'source_file_path': orc_meta['source_file_path'],
        'file_modification_time': datetime.now() - timedelta(minutes=15),
        'file_size': file_size,
        'is_processed': 'Y',
        'insert_ts': datetime.now() - timedelta(minutes=10),
        'last_update_ts': datetime.now() - timedelta(minutes=10)
    })

# Create DataFrame and write
metadata_df = spark.createDataFrame(metadata_records)
metadata_df.write.mode("append").saveAsTable(constants.INGESTION_METADATA_TABLE)

print(f"✓ Inserted {len(metadata_records)} metadata record(s)")

# COMMAND ----------

# DBTITLE 1,Step 12: Verification and Summary

print("\n" + "="*80)
print("STEP 12: Verification and Summary")
print("="*80)

# Verify table counts
print("\n--- Table Row Counts ---")
for table_info in TEST_CONFIG['tables']:
    table_name = table_info['name']
    
    source_table = f"{TEST_CONFIG['test_catalog']}.{TEST_CONFIG['source_schema']}.{table_name}"
    target_table = f"{TEST_CONFIG['test_catalog']}.{TEST_CONFIG['test_schema']}.{table_name}"
    
    source_count = spark.table(source_table).count()
    target_count = spark.table(target_table).count()
    
    print(f"\n{table_name.upper()}:")
    print(f"  Source: {source_count} records")
    print(f"  Target: {target_count} records")
    print(f"  Scenario: {table_info['scenario']}")
    print(f"  Expected: {table_info['description']}")

# Verify metadata
print("\n--- Ingestion Metadata Verification ---")

config_count = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {constants.INGESTION_CONFIG_TABLE}
    WHERE group_name = '{TEST_CONFIG['table_group']}'
""").collect()[0].cnt
print(f"Config records: {config_count}")

audit_count = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {constants.INGESTION_AUDIT_TABLE}
    WHERE group_name = '{TEST_CONFIG['table_group']}'
""").collect()[0].cnt
print(f"Audit records: {audit_count}")

metadata_count = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {constants.INGESTION_METADATA_TABLE}
    WHERE table_name LIKE '{TEST_CONFIG['test_catalog']}.{TEST_CONFIG['test_schema']}.%'
""").collect()[0].cnt
print(f"Metadata records: {metadata_count}")

# Show batch details
print("\n--- Batch Details ---")
batch_df = spark.sql(f"""
    SELECT 
        target_table_name,
        batch_load_id,
        status,
        row_count
    FROM {constants.INGESTION_AUDIT_TABLE}
    WHERE group_name = '{TEST_CONFIG['table_group']}'
    ORDER BY target_table_name, batch_load_id
""")
display(batch_df)

# COMMAND ----------

# DBTITLE 1,Test Environment Ready - Next Steps

print("\n" + "="*80)
print("TEST ENVIRONMENT SETUP COMPLETE!")
print("="*80)

print("\n✅ What was created:")
print(f"  • 2 schemas: {TEST_CONFIG['test_schema']} (target), {TEST_CONFIG['source_schema']} (source)")
print(f"  • 6 tables: 3 source + 3 target tables")
print(f"  • {len(orc_file_metadata)} ORC files in {TEST_CONFIG['test_dbfs_path']}")
print(f"  • {config_count} ingestion config records")
print(f"  • {audit_count} ingestion audit records")
print(f"  • {metadata_count} ingestion metadata records")

print("\n📊 Test Scenarios Created:")
for table_info in TEST_CONFIG['tables']:
    print(f"  • {table_info['name'].upper()}: {table_info['scenario']} - {table_info['description']}")

print("\n🚀 Next Steps:")
print("  1. Run: notebooks/setup/02_setup_validation_mapping.py")
print("     - This will populate the validation_mapping table from ingestion_config")
print()
print("  2. Create validation jobs manually:")
print(f"     - Table Group: {TEST_CONFIG['table_group']}")
print(f"     - Use job_utils/validation_job_config.yml as reference")
print()
print("  3. Run validation job to test:")
print("     - Metrics collection")
print("     - All validators (row count, schema, PK, data reconciliation)")
print("     - Batch-level auditing")
print()
print("  4. Check validation results in:")
print(f"     - {constants.VALIDATION_LOG_TABLE}")
print(f"     - {constants.VALIDATION_SUMMARY_TABLE}")

print("\n" + "="*80)
print("TEST CONFIGURATION SUMMARY")
print("="*80)
print(f"Table Group: {TEST_CONFIG['table_group']}")
print(f"Target Schema: {TEST_CONFIG['test_catalog']}.{TEST_CONFIG['test_schema']}")
print(f"Source Schema: {TEST_CONFIG['test_catalog']}.{TEST_CONFIG['source_schema']}")
print(f"DBFS Location: {TEST_CONFIG['test_dbfs_path']}")
print(f"Number of Batches: {len(TEST_CONFIG['batches'])}")
print(f"Total Records: {sum([b['row_count'] for b in TEST_CONFIG['batches']]) * len(TEST_CONFIG['tables'])}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Quick Test Query - Validate Setup

print("\n--- Quick Validation Query ---")
print("Checking if framework can find unprocessed batches...")

test_query = f"""
SELECT 
    t1.table_name,
    t2.batch_load_id,
    COUNT(t1.source_file_path) as orc_file_count,
    t2.row_count as expected_rows
FROM {constants.INGESTION_METADATA_TABLE} t1
INNER JOIN {constants.INGESTION_AUDIT_TABLE} t2
    ON (t1.table_name = t2.target_table_name 
        AND t1.batch_load_id = t2.batch_load_id)
INNER JOIN {constants.INGESTION_CONFIG_TABLE} t4
    ON t1.table_name = concat_ws('.', t4.target_catalog, t4.target_schema, t4.target_table)
WHERE t2.status = 'COMPLETED'
    AND t4.group_name = '{TEST_CONFIG['table_group']}'
GROUP BY t1.table_name, t2.batch_load_id, t2.row_count
ORDER BY t1.table_name, t2.batch_load_id
"""

result_df = spark.sql(test_query)
print("\n✓ Batches ready for validation:")
display(result_df)

expected_batch_count = len(TEST_CONFIG['tables']) * len(TEST_CONFIG['batches'])
actual_batch_count = result_df.count()

if actual_batch_count == expected_batch_count:
    print(f"\n✅ SUCCESS: Found {actual_batch_count} batches ready for validation (expected: {expected_batch_count})")
else:
    print(f"\n⚠️ WARNING: Found {actual_batch_count} batches, expected {expected_batch_count}")

# COMMAND ----------

# DBTITLE 1,Sample Data Preview

print("\n--- Sample Data Preview ---")

for table_info in TEST_CONFIG['tables']:
    table_name = table_info['name']
    source_table = f"{TEST_CONFIG['test_catalog']}.{TEST_CONFIG['source_schema']}.{table_name}"
    
    print(f"\n{table_name.upper()} (first 5 records from source):")
    sample_df = spark.table(source_table).limit(5)
    display(sample_df)

print("\n✅ Test setup complete! Ready for validation testing.")

