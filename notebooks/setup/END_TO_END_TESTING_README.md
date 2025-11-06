# End-to-End Testing Guide for Delta Recon Framework

This guide walks you through complete end-to-end testing of the Delta Recon validation framework with all source file formats (ORC, TEXT, CSV).

## 📋 Overview

The end-to-end test creates:
- **6 test tables** across 3 different source formats
- **Complete metadata entries** in all ingestion tables
- **Sample data** covering various scenarios
- **All source file options** (headers, separators, schemas)

## 🎯 Test Coverage

### Source File Formats

1. **ORC Format (2 tables)**
   - `orders` - Partitioned table with partition_date
   - `inventory` - Non-partitioned table with timestamps

2. **TEXT Format (2 tables)**
   - `customers` - Tab-delimited (\t), no header
   - `transactions` - Pipe-delimited (|), no header

3. **CSV Format (2 tables)**
   - `employees` - Comma-separated, with header
   - `sales_metrics` - Comma-separated, with header

### Validation Features Tested

✅ Multi-format source reading (ORC, TEXT, CSV)  
✅ Schema validation across formats  
✅ Row count validation  
✅ Primary key compliance  
✅ Data reconciliation (full comparison)  
✅ Partitioned tables (partition_date in orders)  
✅ Non-partitioned tables (5 tables)  
✅ Different data types (INT, STRING, DOUBLE, DATE, TIMESTAMP)  
✅ Source file options parsing (JSON configuration)  
✅ Batch-level auditing  

## 🚀 Step-by-Step Execution

### Step 1: Create Test Environment

Run the test setup notebook:

```bash
# In Databricks
notebooks/setup/00_end_to_end_testing_setup.py
```

This notebook will:
- Create test schema: `ts42_demo.test_e2e_validation`
- Generate 6 source files in DBFS at `/FileStore/delta_recon_test/`
- Create 6 target Delta tables with sample data
- Insert metadata into:
  - `serving_ingestion_config` (6 entries)
  - `serving_ingestion_metadata` (6 entries)
  - `serving_ingestion_audit` (6 entries)

**Expected Output:**
```
✓ Created schema: ts42_demo.test_e2e_validation
✓ Created 6 ORC/TEXT/CSV files in DBFS
✓ Created 6 target Delta tables
✓ Inserted metadata entries
```

### Step 2: Setup Validation Mapping

Run the validation mapping setup:

```bash
# In Databricks
notebooks/setup/02_setup_validation_mapping.py
```

Configure it to sync the test group:
```python
TABLE_GROUPS_TO_SYNC = ['test_e2e_group']  # Add this to the sync list
```

This will create entries in `validation_mapping` table.

**Verify:**
```sql
SELECT * 
FROM cat_ril_nayeem_03.validation_v2.validation_mapping 
WHERE table_group = 'test_e2e_group'
```

Expected: 6 rows (one for each test table)

### Step 3: Run Validation Framework

Execute the validation runner:

```python
# Option 1: From command line
python deltarecon/runner.py --table-group test_e2e_group

# Option 2: From notebook
from deltarecon.runner import main
main(['--table-group', 'test_e2e_group'])
```

The framework will:
1. Read configuration from `validation_mapping` and `ingestion_config`
2. Parse source file options (separators, headers, schemas)
3. Load source data from DBFS using appropriate format
4. Compare source vs target tables
5. Write results to `validation_log` and `validation_summary`

**Expected Output:**
```
Starting validation framework v1.0.0
Table Group: test_e2e_group
Found 6 tables to validate
Processing batch: BATCH_20241106_XXXXXX
✓ Validated: orders (6 records)
✓ Validated: inventory (5 records)
✓ Validated: customers (5 records)
✓ Validated: transactions (5 records)
✓ Validated: employees (5 records)
✓ Validated: sales_metrics (5 records)
Validation completed successfully
```

### Step 4: Verify Results

Check validation results:

```sql
-- Validation Log (execution status)
SELECT 
    batch_load_id,
    tgt_table,
    validation_run_status,
    validation_run_start_time,
    validation_run_end_time,
    exception
FROM cat_ril_nayeem_03.validation_v2.validation_log
WHERE table_family LIKE '%test_e2e_validation%'
ORDER BY validation_run_start_time DESC;

-- Validation Summary (detailed results)
SELECT 
    batch_load_id,
    tgt_table,
    row_count_match_status,
    schema_match_status,
    primary_key_compliance_status,
    data_reconciliation_status,
    overall_status,
    metrics
FROM cat_ril_nayeem_03.validation_v2.validation_summary
WHERE table_family LIKE '%test_e2e_validation%'
ORDER BY tgt_table;
```

**Expected Results:**
- All tables: `validation_run_status = 'SUCCESS'`
- All validations: `PASSED` status
- Row counts match between source and target
- No exceptions

### Step 5: Verify Source File Reading

Manually test source file reading with different formats:

```python
# Test ORC reading
orc_df = spark.read.format("orc").load("/FileStore/delta_recon_test/orc/orders/BATCH_*/")
orc_df.show()

# Test TEXT reading (tab-delimited)
text_options = {
    "header": "false",
    "sep": "\t",
    "schema": "customer_id INT, customer_name STRING, email STRING, phone STRING, registration_date DATE, customer_tier STRING"
}
text_df = spark.read.format("csv").options(**text_options).load("/FileStore/delta_recon_test/text/customers/BATCH_*/")
text_df.show()

# Test CSV reading (with header)
csv_df = spark.read.format("csv").option("header", "true").load("/FileStore/delta_recon_test/csv/employees/BATCH_*/")
csv_df.show()
```

## 📊 Test Data Details

### Table 1: Orders (ORC, Partitioned)
```
Primary Key: order_id
Partition: partition_date
Rows: 6
Partitions: 20241101 (3 rows), 20241102 (3 rows)
```

### Table 2: Inventory (ORC)
```
Primary Key: product_id
Rows: 5
Special: Contains TIMESTAMP fields
```

### Table 3: Customers (TEXT, Tab-delimited)
```
Primary Key: customer_id
Delimiter: \t (tab)
Header: false
Rows: 5
```

### Table 4: Transactions (TEXT, Pipe-delimited)
```
Primary Key: transaction_id
Delimiter: | (pipe)
Header: false
Rows: 5
Special: Contains TIMESTAMP fields
```

### Table 5: Employees (CSV)
```
Primary Key: employee_id
Delimiter: , (comma)
Header: true
Rows: 5
```

### Table 6: Sales Metrics (CSV)
```
Primary Key: metric_id
Delimiter: , (comma)
Header: true
Rows: 5
```

## 🔍 Source File Options Reference

The framework reads `source_file_options` from `ingestion_config` as JSON strings:

### ORC Format
```json
{}
```
No options needed - native Spark format

### TEXT Format (Tab-delimited)
```json
{
  "header": "false",
  "sep": "\t",
  "schema": "customer_id INT, customer_name STRING, ..."
}
```

### TEXT Format (Pipe-delimited)
```json
{
  "header": "false",
  "sep": "|",
  "schema": "transaction_id STRING, order_id INT, ..."
}
```

### CSV Format
```json
{
  "header": "true",
  "inferSchema": "false",
  "sep": ",",
  "schema": "employee_id INT, first_name STRING, ..."
}
```

## 🧹 Cleanup

To clean up test data after testing:

```sql
-- Drop test tables
DROP SCHEMA IF EXISTS ts42_demo.test_e2e_validation CASCADE;

-- Remove ingestion config entries
DELETE FROM ts42_demo.migration_operations.serving_ingestion_config
WHERE group_name = 'test_e2e_group';

-- Remove ingestion metadata entries
DELETE FROM ts42_demo.migration_operations.serving_ingestion_metadata
WHERE table_name LIKE 'ts42_demo.test_e2e_validation.%';

-- Remove ingestion audit entries
DELETE FROM ts42_demo.migration_operations.serving_ingestion_audit
WHERE group_name = 'test_e2e_group';

-- Remove validation mapping entries
DELETE FROM cat_ril_nayeem_03.validation_v2.validation_mapping
WHERE table_group = 'test_e2e_group';

-- Remove validation results (optional)
DELETE FROM cat_ril_nayeem_03.validation_v2.validation_log
WHERE table_family LIKE '%test_e2e_validation%';

DELETE FROM cat_ril_nayeem_03.validation_v2.validation_summary
WHERE table_family LIKE '%test_e2e_validation%';
```

Remove DBFS files:
```python
dbutils.fs.rm("/FileStore/delta_recon_test", recurse=True)
```

## 🐛 Troubleshooting

### Issue: Source files not found
**Solution:** Check DBFS path exists
```python
dbutils.fs.ls("/FileStore/delta_recon_test/")
```

### Issue: Schema mismatch errors
**Solution:** Verify source_file_options in ingestion_config
```sql
SELECT config_id, target_table, source_file_options
FROM ts42_demo.migration_operations.serving_ingestion_config
WHERE group_name = 'test_e2e_group';
```

### Issue: No batches found for validation
**Solution:** Check ingestion_metadata has entries
```sql
SELECT * FROM ts42_demo.migration_operations.serving_ingestion_metadata
WHERE table_name LIKE 'ts42_demo.test_e2e_validation.%';
```

### Issue: TEXT file reading fails
**Solution:** Ensure schema is correctly specified in source_file_options
- Schema must be DDL-style: `col1 TYPE, col2 TYPE, ...`
- Separator must match file format
- Header option must match file structure

## ✅ Success Criteria

Your test is successful when:

1. ✅ All 6 tables created in DBFS and Delta
2. ✅ All metadata entries inserted (config, metadata, audit)
3. ✅ Validation mapping synced (6 entries)
4. ✅ Validation runs without errors
5. ✅ All validation statuses are `PASSED`
6. ✅ Row counts match between source and target
7. ✅ No exceptions in validation_log
8. ✅ All source formats read correctly (ORC, TEXT, CSV)

## 📚 Related Documentation

- [MULTI_FORMAT_SOURCE_SUPPORT.md](../../docs/MULTI_FORMAT_SOURCE_SUPPORT.md) - Source format details
- [INGESTION_SCHEMAS.md](../../docs/INGESTION_SCHEMAS.md) - Metadata table schemas
- [BATCH_TEST_GUIDE.md](../../docs/BATCH_TEST_GUIDE.md) - Batch validation testing

## 🎓 Learning Points

This test demonstrates:
- How to configure different source formats in `ingestion_config`
- How to use `source_file_options` JSON field
- How the framework parses and applies format-specific options
- End-to-end flow from ingestion to validation
- Batch-level auditing and validation
- Multi-format source support in a single framework

## 📝 Notes

- The test uses a unique `BATCH_LOAD_ID` with timestamp to avoid conflicts
- All tables use the same group name: `test_e2e_group`
- Source files are stored in format-specific subdirectories
- Target tables include the source data for validation comparison
- The test covers both partitioned and non-partitioned tables
- Primary keys are defined for all tables to test PK compliance

---

**Last Updated:** 2024-11-06  
**Framework Version:** v1.0.0  
**Test Group:** test_e2e_group

