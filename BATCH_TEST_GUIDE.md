# Batch-Level Auditing Test Setup Guide

## Overview
This test setup creates a simple, controlled environment to verify batch-level auditing functionality.

**Test File**: `notebooks/setup/00_batch_level_test_setup.py`

---

## Test Configuration

### 📊 **Test Data Structure**

```
2 Groups × 2 Tables × 5 Batches = 20 Total Batches
Each batch: 100 rows
Total test records: 2,000 rows
```

### **Group 1: test_group1**
1. **orders** (ts42_demo.batch_test.orders)
   - Columns: order_id (PK), customer_id, amount, status
   - 5 batches × 100 rows = 500 rows total

2. **customers** (ts42_demo.batch_test.customers)
   - Columns: customer_id (PK), name, email, region
   - 5 batches × 100 rows = 500 rows total

### **Group 2: test_group2**
1. **products** (ts42_demo.batch_test.products)
   - Columns: product_id (PK), product_name, price, category
   - 5 batches × 100 rows = 500 rows total

2. **inventory** (ts42_demo.batch_test.inventory)
   - Columns: inventory_id (PK), product_id, quantity, warehouse
   - 5 batches × 100 rows = 500 rows total

---

## Batch Naming Convention

Batches are named with timestamps for easy identification:
```
BATCH_20250101_000000  (Batch 1)
BATCH_20250101_010000  (Batch 2)
BATCH_20250101_020000  (Batch 3)
BATCH_20250101_030000  (Batch 4)
BATCH_20250101_040000  (Batch 5)
```

---

## What the Test Setup Does

### 1. **Creates Test Infrastructure**
- ✅ Creates target catalog: `ts42_demo.batch_test`
- ✅ Creates 4 Delta tables (2 per group)
- ✅ Populates each table with 5 batches (500 rows each)

### 2. **Creates Source ORC Files**
- ✅ Generates ORC files in: `/FileStore/deltarecon_batch_test/orc_files/`
- ✅ Organized by: `{group}/{table}/{batch_id}/data.orc`
- ✅ Total: 20 ORC files (one per batch)

### 3. **Populates Ingestion Metadata**
- ✅ `serving_ingestion_config`: 4 table configurations
- ✅ `serving_ingestion_audit`: 20 batch audit records (all COMPLETED)
- ✅ `serving_ingestion_metadata`: 20 ORC file metadata records

### 4. **Runs Initial Validation Test**
- ✅ Automatically runs validation on `test_group1`
- ✅ Verifies batch-level processing
- ✅ Shows results summary

---

## How to Run

### **Step 1: Drop Old Validation Tables**
```sql
DROP TABLE IF EXISTS cat_ril_nayeem_03.validation_v2.validation_log;
DROP TABLE IF EXISTS cat_ril_nayeem_03.validation_v2.validation_summary;
```

### **Step 2: Create New Validation Tables**
```python
%run /Workspace/path/to/notebooks/setup/01_create_tables.py
```

### **Step 3: Run Batch-Level Test Setup**
```python
%run /Workspace/path/to/notebooks/setup/00_batch_level_test_setup.py
```

This will:
1. Create all test data
2. Populate ingestion metadata
3. Run validation on `test_group1` (2 tables × 5 batches = 10 validations)
4. Show results

---

## Expected Results

### **Validation Log Entries**
You should see **10 log entries** (5 batches × 2 tables):

```sql
SELECT 
    tgt_table,
    batch_load_id,
    validation_run_status,
    validation_run_start_time
FROM cat_ril_nayeem_03.validation_v2.validation_log
WHERE iteration_name LIKE 'batch_test%'
ORDER BY tgt_table, batch_load_id;
```

Expected output:
```
ts42_demo.batch_test.customers | BATCH_20250101_000000 | SUCCESS | ...
ts42_demo.batch_test.customers | BATCH_20250101_010000 | SUCCESS | ...
ts42_demo.batch_test.customers | BATCH_20250101_020000 | SUCCESS | ...
ts42_demo.batch_test.customers | BATCH_20250101_030000 | SUCCESS | ...
ts42_demo.batch_test.customers | BATCH_20250101_040000 | SUCCESS | ...
ts42_demo.batch_test.orders    | BATCH_20250101_000000 | SUCCESS | ...
ts42_demo.batch_test.orders    | BATCH_20250101_010000 | SUCCESS | ...
ts42_demo.batch_test.orders    | BATCH_20250101_020000 | SUCCESS | ...
ts42_demo.batch_test.orders    | BATCH_20250101_030000 | SUCCESS | ...
ts42_demo.batch_test.orders    | BATCH_20250101_040000 | SUCCESS | ...
```

### **Validation Summary Entries**
You should see **10 summary entries** (one per batch):

```sql
SELECT 
    tgt_table,
    batch_load_id,
    overall_status,
    metrics.src_records,
    metrics.tgt_records
FROM cat_ril_nayeem_03.validation_v2.validation_summary
WHERE iteration_name LIKE 'batch_test%'
ORDER BY tgt_table, batch_load_id;
```

Expected output:
```
ts42_demo.batch_test.customers | BATCH_20250101_000000 | SUCCESS | 100 | 100
ts42_demo.batch_test.customers | BATCH_20250101_010000 | SUCCESS | 100 | 100
...
ts42_demo.batch_test.orders    | BATCH_20250101_000000 | SUCCESS | 100 | 100
ts42_demo.batch_test.orders    | BATCH_20250101_010000 | SUCCESS | 100 | 100
...
```

---

## Manual Testing

### **Test Group 1**
```python
from deltarecon import ValidationRunner

runner = ValidationRunner(
    spark=spark,
    table_group="test_group1",
    iteration_suffix="manual_test"
)

result = runner.run()
print(f"Total: {result['total']}, Success: {result['success']}, Failed: {result['failed']}")
```

### **Test Group 2**
```python
runner = ValidationRunner(
    spark=spark,
    table_group="test_group2",
    iteration_suffix="manual_test"
)

result = runner.run()
```

---

## Verification Queries

### **1. Check Batch-Level Granularity**
```sql
-- Each table should have 5 separate validation entries
SELECT 
    tgt_table,
    COUNT(DISTINCT batch_load_id) as batches_validated,
    COUNT(*) as total_validations
FROM cat_ril_nayeem_03.validation_v2.validation_log
WHERE iteration_name LIKE 'batch_test%'
GROUP BY tgt_table;
```

Expected: Each table shows 5 batches

### **2. Check Row Count Accuracy**
```sql
-- Each batch should validate 100 rows
SELECT 
    tgt_table,
    batch_load_id,
    metrics.src_records,
    metrics.tgt_records,
    CASE 
        WHEN metrics.src_records = metrics.tgt_records THEN '✓ Match'
        ELSE '✗ Mismatch'
    END as status
FROM cat_ril_nayeem_03.validation_v2.validation_summary
WHERE iteration_name LIKE 'batch_test%'
ORDER BY tgt_table, batch_load_id;
```

Expected: All should show 100 = 100 (Match)

### **3. Check for Duplicates (Should Be None)**
```sql
-- No duplicate validation entries per batch
SELECT 
    tgt_table,
    batch_load_id,
    COUNT(*) as validation_count
FROM cat_ril_nayeem_03.validation_v2.validation_log
WHERE iteration_name LIKE 'batch_test%'
GROUP BY tgt_table, batch_load_id
HAVING COUNT(*) > 1;
```

Expected: Empty result (no duplicates)

### **4. Check Validation Timestamps**
```sql
-- Batches should be processed in sequence
SELECT 
    tgt_table,
    batch_load_id,
    validation_run_start_time,
    validation_run_end_time,
    TIMESTAMPDIFF(SECOND, validation_run_start_time, validation_run_end_time) as duration_seconds
FROM cat_ril_nayeem_03.validation_v2.validation_log
WHERE iteration_name LIKE 'batch_test%'
ORDER BY tgt_table, validation_run_start_time;
```

---

## Test Scenarios

### **Scenario 1: All Batches Pass**
✅ Current setup - all data matches perfectly

### **Scenario 2: Simulate Batch Failure**
To test failure handling:

```python
# Manually corrupt one batch in target table
spark.sql("""
    UPDATE ts42_demo.batch_test.orders
    SET amount = amount + 100
    WHERE _aud_batch_load_id = 'BATCH_20250101_020000'
    LIMIT 10
""")

# Run validation again
runner = ValidationRunner(
    spark=spark,
    table_group="test_group1",
    iteration_suffix="failure_test"
)
result = runner.run()
```

Expected:
- 9 batches: SUCCESS
- 1 batch: FAILED (the corrupted one)
- Table status: PARTIAL_SUCCESS

### **Scenario 3: Rerun Validation (Idempotency)**
```python
# Run validation twice with same iteration_suffix
runner1 = ValidationRunner(spark=spark, table_group="test_group1", iteration_suffix="rerun_test")
result1 = runner1.run()

runner2 = ValidationRunner(spark=spark, table_group="test_group1", iteration_suffix="rerun_test")
result2 = runner2.run()
```

Expected: Should handle gracefully (update existing records)

---

## Cleanup

To remove test data:

```python
# Drop test tables
spark.sql("DROP SCHEMA IF EXISTS ts42_demo.batch_test CASCADE")

# Clean up ORC files
dbutils.fs.rm("dbfs:/FileStore/deltarecon_batch_test", True)

# Clean up ingestion metadata
spark.sql(f"DELETE FROM {constants.INGESTION_CONFIG_TABLE} WHERE group_name LIKE 'test_group%'")
spark.sql(f"DELETE FROM {constants.INGESTION_AUDIT_TABLE} WHERE group_name LIKE 'test_group%'")
spark.sql(f"DELETE FROM {constants.INGESTION_METADATA_TABLE} WHERE table_name LIKE 'ts42_demo.batch_test.%'")

# Clean up validation metadata
spark.sql(f"DELETE FROM {constants.VALIDATION_LOG_TABLE} WHERE iteration_name LIKE 'batch_test%'")
spark.sql(f"DELETE FROM {constants.VALIDATION_SUMMARY_TABLE} WHERE iteration_name LIKE 'batch_test%'")
```

---

## Success Criteria

✅ **Batch-level granularity**: Each batch validated separately  
✅ **10 log entries**: 5 batches × 2 tables  
✅ **10 summary entries**: One per batch  
✅ **Correct row counts**: 100 rows per batch  
✅ **No duplicates**: Each batch validated once  
✅ **Error isolation**: Failed batch doesn't stop others  
✅ **Idempotency**: Rerun handles gracefully  

---

## Troubleshooting

### Problem: No batches found
**Check**: `serving_ingestion_audit.status = 'COMPLETED'`

### Problem: Validation fails
**Check**: 
1. Tables exist in target catalog
2. ORC files exist and are readable
3. `_aud_batch_load_id` column exists in target tables

### Problem: Duplicate validations
**Check**: Idempotency logic in `metadata_writer.py`

---

## Next Steps After Testing

1. ✅ Verify all 10 validations complete successfully
2. ✅ Test failure scenario (corrupt one batch)
3. ✅ Test rerun (idempotency)
4. ✅ Verify query performance on validation tables
5. ✅ Test with test_group2
6. 🚀 Deploy to production with real data!

---

**Happy Testing!** 🎉

