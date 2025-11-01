# Comprehensive Validation Framework Test Guide

**Notebook**: `notebooks/comprehensive_validation_test.py`  
**Purpose**: End-to-end testing of DeltaRecon validation framework with comprehensive test scenarios

---

## 📋 Overview

This test notebook creates **18 different test scenarios** covering all validator types with both PASS and FAIL cases. It uses a **new DBFS path** to avoid conflicts with existing data.

### Test Coverage

| Validator Type | Scenarios | Description |
|----------------|-----------|-------------|
| **Row Count** | 3 | Perfect match, source has more, target has more |
| **Schema** | 3 | Perfect match, missing column, type mismatch |
| **Primary Key** | 5 | Single PK pass/fail, composite PK pass/fail, no PK |
| **Data Reconciliation** | 4 | Perfect match, source extras, target extras, data mismatch |
| **Multi-Batch** | 2 | Append mode (3 batches), overwrite mode (3 batches) |
| **All Pass** | 1 | Perfect scenario where all validations pass |
| **TOTAL** | **18** | Comprehensive test coverage |

---

## 🚀 How to Use

### Step 1: Create Test Data

Run the notebook up to the **"Validation Mapping Configuration"** section. This will:

1. ✅ Clean up any existing test data
2. ✅ Create fresh target schema: `ts42_demo.validation_test`
3. ✅ Create 18 source ORC files in: `/FileStore/deltarecon_comprehensive_test/`
4. ✅ Create 18 target Delta tables
5. ✅ Populate `ingestion_config` table
6. ✅ Populate `ingestion_audit` table
7. ✅ Populate `ingestion_metadata` table

**Stop here!** ⚠️

---

### Step 2: Setup Validation Mapping (REQUIRED)

Before running validations, you **must** set up the validation mapping table.

**Option A: Use existing setup notebook**

```python
# Run the validation mapping setup notebook
%run ./setup/02_setup_validation_mapping

# Configure for this test group
table_group = "comprehensive_test_group"
```

**Option B: Manual SQL insert**

The test notebook displays sample validation_mapping records you need. Example:

```sql
INSERT INTO cat_ril_nayeem_03.validation_v2.validation_mapping
(table_group, workflow_name, table_family, src_table, tgt_table, 
 tgt_primary_keys, mismatch_exclude_fields, validation_is_active)
VALUES (
    'comprehensive_test_group',
    'validation_comprehensive_test_group',
    'ts42_demo_validation_test_row_count_pass_perfect_match',
    'source_system.row_count_pass_perfect_match',
    'ts42_demo.validation_test.row_count_pass_perfect_match',
    'order_id',
    '_aud_batch_load_id',
    true
);
-- ... repeat for all 18 tables
```

---

### Step 3: Run Validations (Optional)

The notebook provides commented-out code to run validations. You can:

#### Option A: Run in the notebook

Uncomment the validation sections:
- **Basic Mode**: Row Count + Schema + PK validators
- **Full Mode**: All validators + Data Reconciliation

#### Option B: Run separately using ValidationRunner

```python
from deltarecon.runner import ValidationRunner

# Basic Mode
runner = ValidationRunner(
    spark=spark,
    table_group="comprehensive_test_group",
    iteration_suffix="test_basic",
    is_full_validation=False
)
result = runner.run()

# Full Mode (with Data Reconciliation)
runner_full = ValidationRunner(
    spark=spark,
    table_group="comprehensive_test_group",
    iteration_suffix="test_full",
    is_full_validation=True
)
result_full = runner_full.run()
```

---

### Step 4: View Results

The notebook includes several analysis sections:

1. **Validation Summary**: Overall results for all tables
2. **Row Count Validator Results**: Source vs target row counts
3. **Schema Validator Results**: Column and type comparison
4. **PK Validator Results**: Primary key compliance
5. **Multi-Batch Analysis**: Batch-level auditing results
6. **Data Reconciliation Results**: src_extras, tgt_extras, matches
7. **Basic vs Full Comparison**: Mode comparison

---

## 🧪 Test Scenarios Explained

### Row Count Validator

| Scenario | Source Rows | Target Rows | Expected Result |
|----------|-------------|-------------|-----------------|
| `row_count_pass_perfect_match` | 5 | 5 | ✅ PASS |
| `row_count_fail_source_more` | 8 | 5 | ❌ FAIL (source has 3 extra) |
| `row_count_fail_target_more` | 4 | 6 | ❌ FAIL (target has 2 extra) |

### Schema Validator

| Scenario | Issue | Expected Result |
|----------|-------|-----------------|
| `schema_pass_perfect_match` | None | ✅ PASS |
| `schema_fail_missing_column` | Target missing `last_updated` | ❌ FAIL |
| `schema_fail_type_mismatch` | `stock_quantity` is INT in source, STRING in target | ❌ FAIL |

### Primary Key Validator

| Scenario | PK Type | Issue | Expected Result |
|----------|---------|-------|-----------------|
| `pk_pass_single_key` | Single (`order_id`) | None | ✅ PASS |
| `pk_fail_source_duplicates` | Single | Source has duplicate `order_id=9001` | ❌ FAIL |
| `pk_fail_target_duplicates` | Single | Target has duplicate `order_id=9501` | ❌ FAIL |
| `pk_pass_composite_key` | Composite (`store_id\|product_id`) | None | ✅ PASS |
| `pk_fail_composite_key` | Composite | Source has duplicate `(1, 101)` | ❌ FAIL |

### Data Reconciliation Validator

| Scenario | Metrics | Expected Result |
|----------|---------|-----------------|
| `data_recon_pass_perfect_match` | src_extras=0, tgt_extras=0 | ✅ PASS |
| `data_recon_fail_source_extras` | src_extras=2, tgt_extras=0 | ❌ FAIL |
| `data_recon_fail_target_extras` | src_extras=0, tgt_extras=2 | ❌ FAIL |
| `data_recon_fail_mismatch` | Same IDs, different amounts | ❌ FAIL |

### Multi-Batch Processing

| Scenario | Mode | Batches | Expected Behavior |
|----------|------|---------|-------------------|
| `multi_batch_append_mode` | Append | 3 | Each batch validated independently, all data retained |
| `multi_batch_overwrite_mode` | Overwrite | 3 | Each batch validated independently, only latest data in target |

---

## 📊 Expected Metrics

After running validations, you should see:

### validation_summary table columns:
- `batch_load_id`: Unique batch identifier
- `src_table`: Source table name
- `tgt_table`: Target table name
- `row_count_match_status`: PASSED/FAILED/SKIPPED
- `schema_match_status`: PASSED/FAILED/SKIPPED
- `primary_key_compliance_status`: PASSED/FAILED/SKIPPED
- `col_name_compare_status`: PASSED/FAILED/SKIPPED
- `data_type_compare_status`: PASSED/FAILED/SKIPPED
- `overall_status`: SUCCESS/FAILED
- `metrics`: Struct with detailed metrics

### metrics struct fields:
- `src_records`: Total source records
- `tgt_records`: Total target records
- `src_extras`: Records only in source
- `tgt_extras`: Records only in target
- `matches`: Matching records
- `mismatches`: Records with differences

---

## 🗂️ File Locations

### ORC Files
```
dbfs:/FileStore/deltarecon_comprehensive_test/orc_files/
├── row_count_pass_perfect_match/
│   └── BATCH_20250101_001/data.orc
├── row_count_fail_source_more/
│   └── BATCH_20250101_002/data.orc
├── ... (16 more tables)
```

### Target Tables
```
ts42_demo.validation_test.row_count_pass_perfect_match
ts42_demo.validation_test.row_count_fail_source_more
... (16 more tables)
```

### Metadata Tables
```
ts42_demo.migration_operations.serving_ingestion_config
ts42_demo.migration_operations.serving_ingestion_audit
ts42_demo.migration_operations.serving_ingestion_metadata
cat_ril_nayeem_03.validation_v2.validation_mapping
cat_ril_nayeem_03.validation_v2.validation_log
cat_ril_nayeem_03.validation_v2.validation_summary
```

---

## 🧹 Cleanup

To clean up test data:

```python
# Drop target schema
spark.sql("DROP SCHEMA IF EXISTS ts42_demo.validation_test CASCADE")

# Clean DBFS
dbutils.fs.rm("dbfs:/FileStore/deltarecon_comprehensive_test", recurse=True)

# Clean metadata (optional)
spark.sql("""
    DELETE FROM ts42_demo.migration_operations.serving_ingestion_config
    WHERE group_name = 'comprehensive_test_group'
""")

spark.sql("""
    DELETE FROM cat_ril_nayeem_03.validation_v2.validation_mapping
    WHERE table_group = 'comprehensive_test_group'
""")
```

---

## ⚠️ Important Notes

1. **Validation Mapping Required**: You MUST set up validation_mapping before running validations
2. **Batch-Level Auditing**: Each batch is validated independently, creating separate log and summary records
3. **Two Validation Modes**: 
   - Basic (faster): Row Count + Schema + PK
   - Full (comprehensive): All validators + Data Reconciliation
4. **Fresh DBFS Path**: Uses `/FileStore/deltarecon_comprehensive_test/` to avoid conflicts
5. **Accurate Schemas**: All table schemas match the INGESTION_SCHEMAS.md document

---

## 📈 Success Criteria

After running validations, verify:

- [x] All 18 tables processed
- [x] PASS scenarios show `overall_status = 'SUCCESS'`
- [x] FAIL scenarios show `overall_status = 'FAILED'`
- [x] Row count metrics match expectations
- [x] Schema issues detected correctly
- [x] PK duplicates identified
- [x] Data reconciliation metrics accurate
- [x] Multi-batch scenarios validated independently
- [x] validation_log has entries for each batch
- [x] validation_summary has metrics for each batch

---

## 🆘 Troubleshooting

### Issue: "No new batches to validate"
**Solution**: Check that `ingestion_metadata.is_processed = 'N'` for your test batches

### Issue: "Table not found in validation_mapping"
**Solution**: Ensure Step 2 (Setup Validation Mapping) was completed

### Issue: "ORC file not found"
**Solution**: Re-run the test data creation section

### Issue: Validation results don't match expectations
**Solution**: Check the detailed metrics in `validation_summary` table for specific failures

---

## 📝 Additional Resources

- **Framework Documentation**: See `docs/` folder
- **Batch-Level Auditing**: See `docs/BATCH_LEVEL_AUDITING_CHANGES.md`
- **Schema Reference**: See `docs/INGESTION_SCHEMAS.md`
- **Setup Notebooks**: See `notebooks/setup/` folder

---

**Last Updated**: 2025-11-01  
**Framework Version**: v1.1.0 (Batch-Level Auditing)

