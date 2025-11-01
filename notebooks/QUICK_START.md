# 🚀 Quick Start - Comprehensive Test

## 1️⃣ Run Test Data Creation

```python
# Open and run notebook
notebooks/comprehensive_validation_test.py

# Run up to "Validation Mapping Configuration" section
# This creates 18 test tables with various scenarios
```

**✅ Creates:**
- 18 source ORC files in `/FileStore/deltarecon_comprehensive_test/`
- 18 target Delta tables in `ts42_demo.validation_test`
- Metadata in `ingestion_config`, `ingestion_audit`, `ingestion_metadata`

---

## 2️⃣ Setup Validation Mapping ⚠️ REQUIRED

```python
# Option A: Run setup notebook
%run ./setup/02_setup_validation_mapping

# Configure parameters
table_group = "comprehensive_test_group"
```

**OR**

```sql
-- Option B: Manual SQL inserts
-- See notebook output for exact SQL statements needed
INSERT INTO validation_mapping (table_group, workflow_name, ...) VALUES (...)
```

---

## 3️⃣ Run Validations

```python
from deltarecon.runner import ValidationRunner

# Basic Mode (Row Count + Schema + PK)
runner = ValidationRunner(
    spark=spark,
    table_group="comprehensive_test_group",
    iteration_suffix="test_basic",
    is_full_validation=False
)
result = runner.run()

# Full Mode (+ Data Reconciliation)
runner_full = ValidationRunner(
    spark=spark,
    table_group="comprehensive_test_group",
    iteration_suffix="test_full",
    is_full_validation=True
)
result_full = runner_full.run()
```

---

## 4️⃣ View Results

```sql
-- Overall summary
SELECT 
    tgt_table,
    batch_load_id,
    overall_status,
    row_count_match_status,
    schema_match_status,
    primary_key_compliance_status
FROM cat_ril_nayeem_03.validation_v2.validation_summary
WHERE workflow_name = 'validation_comprehensive_test_group'
ORDER BY tgt_table

-- Detailed metrics
SELECT 
    tgt_table,
    metrics.src_records,
    metrics.tgt_records,
    metrics.src_extras,
    metrics.tgt_extras,
    metrics.matches
FROM cat_ril_nayeem_03.validation_v2.validation_summary
WHERE workflow_name = 'validation_comprehensive_test_group'
```

---

## 📊 What to Expect

| Test Scenario | Count | Expected Results |
|--------------|-------|------------------|
| Row Count Tests | 3 | 1 pass, 2 fail |
| Schema Tests | 3 | 1 pass, 2 fail |
| PK Tests | 5 | 2 pass, 3 fail |
| Data Recon Tests | 4 | 1 pass, 3 fail |
| Multi-Batch Tests | 2 | Multiple batch validations |
| All Pass | 1 | Everything passes ✅ |

**Total: 18 tables, ~25 batch validations (multi-batch creates multiple)**

---

## 🧹 Cleanup

```python
# Drop test data
spark.sql("DROP SCHEMA IF EXISTS ts42_demo.validation_test CASCADE")
dbutils.fs.rm("dbfs:/FileStore/deltarecon_comprehensive_test", recurse=True)

# Clean metadata
spark.sql("DELETE FROM ingestion_config WHERE group_name = 'comprehensive_test_group'")
spark.sql("DELETE FROM validation_mapping WHERE table_group = 'comprehensive_test_group'")
```

---

## ⚠️ Important

1. **Must setup validation_mapping** before running validations
2. Each batch validated **independently** (batch-level auditing)
3. Two modes: **Basic** (fast) vs **Full** (comprehensive with data recon)
4. Uses **new DBFS path** to avoid conflicts

---

## 🆘 Help

**Issue**: No batches found  
**Fix**: Check `ingestion_metadata.is_processed = 'N'`

**Issue**: Table not in validation_mapping  
**Fix**: Complete Step 2 (Setup Validation Mapping)

**Issue**: ORC file not found  
**Fix**: Re-run Step 1 (Test Data Creation)

---

**See full guide**: `notebooks/COMPREHENSIVE_TEST_GUIDE.md`

