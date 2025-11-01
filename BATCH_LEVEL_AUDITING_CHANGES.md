# Batch-Level Auditing Implementation Summary

## Overview
This document summarizes all changes made to enable **batch-level auditing** in the DeltaRecon validation framework.

**Date**: 2025-11-01  
**Version**: v1.1.0  
**Breaking Changes**: Schema changes required (validation_log and validation_summary tables must be recreated)

---

## Key Changes

### 1. **Feature Flag Added**
- **File**: `deltarecon/config/constants.py`
- **Change**: Added `BATCH_LEVEL_AUDITING = True` flag
- **Purpose**: Controls whether batches are validated individually (True) or together (False)

### 2. **Database Schema Changes** ⚠️ BREAKING
- **File**: `notebooks/setup/01_create_tables.py`
- **Changes**:
  - `validation_log.batch_load_id`: `ARRAY<STRING>` → `STRING`
  - `validation_summary.batch_load_id`: `ARRAY<STRING>` → `STRING`
- **Action Required**: Drop and recreate tables using updated DDL

### 3. **Data Models Updated**
- **File**: `deltarecon/models/validation_result.py`
- **Changes**:
  - `ValidationResult.batch_load_ids` → `batch_load_id` (List[str] → str)
  - `ValidationLogRecord.batch_load_ids` → `batch_load_id` (List[str] → str)
  - `ValidationSummaryRecord.batch_load_ids` → `batch_load_id` (List[str] → str)

### 4. **Metadata Writer Simplified**
- **File**: `deltarecon/core/metadata_writer.py`
- **Changes**:
  - Removed array construction logic (`array('batch1', 'batch2')` → `'batch1'`)
  - Updated idempotency check to include `batch_load_id` in WHERE clause
  - Simplified INSERT queries (no more array syntax)

### 5. **Batch Processor Enhanced**
- **File**: `deltarecon/core/batch_processor.py`
- **Changes**:
  - Added new method: `get_unprocessed_batches_with_mapping()` 
    - Returns `Dict[str, List[str]]` mapping batch_id to ORC paths
  - Removed `explode(batch_load_id)` from validation_log query
  - Batch-to-path relationship now preserved for individual processing

### 6. **Validation Engine Updated**
- **File**: `deltarecon/core/validation_engine.py`
- **Changes**:
  - Parameter `batch_ids: list` → `batch_id: str`
  - Logs now show single batch ID instead of list
  - ValidationResult created with single batch_id

### 7. **Source Target Loader Updated**
- **File**: `deltarecon/core/source_target_loader.py`
- **Changes**:
  - Parameter `batch_ids: List[str]` → `batch_id: str`
  - Target filter simplified: `WHERE _aud_batch_load_id = 'batch1'` (no more OR chains)
  - Batch verification now checks for single batch

### 8. **Runner - Batch-Level Loop Implemented** 🎯
- **File**: `deltarecon/runner.py`
- **Major Changes**:
  - `_process_table()` now loops through batches individually
  - Each batch gets its own:
    - Log entry (IN_PROGRESS → SUCCESS/FAILED)
    - Data loading (source + target for that batch only)
    - Validation run
    - Summary record
    - Completion status
  - Error handling: Failed batch doesn't stop other batches
  - New status: `PARTIAL_SUCCESS` (some batches passed, some failed)
  - Updated return structure with batch-level metrics

---

## Architecture Changes

### Before (Bulk Processing)
```
Get batches → ['batch1', 'batch2', 'batch3']
                       ↓
            Load ALL batches together
                       ↓
            Validate ONCE (combined)
                       ↓
         Write 1 log + 1 summary (array of batches)
```

### After (Batch-Level Auditing)
```
Get batches → {'batch1': [paths], 'batch2': [paths], 'batch3': [paths]}
                       ↓
           Loop: For each batch_id, paths
                       ↓
    ┌─────────────────┴─────────────────┐
    ↓                                   ↓
Load batch1 only                   Load batch2 only
Validate batch1                    Validate batch2
Write log + summary (batch1)       Write log + summary (batch2)
```

---

## Benefits

### ✅ Granular Tracking
- Each batch has its own log entry
- Easy to identify which specific batch failed
- Clear audit trail at batch level

### ✅ Better Error Handling
- Failed batch doesn't stop other batches
- Can retry individual failed batches
- Partial success scenarios handled gracefully

### ✅ Simpler Queries
- No more `explode(batch_load_id)` needed
- Direct equality checks: `batch_load_id = 'batch1'`
- Better query performance (no array operations)

### ✅ Better Indexing
- Can index on `batch_load_id` column
- Faster lookups and filtering
- More efficient partition pruning

---

## Migration Steps

### 1. Drop Existing Tables
```sql
DROP TABLE IF EXISTS validation_log;
DROP TABLE IF EXISTS validation_summary;
```

### 2. Run Setup Script
```bash
# In Databricks notebook
%run notebooks/setup/01_create_tables.py
```

### 3. Deploy Updated Code
- Deploy all updated Python files to Databricks workspace
- Restart any running jobs

### 4. Test
```python
from deltarecon import ValidationRunner

runner = ValidationRunner(
    spark=spark,
    table_group="your_test_group",
    iteration_suffix="test"
)

result = runner.run()
print(result)
```

---

## Backward Compatibility

⚠️ **NOT BACKWARD COMPATIBLE**

This is a **breaking change** due to schema modifications. You cannot:
- Use old validation_log/summary tables with new code
- Use new tables with old code
- Mix old and new data

**Action**: Must drop and recreate tables.

---

## Query Examples

### Check Batch Status
```sql
-- Simple query with new schema
SELECT 
    batch_load_id,
    tgt_table,
    validation_run_status,
    validation_run_start_time
FROM validation_log
WHERE tgt_table = 'your_table'
ORDER BY validation_run_start_time DESC;
```

### Find Failed Batches
```sql
SELECT 
    batch_load_id,
    tgt_table,
    exception
FROM validation_log
WHERE validation_run_status = 'FAILED'
ORDER BY validation_run_start_time DESC;
```

### Batch-Level Metrics
```sql
SELECT 
    batch_load_id,
    tgt_table,
    metrics.src_records,
    metrics.tgt_records,
    overall_status
FROM validation_summary
WHERE tgt_table = 'your_table';
```

---

## Files Modified

1. `deltarecon/config/constants.py` - Added feature flag
2. `notebooks/setup/01_create_tables.py` - Updated DDLs
3. `deltarecon/models/validation_result.py` - Changed array to string
4. `deltarecon/core/metadata_writer.py` - Simplified SQL
5. `deltarecon/core/batch_processor.py` - Added batch mapping method
6. `deltarecon/core/validation_engine.py` - Updated signatures
7. `deltarecon/core/source_target_loader.py` - Single batch loading
8. `deltarecon/runner.py` - Batch-level loop implementation

---

## Testing Checklist

- [ ] Drop and recreate validation_log table
- [ ] Drop and recreate validation_summary table
- [ ] Deploy updated code to Databricks
- [ ] Run validation on test table group
- [ ] Verify batch-level log entries created
- [ ] Verify batch-level summary entries created
- [ ] Test partial failure scenario (some batches fail)
- [ ] Test full success scenario (all batches pass)
- [ ] Test full failure scenario (all batches fail)
- [ ] Verify error handling for individual batch failures
- [ ] Check query performance

---

## Support

For issues or questions:
1. Check validation_log table for error details
2. Review Databricks job logs
3. Verify tables were recreated with new schema
4. Ensure BATCH_LEVEL_AUDITING flag is set correctly

---

## Version History

- **v1.0.0**: Original bulk processing (array-based)
- **v1.1.0**: Batch-level auditing (string-based) ← Current version

