# Helper Notebooks

This directory contains utility notebooks for debugging and manual validation.

## debug_batch_validation.py

**Purpose:** Debug validation failures for a specific table and batch

**How to use:**

1. Open the notebook in Databricks
2. Fill in the widgets at the top:
   - **Target Table**: Full table name (e.g., `prd_connectivity.home_gold.home_btas_error_kpi_po`)
   - **Batch Load ID**: Batch identifier (e.g., `202510210435`)
   - **Table Group**: Table group name (e.g., `home_gold_10am_daily_job`)
   - **Run Data Reconciliation**: Select Y or N (reconciliation is slower for large datasets)
3. Run all cells
4. Review each validation check

**What it checks:**

1. Row Count - Compares source vs target row counts
2. Schema - Validates column names and data types
3. Primary Key Duplicates - Finds duplicate PKs in source and target
4. Data Reconciliation - Hash-based full row comparison (optional)

**Output:**

- Simple print statements for all checks
- DataFrame displays for sample data and mismatches
- Summary report at the end

**When to use:**

- Investigating validation failures from the main framework
- Manually validating a specific batch before running full validation
- Understanding why data doesn't match between source and target
- Debugging configuration issues

**Example:**

```
Target Table: prd_connectivity.home_gold.home_btas_error_kpi_po
Batch Load ID: 202510210435
Table Group: home_gold_10am_daily_job
Run Reconciliation: Y
```

This will load the batch and run all checks, showing you exactly where the issues are.

