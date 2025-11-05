# Helper Notebooks

This directory contains utility notebooks for debugging and manual validation.

## debug_batch_validation.py

**Purpose:** Debug validation failures for a specific table and batch using simple, transparent code.

**Key Features:**
- No framework imports required
- Simple spark.read.orc() and spark.read.table() for data loading
- All validation logic inline and easy to understand
- Reads configuration directly from metadata tables

**How to use:**

1. Open the notebook in Databricks
2. Fill in the widgets at the top:
   - **Ingestion Ops Schema**: Schema containing ingestion metadata (e.g., `ts42_demo.migration_operations`)
   - **Validation Schema**: Schema containing validation metadata (e.g., `cat_ril_nayeem_03.validation_v2`)
   - **Target Table**: Full table name (e.g., `prd_connectivity.home_gold.home_btas_error_kpi_po`)
   - **Batch Load ID**: Batch identifier (e.g., `202510210435`)
   - **Run Data Reconciliation**: Select Y or N (reconciliation is slower for large datasets)
3. Run all cells
4. Review each validation check

**What it checks:**

1. **Row Count** - Compares source vs target row counts
2. **Schema Validation** - Checks column names and data types
3. **Primary Key Duplicates** - Finds duplicate PKs in source and target, shows top offenders
4. **Data Reconciliation** - Hash-based full row comparison (optional, can be slow)

**How it works:**

1. Queries `{ingestion_ops_schema}.serving_ingestion_config` table for source path and write mode
2. Queries `{validation_schema}.validation_mapping` table for primary keys
3. Uses `dbutils.fs.ls()` to find ORC files matching the batch
4. Loads source with `spark.read.format("orc").load(path)`
5. Extracts partition columns from file paths using regex
6. Loads target with `spark.read.format("delta").table()` and filters by `_aud_batch_load_id`
7. Runs each validation check with simple Spark DataFrame operations
8. Shows sample mismatches for troubleshooting

**Output:**

- Simple print statements with clear formatting
- DataFrame displays for sample data and mismatches
- Summary report at the end

**When to use:**

- Investigating validation failures from the main framework
- Manually validating a specific batch before running full validation
- Understanding why data does not match between source and target
- Debugging configuration issues (wrong PKs, missing columns, etc.)
- Learning how validation works without framework abstractions

**Example:**

```
Ingestion Ops Schema: ts42_demo.migration_operations
Validation Schema: cat_ril_nayeem_03.validation_v2
Target Table: prd_connectivity.home_gold.home_btas_error_kpi_po
Batch Load ID: 202510210435
Run Reconciliation: N
```

This will load the batch and run row count, schema, and PK duplicate checks.

**Dependencies:**

- Access to ingestion metadata tables:
  - `{ingestion_ops_schema}.serving_ingestion_config`
  - `{ingestion_ops_schema}.source_table_partition_mapping`
- Access to validation metadata tables:
  - `{validation_schema}.validation_mapping`
- Read access to source ORC files in ADLS
- Read access to target Delta tables

**Portability:**

The notebook is fully portable across environments. Simply change the schema widgets to point to your environment:

**Dev:**
```
Ingestion Ops Schema: dev_catalog.migration_operations
Validation Schema: dev_catalog.validation
```

**UAT:**
```
Ingestion Ops Schema: uat_catalog.migration_operations
Validation Schema: uat_catalog.validation
```

**Prod:**
```
Ingestion Ops Schema: ts42_demo.migration_operations
Validation Schema: cat_ril_nayeem_03.validation_v2
```
