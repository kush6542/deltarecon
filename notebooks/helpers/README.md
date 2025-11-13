# Helper Notebooks

This directory contains utility notebooks for debugging and manual validation.

## debug_schema_only_validation.py

**Purpose:** ⚡ **FAST** schema-only validation - reads only 10 rows from source and target!

**Key Features:**
- **Ultra-fast** - completes in seconds, not minutes
- Only reads 10 sample rows from source and target
- Schema validation only (column names and data types)
- Perfect for quick debugging and iterative development
- No expensive full table scans

**What this checks:**
- ✅ Column names match
- ✅ Data types match
- ✅ Configuration is valid

**What this DOES NOT check:**
- ❌ Row count (requires full scan)
- ❌ PK duplicates (requires full scan)  
- ❌ Data reconciliation (requires full scan)

**When to use:**
- You want to quickly verify schema compatibility
- You're debugging schema mismatches
- You're iterating on table structure/configuration
- You don't need full data validation yet
- You want fast feedback during development

**For full validation, use:** `debug_batch_validation.py` notebook

**Example:**
```
Ingestion Ops Schema: jio_home_prod.operations
Validation Schema: jio_home_prod.operations
Target Table: catalog.schema.table_name
Batch Load ID: 202511121234
```

Completes in ~5-10 seconds vs several minutes for full validation!

---

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

---

## manual_validation_override.py

**Purpose:** ⚠️ **OPERATIONAL TOOL** - Manually mark validation status as SUCCESS/SKIPPED to exclude batches from future validation runs.

**Key Features:**
- Manual override of validation status for specific batches
- Comprehensive safety checks and validations
- Full audit trail with user, timestamp, and reason
- Dry-run mode for preview before execution
- Verification of batch existence before override

**Use Cases:**
- ✅ Transient failures that are acceptable (approved by stakeholders)
- ✅ Known data quality issues with business approval
- ✅ Emergency scenarios requiring bypass of failed validations
- ✅ Batches that need to be excluded from validation
- ✅ Manual verification completed outside the framework

**⚠️ IMPORTANT WARNINGS:**
- **USE WITH CAUTION** - This bypasses automated validation checks
- **APPROVAL REQUIRED** - Ensure proper approval before marking as SUCCESS
- **MANUAL VERIFICATION** - Always verify data quality manually first
- **AUDIT TRAIL** - All overrides are logged with reason and user

**How to use:**

1. Open the notebook in Databricks
2. Fill in the required widgets:
   - **Target Table**: Full table name (catalog.schema.table)
   - **Batch Load ID**: Batch identifier to override
   - **Override Status**: SUCCESS or SKIPPED
   - **Reason**: Detailed reason for override (minimum 10 characters, include ticket numbers)
   - **Dry Run**: true (preview) or false (execute)
3. **Always start with Dry Run = true** to preview changes
4. Review all information displayed
5. If everything looks correct, set Dry Run = false and re-run
6. Verify the override was successful

**What it does:**

1. **Validates inputs** - Ensures all required fields are provided
2. **Verifies batch exists** - Checks ingestion audit table
3. **Shows current status** - Displays existing validation records
4. **Previews operation** - Shows exactly what will be inserted
5. **Executes override** (if dry_run=false) - Inserts SUCCESS/SKIPPED record into validation_log
6. **Verifies effect** - Confirms batch will be skipped in future runs

**How it works:**

The notebook inserts a record into the `validation_log` table with status = SUCCESS or SKIPPED. The validation framework checks this table and skips batches that have a latest validation status of SUCCESS or SKIPPED.

**Example Usage:**

**Example 1: Mark Failed Validation as Success**
```
Target Table: jio_home_prod.gold.customer_orders
Batch Load ID: 20241113_092145_abc123
Override Status: SUCCESS
Reason: Data reconciliation failed due to timezone mismatch. Manually verified data is correct. Approved by Data Quality team (Ticket: DQ-1234)
Dry Run: false
```

**Example 2: Skip Validation for Known Issue**
```
Target Table: jio_infra_prod.silver.network_events
Batch Load ID: 20241113_080000_def456
Override Status: SKIPPED
Reason: Source system outage caused incomplete data load. Business approved skipping this batch. Will be reprocessed in next cycle. (Ticket: INC-5678)
Dry Run: false
```

**Example 3: Preview Before Execution**
```
Target Table: jio_mobility_prod.bronze.location_data
Batch Load ID: 20241113_103000_ghi789
Override Status: SUCCESS
Reason: Validation timeout due to large batch size. Data quality verified through alternative checks. Approved by Platform team.
Dry Run: true  (Review, then set to false)
```

**Best Practices:**

1. **Always use Dry Run first** - Review the preview before executing
2. **Provide detailed reasons** - Include ticket numbers, approvals, and justification
3. **Verify manually first** - Ensure data quality before marking as SUCCESS
4. **Get proper approval** - Follow your organization's approval process
5. **Document externally** - Create tickets/documentation for all manual overrides
6. **Include ticket references** - Always reference incident/change tickets in reason

**Output:**

- Clear step-by-step progress with checkmarks (✅) and warnings (⚠️)
- Batch information from ingestion audit
- Current validation status (if any)
- Preview of operation to be performed
- Verification that override was successful
- Confirmation that batch will be skipped in future runs

**Safety Features:**

- Input validation (required fields, format checks)
- Batch existence verification
- Display of current validation status
- Dry-run mode for preview
- Post-execution verification
- Complete audit trail

**Dependencies:**

- Access to framework configuration (constants.py)
- Write access to validation_log table
- Read access to:
  - Ingestion audit table
  - Ingestion config table
  - Validation log table
  - Validation summary table

**Troubleshooting:**

**Batch Not Found:**
- Verify batch_load_id is correct (check ingestion audit table)
- Ensure target table uses full name (catalog.schema.table)
- Check that batch completed ingestion successfully

**Override Not Working:**
- Verify the record was inserted into validation_log
- Check that status is SUCCESS or SKIPPED
- Ensure timestamp is recent (latest record is used)
- Run verification query to check latest status

**When NOT to use:**

- ❌ For convenience without proper investigation
- ❌ Without manual verification of data quality
- ❌ Without proper approval and documentation
- ❌ For recurring issues (fix the root cause instead)

**Support:**

For issues or questions, contact:
- Data Platform Team
- Databricks Support
