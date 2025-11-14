# DeltaRecon

**A robust data validation framework for Databricks**

DeltaRecon validates the accuracy of data migrations by comparing source data (ORC, TEXT, CSV files) against target data (Delta tables). It ensures data consistency, schema compliance, and primary key integrity across your data pipeline with comprehensive batch-level auditing.

---

## Features

✓ **Multi-Format Source Support** - ORC, TEXT (delimited), and CSV files with custom options  
✓ **Batch-Level Auditing** - Each batch validated independently with complete audit trail  
✓ **Parallel Execution** - Processes multiple tables concurrently for faster validation  
✓ **Multiple Validation Modes** - Quick validation or full data reconciliation  
✓ **Complete Audit Trail** - All validation results stored in Delta tables per batch  
✓ **Flexible Validators** - Row count, schema, primary key, and data reconciliation  
✓ **Partial Success Model** - Graceful handling when some batches fail  
✓ **Write Mode Awareness** - Smart handling of append vs overwrite ingestion patterns  
✓ **End-to-End Testing** - Comprehensive test suite with sample data for all formats  
✓ **Production Ready** - Enterprise-grade code quality and automated job deployment  

---

## What Makes DeltaRecon Different?

### Batch-Level Auditing
Unlike traditional table-level validation, DeltaRecon validates each batch independently:
- **Granular tracking**: Separate audit record for each batch
- **Partial success**: Some batches can pass while others fail
- **Idempotency**: Re-run only failed batches, not entire tables
- **Better debugging**: Pinpoint exactly which batch has issues

### Intelligent Write Mode Handling
- **Append Mode**: Validates all unprocessed batches incrementally
- **Overwrite Mode**: Validates only the latest batch (historical validations become irrelevant)
- Automatically determined from ingestion metadata

### Flexible Validation Modes
- **Quick Mode** (default): Row count, schema, and PK validation (~2-5 min per table)
- **Full Mode** (`is_full_validation=True`): Adds hash-based data reconciliation (~5-15 min per table)

### Production-Ready Architecture
- Thread-safe parallel processing
- Comprehensive error handling
- Complete audit trail for compliance
- Automated job deployment utilities

---

## Quick Start

### Prerequisites

- Databricks workspace
- Python 3.8+
- Access to ingestion metadata tables
- Databricks CLI (for deployment)

### Installation

**Option 1: Databricks Asset Bundles (Recommended)**

```bash
# Install Databricks CLI
pip install databricks-cli

# Clone repository
git clone https://github.com/YOUR_USERNAME/deltarecon.git
cd deltarecon

# Deploy to Databricks
databricks bundle deploy -t prod
```

**Option 2: Databricks Repos**

1. In Databricks workspace: **Repos** → **Add Repo**
2. Connect to: `https://github.com/YOUR_USERNAME/deltarecon.git`
3. Repos handles Python paths automatically

---

## Configuration

Edit `deltarecon/config/constants.py` to set your environment:

```python
# Ingestion metadata tables (read from)
INGESTION_OPS_CATALOG = "your_catalog"
INGESTION_OPS_SCHEMA = "your_catalog.your_schema"

# Validation metadata tables (write to)
VALIDATION_OPS_CATALOG = "your_validation_catalog"
VALIDATION_SCHEMA = "your_validation_catalog.validation_schema"

# Framework settings
PARALLELISM = 5  # Number of tables to process in parallel
FRAMEWORK_VERSION = "v1.0.0"
BATCH_LEVEL_AUDITING = True  # Always enabled for granular tracking
```

---

## Setup (First Time)

### Step 1: Create Metadata Tables

Run: `notebooks/setup/01_create_tables.py`

Creates:
- `validation_mapping` - Configuration table
- `validation_log` - Run tracking table
- `validation_summary` - Metrics table

### Step 2: Sync Configuration

Run: `notebooks/setup/02_setup_validation_mapping.py`

Syncs validation configuration from your ingestion metadata.

### Step 3: Verify Setup

Check that tables were created and populated:

```sql
SELECT * FROM your_catalog.your_schema.validation_mapping LIMIT 10;
```

---

## Testing the Framework

DeltaRecon includes a comprehensive end-to-end test suite that creates sample data for all supported formats.

### Quick Test (10 minutes)

```bash
# Step 1: Run test setup (creates 6 test tables)
Run: notebooks/setup/00_end_to_end_testing_setup.py

# Step 2: Sync validation mapping
Run: notebooks/setup/02_setup_validation_mapping.py
     (Add 'test_e2e_group' to TABLE_GROUPS_TO_SYNC)

# Step 3: Run validation
python deltarecon/runner.py --table-group test_e2e_group

# Step 4: Verify results
Run: notebooks/setup/00_verify_validation_results.py
```

### What Gets Tested

The test creates 6 tables covering all features:

| Format | Tables | Features Tested |
|--------|--------|-----------------|
| **ORC** | 2 | Native format, partitioned & non-partitioned |
| **TEXT** | 2 | Tab-delimited, pipe-delimited, custom schemas |
| **CSV** | 2 | Headers, custom separators, schema enforcement |

**Validation Coverage:**
- ✅ Multi-format source reading (ORC, TEXT, CSV)
- ✅ Schema validation across formats
- ✅ Row count matching
- ✅ Primary key compliance
- ✅ Data reconciliation
- ✅ Partitioned tables
- ✅ Source file options parsing (JSON config)

### Expected Results

All validations should show:
- `validation_run_status` = **SUCCESS**
- `overall_status` = **SUCCESS**
- All check statuses = **PASSED**
- No exceptions

### Quick Reference

See [E2E_TEST_QUICK_REFERENCE.md](docs/E2E_TEST_QUICK_REFERENCE.md) for commands and troubleshooting.

Full guide: [END_TO_END_TESTING_README.md](notebooks/setup/END_TO_END_TESTING_README.md)

---

## Usage

### Basic Usage

```python
from deltarecon import ValidationRunner

# Quick validation (row count, schema, PK only)
runner = ValidationRunner(
    spark=spark,
    table_group="sales",
    iteration_suffix="daily",
    is_full_validation=False  # Default: faster
)

# Full validation (includes hash-based data reconciliation)
runner = ValidationRunner(
    spark=spark,
    table_group="sales",
    iteration_suffix="daily",
    is_full_validation=True  # Slower but comprehensive
)

# Run validation
result = runner.run()

# Check results
print(f"Success: {result['success']}, Failed: {result['failed']}")
print(f"Partial Success: {result.get('partial_success', 0)}")
```

### Running from Notebook

Run: `notebooks/main.py` with parameters:
- `table_group`: Name of table group to validate (required)
- `iteration_suffix`: Identifier for this run (default: "default")
- `isFullValidation`: Enable full data reconciliation (default: "false")

### Querying Results

```sql
-- Recent validation runs (batch-level)
SELECT 
  iteration_name,
  table_family,
  batch_load_id,
  validation_run_status,
  validation_run_start_time,
  validation_run_end_time
FROM validation_log
ORDER BY validation_run_start_time DESC
LIMIT 10;

-- Validation summary with metrics (per batch)
SELECT 
  batch_load_id,
  table_family,
  overall_status,
  row_count_match_status,
  schema_match_status,
  primary_key_compliance_status,
  data_reconciliation_status,
  metrics.src_records,
  metrics.tgt_records
FROM validation_summary
WHERE iteration_name = 'daily_20241103_120000'
ORDER BY table_family, batch_load_id;

-- Table-level summary (aggregate batches)
SELECT 
  table_family,
  COUNT(*) as total_batches,
  SUM(CASE WHEN overall_status = 'SUCCESS' THEN 1 ELSE 0 END) as success_batches,
  SUM(CASE WHEN overall_status = 'FAILED' THEN 1 ELSE 0 END) as failed_batches
FROM validation_summary
WHERE iteration_name = 'daily_20241103_120000'
GROUP BY table_family;
```

### Monitoring and Alerts

```sql
-- Tables with partial success (some batches failed)
SELECT 
  vl.table_family,
  COUNT(DISTINCT vl.batch_load_id) as total_batches,
  SUM(CASE WHEN vl.validation_run_status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
  SUM(CASE WHEN vl.validation_run_status = 'FAILED' THEN 1 ELSE 0 END) as failed_count
FROM validation_log vl
WHERE vl.iteration_name = 'daily_20241103_120000'
GROUP BY vl.table_family
HAVING failed_count > 0 AND success_count > 0;

-- Success rate by table over last 30 days
SELECT 
  table_family,
  COUNT(*) as total_validations,
  SUM(CASE WHEN validation_run_status = 'SUCCESS' THEN 1 ELSE 0 END) as successes,
  ROUND(100.0 * SUM(CASE WHEN validation_run_status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
FROM validation_log
WHERE validation_run_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY table_family
ORDER BY success_rate_pct ASC;

-- Unvalidated batches (candidates for next run)
SELECT 
  ia.batch_load_id,
  ia.target_table_name,
  ia.start_time as ingestion_time,
  ia.status as ingestion_status,
  vl.validation_run_status
FROM serving_ingestion_audit ia
LEFT JOIN validation_log vl 
  ON ia.batch_load_id = vl.batch_load_id
WHERE ia.status = 'COMPLETED'
  AND (vl.validation_run_status IS NULL OR vl.validation_run_status = 'FAILED')
ORDER BY ia.start_time DESC;
```

---

## Architecture

```
deltarecon/
├── deltarecon/             # Main package
│   ├── __init__.py
│   ├── runner.py           # Main orchestrator
│   ├── config/             # Configuration
│   │   ├── __init__.py
│   │   ├── constants.py
│   │   └── constants_kushagra.py (example)
│   ├── core/               # Core processing
│   │   ├── batch_processor.py
│   │   ├── ingestion_reader.py
│   │   ├── metadata_writer.py
│   │   ├── source_target_loader.py
│   │   └── validation_engine.py
│   ├── models/             # Data models
│   │   ├── table_config.py
│   │   └── validation_result.py
│   ├── utils/              # Utilities
│   │   ├── banner.py
│   │   ├── exceptions.py
│   │   └── logger.py
│   └── validators/         # Validators
│       ├── base_validator.py
│       ├── data_reconciliation_validator.py
│       ├── pk_validator.py
│       ├── row_count_validator.py
│       └── schema_validator.py
├── notebooks/              # Notebooks
│   ├── main.py             # Entry point
│   └── setup/              # Setup notebooks
│       ├── 01_create_tables.py
│       └── 02_setup_validation_mapping.py
├── job_utils/              # Job deployment utilities
│   ├── job_deployment.py
│   ├── pause_unpause_jobs.py
│   └── validation_job_config.yml
└── docs/                   # Documentation
```

---

## Validation Workflow

```
1. Job starts with table_group parameter (e.g., "sales")
2. Read configurations for all tables in group
3. For each table (in parallel):
   a. Identify unprocessed batches (considering write mode)
   b. For each batch (batch-level auditing):
      i.   Write log entry (IN_PROGRESS)
      ii.  Load source ORC data for this batch
      iii. Load target Delta data for this batch
      iv.  Cache DataFrames for consistent snapshot
      v.   Run validators:
           - Row count comparison
           - Schema compatibility
           - Primary key uniqueness
           - Data reconciliation (if is_full_validation=True)
      vi.  Unpersist DataFrames
      vii. Write validation summary
      viii. Update log entry (SUCCESS/FAILED)
   c. Determine table status:
      - SUCCESS: All batches passed
      - FAILED: All batches failed
      - PARTIAL_SUCCESS: Some batches passed, some failed
4. Return summary with results
```

### Write Mode Handling

**Append Mode:**
- Validates ALL unprocessed COMPLETED batches
- Each batch validated independently
- Historical batches remain validated

**Overwrite Mode:**
- Validates ONLY the LATEST COMPLETED batch
- Previous batch validations become irrelevant
- Only latest data state matters

---

## Validators

### RowCountValidator
- Compares source and target row counts
- Fast and lightweight validation
- Status: PASSED if counts match, FAILED otherwise
- Always runs

### SchemaValidator
- Compares column names and data types
- Identifies missing/extra columns
- Detects type mismatches
- Excludes audit columns (`_aud_*`) and partition columns
- Always runs

### PKValidator
- Validates primary key uniqueness in target
- Detects duplicate rows
- Captures sample duplicates for investigation
- Status: SKIPPED if no PK defined
- Always runs

### DataReconciliationValidator (Optional)
- Full row-level hash-based data comparison
- Compares all data columns (excludes partitions and audit columns)
- Reports:
  - `src_extras`: Records in source but not in target
  - `tgt_extras`: Records in target but not in source
  - `matches`: Identical records in both
- Only runs when `is_full_validation=True`
- Slower but provides comprehensive validation
- Uses MD5 hash of concatenated column values

---

## Troubleshooting

### Import Errors

If you get `ModuleNotFoundError: No module named 'deltarecon'`:

**Using Repos:** Already handled automatically ✓

**Using Workspace Upload:** Add to top of notebook:
```python
import sys
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_root = "/Workspace" + "/".join(notebook_path.split("/")[:-2])
if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)
```

See `docs/WORKSPACE_UPLOAD_GUIDE.md` for details.

### No Batches Found

Check:
1. Is `validation_mapping` populated? 
   ```sql
   SELECT * FROM validation_mapping WHERE table_group = 'your_group';
   ```
2. Are there COMPLETED batches in `ingestion_audit`?
   ```sql
   SELECT * FROM serving_ingestion_audit WHERE status = 'COMPLETED';
   ```
3. Have the batches already been validated successfully?
   ```sql
   SELECT * FROM validation_log WHERE validation_run_status = 'SUCCESS';
   ```
4. For overwrite mode: Only the latest batch will be validated

### Validation Failures

Check `validation_log` table for error details:
```sql
SELECT 
  batch_load_id,
  table_family,
  validation_run_status,
  exception,
  validation_run_start_time
FROM validation_log 
WHERE validation_run_status = 'FAILED'
ORDER BY validation_run_start_time DESC;
```

### Partial Success

When some batches pass and some fail:
```sql
-- See which batches failed for a table
SELECT 
  batch_load_id,
  overall_status,
  row_count_match_status,
  schema_match_status,
  primary_key_compliance_status
FROM validation_summary
WHERE table_family = 'your_table'
  AND overall_status = 'FAILED';
```

### Performance Issues

If validation is slow:
1. Set `is_full_validation=False` for faster validation (50-80% faster)
2. Increase cluster size (more workers)
3. Increase `PARALLELISM` in `constants.py`
4. Partition target tables by `_aud_batch_load_id` for better data skipping

---

## Job Deployment

### Automated Deployment

DeltaRecon includes utilities for automated job deployment:

**1. Configure Jobs**

Edit `job_utils/validation_job_config.yml`:

```yaml
jobs:
  - job_name: "validation_job_sales"
    table_group: "sales"
    schedule: "0 0 6 * * ?"  # Daily at 6 AM
    cluster_config:
      node_type: "i3.xlarge"
      num_workers: 2
    parameters:
      isFullValidation: "false"
```

**2. Deploy Jobs**

```bash
# Deploy all jobs defined in config
python job_utils/job_deployment.py --config job_utils/validation_job_config.yml
```

This will:
- Create/update Databricks jobs via REST API
- Configure clusters and schedules
- Set up email notifications (if specified)
- Link to `notebooks/main.py` as the entry point

**3. Manage Jobs**

```bash
# Pause all validation jobs
python job_utils/pause_unpause_jobs.py --action pause

# Resume all validation jobs
python job_utils/pause_unpause_jobs.py --action unpause
```

See `docs/DEPLOYMENT_GUIDE.txt` for detailed deployment instructions.

---

## Development

### Project Structure

- **deltarecon/**: Main framework package
- **notebooks/**: Entry points and setup scripts
- **job_utils/**: Job deployment and management utilities
- **docs/**: Comprehensive documentation

### Key Design Principles

1. **Batch-Level Auditing**: Each batch validated independently
2. **Thread Safety**: Fresh component instances per thread
3. **Idempotency**: Safe to re-run validations
4. **Separation of Concerns**: Clear module boundaries
5. **Graceful Degradation**: Partial success model

### Code Style

This project follows:
- PEP 8 style guide
- Type hints for function signatures
- Comprehensive docstrings
- Structured logging with context

---

## Documentation

Comprehensive documentation is available in the `docs/` directory:

### Core Documentation
- **DEPLOYMENT_GUIDE.txt**: Complete deployment and operational guide
- **DESIGN_DOCUMENTATION.txt**: Detailed architecture and design decisions
- **DEVELOPMENT_PLAN.md**: Development roadmap and planning
- **FLOW_DIAGRAMS.md**: Visual workflow diagrams

### Testing & Validation
- **E2E_TEST_QUICK_REFERENCE.md**: Quick reference for end-to-end testing (⭐ Start here!)
- **END_TO_END_TESTING_README.md**: Comprehensive testing guide
- **BATCH_TEST_GUIDE.md**: Guide for batch-level testing
- **TEST_FRAMEWORK_SUMMARY.md**: Testing framework overview
- **MULTI_FORMAT_SOURCE_SUPPORT.md**: Multi-format source details

### Implementation Details
- **DATA_RECONCILIATION_IMPLEMENTATION.md**: Data reconciliation details
- **BATCH_LEVEL_AUDITING_CHANGES.md**: Batch-level auditing implementation
- **INGESTION_SCHEMAS.md**: Metadata table schemas reference

### Quick Reference

Key concepts:
- **Batch**: A logical grouping of data files loaded together
- **Table Group**: Collection of related tables validated together
- **Table Family**: Individual table within a group
- **Iteration**: Single execution of validation workflow
- **Write Mode**: Strategy for loading data (append or overwrite)

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## Version History

### v1.0.0 (Current - November 2025)
- **Batch-level auditing**: Each batch validated independently with dedicated audit records
- **Multiple validation modes**: Quick validation (default) and full data reconciliation
- **Four validators**: Row count, schema, primary key, and data reconciliation
- **Write mode awareness**: Smart handling of append vs overwrite ingestion patterns
- **Partial success model**: Graceful handling when some batches fail
- **Parallel processing**: Table-level parallelism with configurable workers
- **Automated job deployment**: Utilities for deploying and managing Databricks jobs
- **Thread safety**: Fresh component instances per thread for safe parallel execution
- **Complete documentation**: Comprehensive guides and design documentation
- **Production ready**: Enterprise-grade logging, error handling, and audit trail

---

## License

[Add your license here]

---

## Support

For issues and questions:
- Open an issue on GitHub
- Contact: [Your contact information]


