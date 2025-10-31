# DeltaRecon

**A robust data validation framework for Databricks**

DeltaRecon validates the accuracy of data migrations by comparing source data (ORC files) against target data (Delta tables). It ensures data consistency, schema compliance, and primary key integrity across your data pipeline.

---

## Features

✓ **Automated Batch Processing** - Identifies and validates unprocessed batches automatically  
✓ **Parallel Execution** - Processes multiple tables concurrently for faster validation  
✓ **Double-Verification** - Critical metrics verified using multiple methods  
✓ **Complete Audit Trail** - All validation results stored in Delta tables  
✓ **Flexible Validators** - Row count, schema, and primary key validation  
✓ **Error Recovery** - Graceful error handling with detailed logging  
✓ **Production Ready** - Enterprise-grade code quality and standards  

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

Edit `config/constants.py` to set your environment:

```python
# Ingestion metadata tables (read from)
INGESTION_OPS_CATALOG = "your_catalog"
INGESTION_OPS_SCHEMA = "your_catalog.your_schema"

# Validation metadata tables (write to)
VALIDATION_OPS_CATALOG = "your_validation_catalog"
VALIDATION_SCHEMA = "your_validation_catalog.validation_schema"

# Framework settings
PARALLELISM = 5  # Number of tables to process in parallel
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

## Usage

### Basic Usage

```python
from deltarecon import ValidationRunner

# Create runner
runner = ValidationRunner(
    spark=spark,
    table_group="sales",
    iteration_suffix="daily"
)

# Run validation
result = runner.run()

# Check results
print(f"Success: {result['success']}, Failed: {result['failed']}")
```

### Running from Notebook

Run: `notebooks/main.py` with parameters:
- `table_group`: Name of table group to validate
- `iteration_suffix`: Identifier for this run (e.g., "daily", "hourly")

### Querying Results

```sql
-- Recent validation runs
SELECT 
  iteration_name,
  table_family,
  validation_run_status,
  validation_run_start_time,
  validation_run_end_time
FROM validation_log
ORDER BY validation_run_start_time DESC
LIMIT 10;

-- Validation summary with metrics
SELECT 
  table_family,
  overall_status,
  row_count_match_status,
  schema_match_status,
  primary_key_compliance_status,
  metrics.src_records,
  metrics.tgt_records
FROM validation_summary
WHERE iteration_name = 'daily_20250131_120000';
```

---

## Architecture

```
deltarecon/
├── config/                  # Configuration
│   ├── __init__.py
│   └── constants.py
├── deltarecon/             # Main package
│   ├── __init__.py
│   ├── runner.py           # Main orchestrator
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
│   │   ├── exceptions.py
│   │   └── logger.py
│   └── validators/         # Validators
│       ├── base_validator.py
│       ├── pk_validator.py
│       ├── row_count_validator.py
│       └── schema_validator.py
└── notebooks/              # Notebooks
    ├── main.py             # Entry point
    └── setup/              # Setup notebooks
```

---

## Validation Workflow

```
1. Job starts with table_group parameter (e.g., "sales")
2. Read configurations for all tables in group
3. For each table (in parallel):
   a. Identify unprocessed batches
   b. Write log entry (IN_PROGRESS)
   c. Load source ORC data
   d. Load target Delta data
   e. Run validators:
      - Row count (with double-verification)
      - Schema comparison  
      - Primary key uniqueness
   f. Write validation summary
   g. Update log entry (SUCCESS/FAILED)
4. Return summary with results
```

---

## Validators

### RowCountValidator
- Compares source and target row counts
- Double-verification using two independent methods
- Status: PASSED if counts match, FAILED otherwise

### SchemaValidator
- Compares column names and data types
- Identifies missing/extra columns
- Detects type mismatches
- Excludes audit columns (`_aud_*`)

### PKValidator
- Validates primary key uniqueness
- Detects duplicate rows
- Captures sample duplicates for investigation
- Status: SKIPPED if no PK defined

---

## Troubleshooting

### Import Errors

If you get `ModuleNotFoundError: No module named 'config'`:

**Using Repos:** Already handled automatically ✓

**Using Workspace Upload:** Add to top of notebook:
```python
import sys
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_root = "/Workspace" + "/".join(notebook_path.split("/")[:-2])
if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)
```

See `WORKSPACE_UPLOAD_GUIDE.md` for details.

### No Batches Found

Check:
1. Is `validation_mapping` populated?
2. Are there COMPLETED batches in `ingestion_audit`?
3. Have the batches already been validated successfully?

### Validation Failures

Check `validation_log` table for error details:
```sql
SELECT * FROM validation_log 
WHERE validation_run_status = 'FAILED'
ORDER BY validation_run_start_time DESC;
```

---

## Development

### Running Tests

```bash
# Install dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/
```

### Code Style

This project follows:
- PEP 8 style guide
- Type hints for function signatures
- Comprehensive docstrings

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## Version History

### v1.0.0 (Current)
- Initial production release
- Row count, schema, and PK validators
- Parallel table processing
- Complete audit trail

---

## License

[Add your license here]

---

## Support

For issues and questions:
- Open an issue on GitHub
- Contact: kushagra.parashar@databricks.com

---

## Acknowledgments

Built with ❤️ for robust data validation on Databricks

