"""
Lightweight Validation Framework

A robust, class-based validation framework for data migration validation on Databricks.

Usage:
    Basic (recommended):
        from deltarecon import ValidationRunner
        
        runner = ValidationRunner(
            spark=spark,
            table_group="sales",
            iteration_suffix="daily"
        )
        result = runner.run()
    
    Advanced:
        from deltarecon import (
            ValidationRunner,
            TableConfig,
            ValidationResult,
            ValidationFrameworkException
        )

Public API:
    - ValidationRunner: Main entry point for running validations
    - TableConfig: Configuration for a single table
    - ValidationResult: Result object from validation
    - All exception classes for error handling
"""

__version__ = "1.0.0"

# Main API - The star of the show!
from deltarecon.runner import ValidationRunner

# Models (for advanced usage)
from deltarecon.models.table_config import TableConfig
from deltarecon.models.validation_result import (
    ValidationResult,
    ValidatorResult,
    ValidationLogRecord,
    ValidationSummaryRecord,
)

# Exceptions (for error handling)
from deltarecon.utils.exceptions import (
    ValidationFrameworkException,
    ConfigurationError,
    DataConsistencyError,
    ValidationAccuracyError,
    BatchProcessingError,
    DataLoadError,
    MetadataWriteError,
)

# Configuration (for advanced usage)
from deltarecon.config import constants

# Define public API
__all__ = [
    # Main entry point - this is what most users will use
    "ValidationRunner",
    
    # Models
    "TableConfig",
    "ValidationResult",
    "ValidatorResult",
    "ValidationLogRecord",
    "ValidationSummaryRecord",
    
    # Exceptions
    "ValidationFrameworkException",
    "ConfigurationError",
    "DataConsistencyError",
    "ValidationAccuracyError",
    "BatchProcessingError",
    "DataLoadError",
    "MetadataWriteError",
    
    # Configuration
    "constants",
]
