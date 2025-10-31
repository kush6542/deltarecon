"""
Custom exceptions for validation framework

Exception hierarchy:
    ValidationFrameworkException (base)
    ├── ConfigurationError
    ├── DataConsistencyError
    ├── ValidationAccuracyError
    ├── BatchProcessingError
    ├── DataLoadError
    └── MetadataWriteError
"""


class ValidationFrameworkException(Exception):
    """Base exception for validation framework"""
    pass


class ConfigurationError(ValidationFrameworkException):
    """
    Raised when configuration is invalid or missing
    
    Examples:
    - Missing required fields in config
    - Invalid write_mode value
    - Primary key columns don't exist
    """
    pass


class DataConsistencyError(ValidationFrameworkException):
    """
    Raised when data consistency checks fail
    
    Examples:
    - Batch IDs don't match between source and target
    - Partition extraction changed row counts
    - Source file paths don't map to expected batches
    """
    pass


class ValidationAccuracyError(ValidationFrameworkException):
    """
    Raised when validation accuracy checks fail
    
    Examples:
    - Double-count verification mismatch
    - Cross-verification between methods shows different results
    """
    pass


class BatchProcessingError(ValidationFrameworkException):
    """
    Raised when batch processing fails
    
    Examples:
    - Cannot identify unprocessed batches
    - Batch query returns unexpected results
    """
    pass


class DataLoadError(ValidationFrameworkException):
    """
    Raised when data loading fails
    
    Examples:
    - Cannot read ORC files
    - Cannot read Delta table
    - File paths don't exist
    """
    pass


class MetadataWriteError(ValidationFrameworkException):
    """
    Raised when metadata write operations fail
    
    Examples:
    - Cannot write to validation_log
    - Cannot write to validation_summary
    - UPDATE statement fails
    """
    pass

