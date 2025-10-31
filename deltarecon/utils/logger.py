"""
Structured logger for validation framework

Provides:
- Consistent log formatting
- Validation-specific logging methods
- Section headers for readability
- Table context support for parallel processing
"""

import logging
from typing import Any, Dict, Optional
from contextlib import contextmanager


class ValidationLogger:
    """Structured logger with validation-specific methods"""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        self._table_context: Optional[str] = None
        
        # Clear existing handlers to avoid duplicates
        self.logger.handlers = []
        
        # Format: timestamp [level] [component] - message
        formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d [%(levelname)s] [%(name)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def set_table_context(self, table_name: str):
        """Set table context for subsequent log messages"""
        self._table_context = table_name
    
    def clear_table_context(self):
        """Clear table context"""
        self._table_context = None
    
    @contextmanager
    def table_context(self, table_name: str):
        """Context manager for table-specific logging"""
        old_context = self._table_context
        self._table_context = table_name
        try:
            yield
        finally:
            self._table_context = old_context
    
    def _format_message(self, message: str) -> str:
        """Format message with table context if available"""
        if self._table_context:
            return f"[Table: {self._table_context}] {message}"
        return message
    
    def info(self, message: str):
        """Log info level message"""
        self.logger.info(self._format_message(message))
    
    def warning(self, message: str):
        """Log warning level message"""
        self.logger.warning(self._format_message(message))
    
    def error(self, message: str, exc_info: bool = False):
        """Log error level message"""
        self.logger.error(self._format_message(message), exc_info=exc_info)
    
    def debug(self, message: str):
        """Log debug level message"""
        self.logger.debug(self._format_message(message))
    
    def log_section(self, title: str, width: int = 80):
        """Log a major section header"""
        self.logger.info(f"[SECTION] {title}")
    
    def log_subsection(self, title: str, width: int = 80):
        """Log a subsection header"""
        self.logger.info(f"[SUBSECTION] {title}")
    
    def log_validation_start(self, table_name: str, batch_ids: list):
        """Log validation start for a table"""
        self.logger.info(
            f"Starting validation for table: {table_name}, "
            f"batches: {batch_ids}"
        )
    
    def log_validation_complete(self, table_name: str, status: str, duration: float):
        """Log validation completion for a table"""
        self.logger.info(
            f"Completed validation for table: {table_name}, "
            f"status: {status}, duration: {duration:.2f}s"
        )
    
    def log_metrics(self, metrics: Dict[str, Any]):
        """Log metrics in structured format"""
        self.logger.info(f"Metrics: {metrics}")
    
    def log_error_with_context(self, error: Exception, context: Dict[str, Any]):
        """Log error with contextual information for debugging"""
        self.logger.error(
            f"Error occurred: {str(error)}, Context: {context}",
            exc_info=True
        )


def get_logger(name: str) -> ValidationLogger:
    """
    Get logger instance
    
    Args:
        name: Logger name (typically __name__ from calling module)
    
    Returns:
        ValidationLogger instance
    """
    return ValidationLogger(name)

