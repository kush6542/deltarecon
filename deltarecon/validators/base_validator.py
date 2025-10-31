"""
Abstract base class for all validators

All validators must inherit from BaseValidator and implement:
- name property
- validate() method

Note: Loaded via %run - TableConfig, ValidatorResult, logger available in namespace
"""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

from deltarecon.models.table_config import TableConfig
from deltarecon.models.validation_result import ValidatorResult
from deltarecon.utils.logger import get_logger

logger = get_logger(__name__)

class BaseValidator(ABC):
    """
    Abstract base class for all validators
    
    Provides:
    - Common interface for all validators
    - Logging helpers
    - Standardized result format
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """
        Validator name
        
        Examples: 'row_count', 'schema', 'pk_validation'
        
        Returns:
            str: Unique validator name
        """
        pass
    
    @abstractmethod
    def validate(
        self, 
        source_df: DataFrame, 
        target_df: DataFrame,
        config: TableConfig
    ) -> ValidatorResult:
        """
        Run validation logic
        
        Args:
            source_df: Source DataFrame (from ORC files)
            target_df: Target DataFrame (from Delta table)
            config: Table configuration with validation settings
        
        Returns:
            ValidatorResult with status, metrics, and details
        
        Raises:
            ValidationFrameworkException: If validation fails critically
        """
        pass
    
    def log_start(self):
        """Log validator start"""
        logger.log_subsection(f"Running validator: {self.name}")
    
    def log_result(self, result: ValidatorResult):
        """
        Log validator result
        
        Args:
            result: Validator result to log
        """
        logger.info(f"Validator '{self.name}' completed")
        logger.info(f"  Status: {result.status}")
        logger.info(f"  Metrics: {result.metrics}")
        if result.message:
            logger.info(f"  Message: {result.message}")

