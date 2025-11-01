"""
Validation engine - Orchestrates all validators

Runs all validators and aggregates results.

Note: Loaded via %run - TableConfig, ValidationResult, validators, logger available in namespace
"""

from typing import Dict
from pyspark.sql import DataFrame
from datetime import datetime
import time

from deltarecon.models.table_config import TableConfig
from deltarecon.models.validation_result import ValidationResult, ValidatorResult
from deltarecon.validators.row_count_validator import RowCountValidator
from deltarecon.validators.schema_validator import SchemaValidator
from deltarecon.validators.pk_validator import PKValidator
from deltarecon.utils.logger import get_logger

logger = get_logger(__name__)

class ValidationEngine:
    """
    Orchestrates all validators
    
    Runs validators in sequence and aggregates results.
    """
    
    def __init__(self):
        """
        Initialize validation engine with all validators
        """
        self.validators = [
            RowCountValidator(),
            SchemaValidator(),
            PKValidator()
        ]
        self.logger = logger
    
    def run_validations(
        self,
        source_df: DataFrame,
        target_df: DataFrame,
        config: TableConfig,
        batch_id: str,
        iteration_name: str
    ) -> ValidationResult:
        """
        Run all validators and aggregate results
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            config: Table configuration
            batch_id: Batch ID being validated (single batch)
            iteration_name: Current iteration name
        
        Returns:
            ValidationResult with aggregated results
        """
        start_time = datetime.now()
        
        self.logger.set_table_context(config.table_name)
        self.logger.log_section(f"VALIDATION RUN: {config.table_name}")
        self.logger.info(f"Batch: {batch_id}")
        self.logger.info(f"Iteration: {iteration_name}")
        
        # Log DataFrame details
        self.logger.info(f"Source DataFrame:")
        self.logger.info(f"  Rows: {source_df.count():,}")
        self.logger.info(f"  Columns: {len(source_df.columns)}")
        
        self.logger.info(f"Target DataFrame:")
        self.logger.info(f"  Rows: {target_df.count():,}")
        self.logger.info(f"  Columns: {len(target_df.columns)}")
        
        # Cache DataFrames for consistent reads across validators
        self.logger.info("Caching DataFrames for consistency...")
        source_df.cache()
        target_df.cache()
        
        # Run all validators
        validator_results = {}
        
        # Import validator logger to set table context
        from deltarecon.validators.base_validator import logger as validator_logger
        
        for validator in self.validators:
            try:
                # Set table context for validator logger
                validator_logger.set_table_context(config.table_name)
                
                validator_start = time.time()
                result = validator.validate(source_df, target_df, config)
                validator_duration = time.time() - validator_start
                
                validator_results[validator.name] = result
                
                self.logger.info(f"Validator '{validator.name}' completed in {validator_duration:.2f}s")
                
            except Exception as e:
                self.logger.error(f"Validator '{validator.name}' FAILED: {str(e)}", exc_info=True)
                
                # Store error result
                validator_results[validator.name] = ValidatorResult(
                    status="ERROR",
                    metrics={},
                    message=str(e)
                )
            finally:
                # Clear table context after each validator
                validator_logger.clear_table_context()
        
        # Unpersist DataFrames
        source_df.unpersist()
        target_df.unpersist()
        
        # Aggregate results
        end_time = datetime.now()
        overall_status = self._determine_overall_status(validator_results)
        
        validation_result = ValidationResult(
            table_name=config.table_name,
            table_family=config.table_family,
            batch_load_id=batch_id,
            iteration_name=iteration_name,
            overall_status=overall_status,
            validator_results=validator_results,
            start_time=start_time,
            end_time=end_time,
            spot_check_result=None  # Can be added later
        )
        
        duration = validation_result.duration_seconds
        self.logger.log_section(f"VALIDATION COMPLETE: {overall_status} ({duration:.2f}s)")
        self.logger.clear_table_context()
        
        return validation_result
    
    def _determine_overall_status(self, validator_results: Dict[str, ValidatorResult]) -> str:
        """
        Determine overall validation status
        
        Rule: If all validators PASSED or SKIPPED, then SUCCESS
              If any validator FAILED or ERROR, then FAILED
        
        Args:
            validator_results: Dict of validator results
        
        Returns:
            Overall status string
        """
        for validator_name, result in validator_results.items():
            if result.status in ["FAILED", "ERROR"]:
                self.logger.warning(f"Validator '{validator_name}' status: {result.status}")
                return "FAILED"
        
        return "SUCCESS"

