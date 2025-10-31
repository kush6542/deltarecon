"""
DeltaRecon Runner - Main Orchestrator

This module provides the high-level ValidationRunner class that orchestrates
the entire validation workflow. Notebooks should use this as the main entry point.

Usage:
    from deltarecon import ValidationRunner
    
    runner = ValidationRunner(
        spark=spark,
        table_group="sales",
        iteration_suffix="daily"
    )
    
    result = runner.run()
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession

from deltarecon.config import constants
from deltarecon.core.ingestion_reader import IngestionConfigReader
from deltarecon.core.batch_processor import BatchProcessor
from deltarecon.core.source_target_loader import SourceTargetLoader
from deltarecon.core.validation_engine import ValidationEngine
from deltarecon.core.metadata_writer import MetadataWriter
from deltarecon.models.table_config import TableConfig
from deltarecon.models.validation_result import ValidationLogRecord, ValidationSummaryRecord
from deltarecon.utils.banner import print_banner
from deltarecon.utils.logger import get_logger
from deltarecon.utils.exceptions import ValidationFrameworkException


class ValidationRunner:
    """
    Main orchestrator for the validation framework.
    
    This class encapsulates all the logic for running validations on a table group.
    It handles:
    - Reading configurations
    - Processing tables in parallel
    - Orchestrating all validation steps
    - Writing results to metadata tables
    
    Attributes:
        spark: SparkSession instance
        table_group: Name of the table group to validate
        iteration_suffix: Suffix for the iteration name (e.g., "daily", "hourly")
        iteration_name: Generated unique iteration identifier
        logger: Logger instance for this runner
    
    Example:
        >>> runner = ValidationRunner(
        ...     spark=spark,
        ...     table_group="sales",
        ...     iteration_suffix="daily"
        ... )
        >>> result = runner.run()
        >>> print(f"Success: {result['success']}, Failed: {result['failed']}")
    """
    
    def __init__(
        self,
        spark: SparkSession,
        table_group: str,
        iteration_suffix: str = "default"
    ):
        """
        Initialize the ValidationRunner.
        
        Args:
            spark: SparkSession instance
            table_group: Name of the table group to validate (required)
            iteration_suffix: Suffix for iteration name (default: "default")
        
        Raises:
            ValueError: If table_group is empty
        """
        if not table_group:
            raise ValueError("table_group parameter is required!")
        
        self.spark = spark
        self.table_group = table_group
        self.iteration_suffix = iteration_suffix
        
        # Generate unique iteration name
        self.iteration_name = f"{iteration_suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Initialize logger
        self.logger = get_logger(__name__)
        
        # Components (initialized on demand)
        self._config_reader = None
        self._batch_processor = None
        self._loader = None
        self._validation_engine = None
        self._metadata_writer = None
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the complete validation workflow.
        
        This method:
        1. Logs the validation start
        2. Reads table configurations for the table group
        3. Processes all tables in parallel
        4. Collects and summarizes results
        5. Returns a summary
        
        Returns:
            dict: Summary of the validation run
                {
                    "total": int,           # Total number of tables
                    "success": int,         # Number of successful validations
                    "failed": int,          # Number of failed validations
                    "no_batches": int,      # Number with no new batches
                    "results": List[dict]   # Detailed results per table
                }
        
        Raises:
            ValidationFrameworkException: If a fatal error occurs
        """
        try:
            # Print ASCII art banner
            print_banner()
            
            # Log start
            self.logger.log_section("DELTARECON - Starting")
            self.logger.info(f"Table Group: {self.table_group}")
            self.logger.info(f"Iteration: {self.iteration_name}")
            self.logger.info(f"Framework Version: {constants.FRAMEWORK_VERSION}")
            
            # Step 1: Read table configurations
            self.logger.info("Reading table configurations...")
            config_reader = IngestionConfigReader(self.spark, self.table_group)
            table_configs = config_reader.get_tables_in_group()
            
            self.logger.info(f"Found {len(table_configs)} tables to validate")
            
            # Step 2: Process tables in parallel
            self.logger.info(f"Processing tables with parallelism: {constants.PARALLELISM}")
            
            results = []
            with ThreadPoolExecutor(max_workers=constants.PARALLELISM) as executor:
                futures = [
                    executor.submit(self._process_table, config)
                    for config in table_configs
                ]
                
                # Collect results as they complete
                for future in futures:
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        self.logger.error(f"Thread execution failed: {e}")
                        results.append({"status": "FAILED", "error": str(e)})
            
            # Step 3: Summarize results
            summary = self._summarize_results(results)
            
            # Log summary
            self._log_summary(summary)
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Fatal error in validation runner: {e}")
            self.logger.error(str(e), exc_info=True)
            raise ValidationFrameworkException(f"Validation run failed: {e}")
        
        finally:
            self.logger.log_section("COMPLETE")
    
    def _process_table(self, table_config: TableConfig) -> Dict[str, Any]:
        """
        Process validation for a single table.
        
        This method handles the complete validation workflow for one table:
        1. Identify unprocessed batches
        2. Write log entry (IN_PROGRESS)
        3. Load source and target data
        4. Run validations
        5. Write validation summary
        6. Update log entry (COMPLETE)
        
        Args:
            table_config: Configuration for the table to validate
        
        Returns:
            dict: Result summary for this table
                {
                    "table": str,
                    "status": str,  # "SUCCESS", "FAILED", or "NO_NEW_BATCHES"
                    "batches": int,
                    "error": str (optional)
                }
        """
        table_name = table_config.table_name
        
        # CRITICAL: Initialize MetadataWriter BEFORE try block so it's always available
        # for error logging, even if other components fail during initialization.
        # This is architecturally sound: initialize observability/logging components first.
        # If MetadataWriter initialization fails, that's a fatal error - we cannot proceed
        # without the ability to log metadata.
        try:
            metadata_writer = MetadataWriter(self.spark)
        except Exception as init_error:
            # MetadataWriter is critical - if it fails, we cannot proceed safely
            self.logger.error(f"FATAL: Failed to initialize MetadataWriter: {init_error}")
            raise ValidationFrameworkException(
                f"Cannot proceed without MetadataWriter: {init_error}"
            ) from init_error
        
        try:
            self.logger.log_section(f"Processing Table: {table_name}")
            
            # Initialize other components (create fresh instances for thread safety)
            batch_processor = BatchProcessor(self.spark)
            loader = SourceTargetLoader(self.spark)
            validation_engine = ValidationEngine()
            
            # Set table context for all operations
            self.logger.set_table_context(table_name)
            
            # Step 1: Identify unprocessed batches
            self.logger.info("Step 1: Identifying unprocessed batches...")
            batch_ids, orc_paths = batch_processor.get_unprocessed_batches(table_config)
            
            if not batch_ids:
                result = self._handle_no_batches(table_config, metadata_writer)
                self.logger.clear_table_context()
                return result
            
            # Step 2: Write log entry (IN_PROGRESS)
            self.logger.info("Step 2: Writing log entry...")
            log_record = self._create_log_record(table_config, batch_ids, "IN_PROGRESS")
            metadata_writer.write_log_start(log_record)
            
            # Step 3: Load source and target data
            self.logger.info("Step 3: Loading data...")
            src_df, tgt_df = loader.load_source_and_target(table_config, batch_ids, orc_paths)
            
            # Step 4: Run validations
            self.logger.info("Step 4: Running validations...")
            validation_result = validation_engine.run_validations(
                source_df=src_df,
                target_df=tgt_df,
                config=table_config,
                batch_ids=batch_ids,
                iteration_name=self.iteration_name
            )
            
            # Step 5: Write validation summary
            self.logger.info("Step 5: Writing validation summary...")
            summary_record = ValidationSummaryRecord.from_validation_result(
                validation_result=validation_result,
                table_config=table_config,
                workflow_name=f"validation_{table_config.table_group}"
            )
            metadata_writer.write_summary(summary_record)
            
            # Step 6: Update log entry (COMPLETE)
            log_record.status = validation_result.overall_status
            log_record.end_time = datetime.now()
            metadata_writer.write_log_complete(log_record)
            
            self.logger.info(f"Table completed - Status: {validation_result.overall_status}")
            self.logger.clear_table_context()
            
            return {
                "table": table_name,
                "status": validation_result.overall_status,
                "batches": len(batch_ids)
            }
            
        except Exception as e:
            self.logger.clear_table_context()
            return self._handle_table_error(table_config, e, metadata_writer)
    
    def _handle_no_batches(
        self,   
        table_config: TableConfig,
        metadata_writer: MetadataWriter
    ) -> Dict[str, Any]:
        """Handle case when there are no new batches to validate."""
        self.logger.set_table_context(table_config.table_name)
        self.logger.info("No new batches to validate - writing log entry")
        
        # Write log entry for "no new batches"
        log_record = self._create_log_record(
            table_config,
            batch_ids=[],
            status="SUCCESS"
        )
        log_record.end_time = datetime.now()
        log_record.exception = "No new batches to validate"
        
        metadata_writer.write_log_start(log_record)
        metadata_writer.write_log_complete(log_record)
        
        self.logger.clear_table_context()
        return {
            "table": table_config.table_name,
            "status": "NO_NEW_BATCHES",
            "batches": 0
        }
    
    def _handle_table_error(
        self,
        table_config: TableConfig,
        error: Exception,
        metadata_writer: MetadataWriter
    ) -> Dict[str, Any]:
        """Handle errors during table validation."""
        table_name = table_config.table_name
        
        self.logger.set_table_context(table_name)
        self.logger.error(f"Table FAILED - Error: {str(error)}")
        self.logger.error(str(error), exc_info=True)
        
        # Write error to log (metadata_writer is guaranteed to be initialized)
        try:
            log_record = self._create_log_record(table_config, [], "FAILED")
            log_record.end_time = datetime.now()
            log_record.exception = str(error)[:1000]  # Limit exception length
            metadata_writer.write_log_complete(log_record)
        except Exception as log_error:
            self.logger.error("Failed to write error to log")
            self.logger.error(str(log_error), exc_info=True)
        
        self.logger.clear_table_context()
        return {
            "table": table_name,
            "status": "FAILED",
            "batches": 0,
            "error": str(error)
        }
    
    def _create_log_record(
        self,
        table_config: TableConfig,
        batch_ids: List[str],
        status: str
    ) -> ValidationLogRecord:
        """Create a ValidationLogRecord for a table."""
        return ValidationLogRecord(
            iteration_name=self.iteration_name,
            workflow_name=f"validation_{table_config.table_group}",
            table_family=table_config.table_family,
            src_table=table_config.source_table,
            tgt_table=table_config.table_name,
            batch_load_ids=batch_ids,
            status=status,
            start_time=datetime.now()
        )
    
    def _summarize_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Summarize validation results."""
        total = len(results)
        success = len([r for r in results if r["status"] == "SUCCESS"])
        failed = len([r for r in results if r["status"] == "FAILED"])
        no_batches = len([r for r in results if r["status"] == "NO_NEW_BATCHES"])
        
        return {
            "total": total,
            "success": success,
            "failed": failed,
            "no_batches": no_batches,
            "results": results
        }
    
    def _log_summary(self, summary: Dict[str, Any]):
        """Log the validation summary."""
        self.logger.log_section("VALIDATION RUN SUMMARY")
        self.logger.info(f"Total tables: {summary['total']}")
        self.logger.info(f"Success: {summary['success']}")
        self.logger.info(f"Failed: {summary['failed']}")
        self.logger.info(f"No new batches: {summary['no_batches']}")
        
        # Log appropriate message
        if summary['failed'] > 0:
            self.logger.warning(f"Validation completed with {summary['failed']} failures")
        else:
            self.logger.info("All validations completed successfully")

