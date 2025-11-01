"""
Metadata writer - Writes validation results to Delta tables

Writes to:
- validation_log (INSERT + UPDATE pattern, idempotent)
- validation_summary (INSERT only)

Note: Loaded via %run - constants, ValidationResult, TableConfig, exceptions, logger available in namespace
"""

from pyspark.sql import SparkSession

from deltarecon.config import constants
from deltarecon.models.validation_result import ValidationLogRecord, ValidationSummaryRecord, ValidationResult
from deltarecon.models.table_config import TableConfig
from deltarecon.utils.exceptions import MetadataWriteError
from deltarecon.utils.logger import get_logger

logger = get_logger(__name__)

class MetadataWriter:
    """
    Writes validation results to metadata tables
    
    Uses INSERT + UPDATE pattern for idempotency.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize metadata writer
        
        Args:
            spark: SparkSession
        """
        self.spark = spark
        self.logger = logger
    
    def write_log_start(self, log_record: ValidationLogRecord):
        """
        Write log entry at validation start (idempotent)
        
        If entry already exists (rerun), updates it to IN_PROGRESS.
        
        Args:
            log_record: ValidationLogRecord with status='IN_PROGRESS'
        
        Raises:
            MetadataWriteError: If write fails
        """
        self.logger.set_table_context(log_record.tgt_table)
        self.logger.info(f"Writing log entry (start) for batch: {log_record.batch_load_id}")
        
        try:
            # Check if entry already exists for this specific batch
            check_query = f"""
                SELECT COUNT(*) as cnt
                FROM {constants.VALIDATION_LOG_TABLE}
                WHERE iteration_name = '{log_record.iteration_name}'
                  AND table_family = '{log_record.table_family}'
                  AND tgt_table = '{log_record.tgt_table}'
                  AND batch_load_id = '{log_record.batch_load_id}'
            """
            
            existing_count = self.spark.sql(check_query).first()['cnt']
            
            if existing_count > 0:
                self.logger.warning(
                    f"Log entry already exists for {log_record.tgt_table} "
                    f"batch {log_record.batch_load_id} in iteration {log_record.iteration_name}. Updating (rerun)."
                )
                
                # Update existing entry
                update_query = f"""
                    UPDATE {constants.VALIDATION_LOG_TABLE}
                    SET validation_run_status = 'IN_PROGRESS',
                        validation_run_start_time = current_timestamp(),
                        validation_run_end_time = NULL,
                        exception = NULL
                    WHERE iteration_name = '{log_record.iteration_name}'
                      AND table_family = '{log_record.table_family}'
                      AND tgt_table = '{log_record.tgt_table}'
                      AND batch_load_id = '{log_record.batch_load_id}'
                """
                self.spark.sql(update_query)
            else:
                # Insert new entry - batch_load_id is now a simple string
                insert_query = f"""
                    INSERT INTO {constants.VALIDATION_LOG_TABLE}
                    VALUES (
                        '{log_record.batch_load_id}',
                        '{log_record.src_table}',
                        '{log_record.tgt_table}',
                        'IN_PROGRESS',
                        current_timestamp(),
                        NULL,
                        NULL,
                        '{log_record.iteration_name}',
                        '{log_record.workflow_name}',
                        '{log_record.table_family}'
                    )
                """
                self.spark.sql(insert_query)
            
            self.logger.info("Log entry written (idempotent)")
            self.logger.clear_table_context()
            
        except Exception as e:
            error_msg = f"Failed to write log start for {log_record.tgt_table}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            self.logger.clear_table_context()
            raise MetadataWriteError(error_msg)
    
    def write_log_complete(self, log_record: ValidationLogRecord):
        """
        Update log entry at validation completion
        
        Args:
            log_record: ValidationLogRecord with status='SUCCESS'/'FAILED'
        
        Raises:
            MetadataWriteError: If update fails
        """
        self.logger.set_table_context(log_record.tgt_table)
        self.logger.info(f"Updating log entry (complete) for batch: {log_record.batch_load_id}")
        
        try:
            # Escape single quotes in exception message for SQL (replace ' with '')
            if log_record.exception:
                escaped_exception = log_record.exception.replace("'", "''")
                exception_value = f"'{escaped_exception}'"
            else:
                exception_value = "NULL"
            
            update_query = f"""
                UPDATE {constants.VALIDATION_LOG_TABLE}
                SET validation_run_status = '{log_record.status}',
                    validation_run_end_time = current_timestamp(),
                    exception = {exception_value}
                WHERE iteration_name = '{log_record.iteration_name}'
                  AND table_family = '{log_record.table_family}'
                  AND tgt_table = '{log_record.tgt_table}'
                  AND batch_load_id = '{log_record.batch_load_id}'
            """
            
            self.spark.sql(update_query)
            self.logger.info(f"Log entry updated: status={log_record.status}")
            self.logger.clear_table_context()
            
        except Exception as e:
            error_msg = f"Failed to update log for {log_record.tgt_table}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            self.logger.clear_table_context()
            raise MetadataWriteError(error_msg)
    
    def write_summary(self, summary_record: ValidationSummaryRecord):
        """
        Write validation summary
        
        Args:
            summary_record: ValidationSummaryRecord with metrics
        
        Raises:
            MetadataWriteError: If write fails
        """
        self.logger.set_table_context(summary_record.tgt_table)
        self.logger.info(f"Writing summary for batch: {summary_record.batch_load_id}")
        
        try:
            # Build metrics struct
            metrics_struct = f"""
                named_struct(
                    'src_records', {summary_record.src_records},
                    'tgt_records', {summary_record.tgt_records},
                    'src_extras', {summary_record.src_extras or 'NULL'},
                    'tgt_extras', {summary_record.tgt_extras or 'NULL'},
                    'mismatches', {summary_record.mismatches or 'NULL'},
                    'matches', {summary_record.matches or 'NULL'}
                )
            """
            
            insert_query = f"""
                INSERT INTO {constants.VALIDATION_SUMMARY_TABLE}
                VALUES (
                    '{summary_record.batch_load_id}',
                    '{summary_record.src_table}',
                    '{summary_record.tgt_table}',
                    '{summary_record.row_count_match_status}',
                    '{summary_record.schema_match_status}',
                    '{summary_record.primary_key_compliance_status}',
                    '{summary_record.col_name_compare_status}',
                    '{summary_record.data_type_compare_status}',
                    '{summary_record.overall_status}',
                    {metrics_struct},
                    '{summary_record.iteration_name}',
                    '{summary_record.workflow_name}',
                    '{summary_record.table_family}'
                )
            """
            
            self.spark.sql(insert_query)
            self.logger.info("Summary written")
            self.logger.clear_table_context()
            
        except Exception as e:
            error_msg = f"Failed to write summary for {summary_record.tgt_table}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            self.logger.clear_table_context()
            raise MetadataWriteError(error_msg)

