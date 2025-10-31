"""
Row count validator with double-verification

CRITICAL: Uses two different methods to count rows to catch framework bugs.
If the two methods disagree, raises ValidationAccuracyError.

Note: Loaded via %run - BaseValidator, TableConfig, ValidatorResult, exceptions, logger available in namespace
"""

from pyspark.sql import DataFrame

from deltarecon.validators.base_validator import BaseValidator
from deltarecon.models.table_config import TableConfig
from deltarecon.models.validation_result import ValidatorResult
from deltarecon.utils.exceptions import ValidationAccuracyError
from deltarecon.utils.logger import get_logger

logger = get_logger(__name__)

class RowCountValidator(BaseValidator):
    """
    Validates row counts between source and target
    
    Uses double-verification:
    1. DataFrame.count()
    2. SQL COUNT(*)
    
    If methods disagree, raises error (framework bug detected).
    """
    
    @property
    def name(self) -> str:
        return "row_count"
    
    def validate(
        self, 
        source_df: DataFrame, 
        target_df: DataFrame,
        config: TableConfig
    ) -> ValidatorResult:
        """
        Validate row counts with double-verification
        
        Returns:
            ValidatorResult with PASSED/FAILED status
        
        Raises:
            ValidationAccuracyError: If double-verification fails
        """
        
        self.log_start()
        
        # Method 1: DataFrame.count()
        logger.info("Counting rows using DataFrame.count()...")
        src_count_method1 = source_df.count()
        tgt_count_method1 = target_df.count()
        
        logger.info(f"  Source: {src_count_method1:,}")
        logger.info(f"  Target: {tgt_count_method1:,}")
        
        # Method 2: SQL COUNT(*) - different execution path
        logger.info("Double-checking with SQL COUNT(*)...")
        source_df.createOrReplaceTempView("temp_source_rowcount_verify")
        target_df.createOrReplaceTempView("temp_target_rowcount_verify")
        
        src_count_method2 = source_df.sparkSession.sql(
            "SELECT COUNT(*) as cnt FROM temp_source_rowcount_verify"
        ).first()['cnt']
        
        tgt_count_method2 = target_df.sparkSession.sql(
            "SELECT COUNT(*) as cnt FROM temp_target_rowcount_verify"
        ).first()['cnt']
        
        logger.info(f"  Source (SQL): {src_count_method2:,}")
        logger.info(f"  Target (SQL): {tgt_count_method2:,}")
        
        # CRITICAL: Verify both methods agree
        if src_count_method1 != src_count_method2:
            error_msg = (
                f"Source count mismatch between methods! "
                f"DataFrame.count()={src_count_method1}, SQL COUNT()={src_count_method2}. "
                f"This indicates data inconsistency or framework bug."
            )
            logger.error(error_msg)
            raise ValidationAccuracyError(error_msg)
        
        if tgt_count_method1 != tgt_count_method2:
            error_msg = (
                f"Target count mismatch between methods! "
                f"DataFrame.count()={tgt_count_method1}, SQL COUNT()={tgt_count_method2}. "
                f"This indicates data inconsistency or framework bug."
            )
            logger.error(error_msg)
            raise ValidationAccuracyError(error_msg)
        
        logger.info("Count verification passed (double-checked)")
        
        # Determine status
        match = src_count_method1 == tgt_count_method1
        status = "PASSED" if match else "FAILED"
        
        if not match:
            diff = abs(src_count_method1 - tgt_count_method1)
            pct_diff = (diff / max(src_count_method1, tgt_count_method1)) * 100
            logger.warning(
                f"Row count mismatch: {diff:,} rows difference ({pct_diff:.2f}%)"
            )
        else:
            logger.info("Row counts match")
        
        result = ValidatorResult(
            status=status,
            metrics={
                "src_records": src_count_method1,
                "tgt_records": tgt_count_method1,
                "verified": True,
                "match": match,
                "difference": abs(src_count_method1 - tgt_count_method1) if not match else 0
            },
            message=f"Source: {src_count_method1:,}, Target: {tgt_count_method1:,}"
        )
        
        self.log_result(result)
        return result

