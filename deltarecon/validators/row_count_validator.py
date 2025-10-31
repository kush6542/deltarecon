"""
Row count validator

Validates that source and target have the same number of rows.
"""

from pyspark.sql import DataFrame

from deltarecon.validators.base_validator import BaseValidator
from deltarecon.models.table_config import TableConfig
from deltarecon.models.validation_result import ValidatorResult
from deltarecon.utils.logger import get_logger

logger = get_logger(__name__)

class RowCountValidator(BaseValidator):
    """
    Validates row counts between source and target using DataFrame.count()
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
        Validate row counts between source and target
        
        Returns:
            ValidatorResult with PASSED/FAILED status
        """
        
        self.log_start()
        
        # Count rows using DataFrame.count()
        logger.info("Counting rows...")
        src_count = source_df.count()
        tgt_count = target_df.count()
        
        logger.info(f"  Source: {src_count:,}")
        logger.info(f"  Target: {tgt_count:,}")
        
        # Determine status
        match = src_count == tgt_count
        status = "PASSED" if match else "FAILED"
        
        if not match:
            diff = abs(src_count - tgt_count)
            pct_diff = (diff / max(src_count, tgt_count)) * 100
            logger.warning(
                f"Row count mismatch: {diff:,} rows difference ({pct_diff:.2f}%)"
            )
        else:
            logger.info("Row counts match")
        
        result = ValidatorResult(
            status=status,
            metrics={
                "src_records": src_count,
                "tgt_records": tgt_count,
                "match": match,
                "difference": abs(src_count - tgt_count) if not match else 0
            },
            message=f"Source: {src_count:,}, Target: {tgt_count:,}"
        )
        
        self.log_result(result)
        return result

