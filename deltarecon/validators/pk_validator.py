"""
Primary key validator with duplicate detection

Checks for:
- Duplicate keys in source
- Duplicate keys in target
- Captures examples of duplicates for investigation

Note: Loaded via %run - BaseValidator, TableConfig, ValidatorResult, exceptions, logger available in namespace
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count as spark_count

from deltarecon.validators.base_validator import BaseValidator
from deltarecon.models.table_config import TableConfig
from deltarecon.models.validation_result import ValidatorResult
from deltarecon.utils.exceptions import ConfigurationError
from deltarecon.utils.logger import get_logger

logger = get_logger(__name__)

class PKValidator(BaseValidator):
    """
    Validates primary key uniqueness
    
    If no PKs defined, returns SKIPPED status.
    If duplicates found, captures top 10 examples.
    """
    
    @property
    def name(self) -> str:
        return "pk_validation"
    
    def validate(
        self, 
        source_df: DataFrame, 
        target_df: DataFrame,
        config: TableConfig
    ) -> ValidatorResult:
        """
        Validate PK uniqueness with detailed duplicate tracking
        
        Returns:
            ValidatorResult with duplicate detection results
        
        Raises:
            ConfigurationError: If PK columns don't exist in DataFrames
        """
        
        self.log_start()
        
        # Check if PKs are defined
        if not config.has_primary_keys:
            logger.info("No primary keys defined - skipping PK validation")
            return ValidatorResult(
                status="SKIPPED",
                metrics={},
                message="NO_PK_DEFINED"
            )
        
        pk_columns = config.primary_keys
        logger.info(f"Validating primary keys: {pk_columns}")
        
        # Verify PK columns exist in both DataFrames
        missing_in_source = set(pk_columns) - set(source_df.columns)
        missing_in_target = set(pk_columns) - set(target_df.columns)
        
        if missing_in_source or missing_in_target:
            error_msg = (
                f"Primary key columns missing! "
                f"Source missing: {missing_in_source}, "
                f"Target missing: {missing_in_target}"
            )
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
        
        # Check source duplicates
        logger.info("Checking source for duplicate primary keys...")
        src_total = source_df.count()
        src_distinct = source_df.select(pk_columns).distinct().count()
        src_duplicates = src_total - src_distinct
        
        logger.info(f"  Total rows: {src_total:,}")
        logger.info(f"  Distinct key combinations: {src_distinct:,}")
        logger.info(f"  Duplicate rows: {src_duplicates:,}")
        
        # Check target duplicates
        logger.info("Checking target for duplicate primary keys...")
        tgt_total = target_df.count()
        tgt_distinct = target_df.select(pk_columns).distinct().count()
        tgt_duplicates = tgt_total - tgt_distinct
        
        logger.info(f"  Total rows: {tgt_total:,}")
        logger.info(f"  Distinct key combinations: {tgt_distinct:,}")
        logger.info(f"  Duplicate rows: {tgt_duplicates:,}")
        
        # CRITICAL: If duplicates found, capture examples for investigation
        duplicate_examples = []
        
        if src_duplicates > 0:
            logger.error(f"SOURCE HAS {src_duplicates:,} DUPLICATE ROWS!")
            
            # Get top 10 duplicate keys
            dup_keys = source_df.groupBy(pk_columns) \
                .agg(spark_count("*").alias("count")) \
                .filter("count > 1") \
                .orderBy(col("count").desc()) \
                .limit(10) \
                .collect()
            
            dup_examples = [row.asDict() for row in dup_keys]
            duplicate_examples.append({
                "location": "source",
                "count": src_duplicates,
                "examples": dup_examples
            })
            
            logger.warning(f"Sample duplicate keys: {dup_examples[:3]}")
        
        if tgt_duplicates > 0:
            logger.error(f"TARGET HAS {tgt_duplicates:,} DUPLICATE ROWS!")
            
            dup_keys = target_df.groupBy(pk_columns) \
                .agg(spark_count("*").alias("count")) \
                .filter("count > 1") \
                .orderBy(col("count").desc()) \
                .limit(10) \
                .collect()
            
            dup_examples = [row.asDict() for row in dup_keys]
            duplicate_examples.append({
                "location": "target",
                "count": tgt_duplicates,
                "examples": dup_examples
            })
            
            logger.warning(f"Sample duplicate keys: {dup_examples[:3]}")
        
        # Determine status
        status = "PASSED" if (src_duplicates == 0 and tgt_duplicates == 0) else "FAILED"
        
        if status == "PASSED":
            logger.info("No duplicate primary keys found")
        
        result = ValidatorResult(
            status=status,
            metrics={
                "src_total_rows": src_total,
                "src_distinct_keys": src_distinct,
                "src_duplicates": src_duplicates,
                "tgt_total_rows": tgt_total,
                "tgt_distinct_keys": tgt_distinct,
                "tgt_duplicates": tgt_duplicates
            },
            details={
                "duplicate_examples": duplicate_examples
            },
            message=f"Source duplicates: {src_duplicates}, Target duplicates: {tgt_duplicates}"
        )
        
        self.log_result(result)
        return result

