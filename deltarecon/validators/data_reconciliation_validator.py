"""
Data reconciliation validator using hash-based exceptAll approach

Validates data consistency between source and target by computing:
- src_extras: Records in source but NOT in target
- tgt_extras: Records in target but NOT in source
- matches: Identical records in both source and target

Uses full row hash comparison for all tables (with or without primary keys).
Note: PKValidator should be run separately to check primary key uniqueness.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import md5, concat_ws, coalesce, lit, col

from deltarecon.validators.base_validator import BaseValidator
from deltarecon.models.table_config import TableConfig
from deltarecon.models.validation_result import ValidatorResult
from deltarecon.utils.logger import get_logger

logger = get_logger(__name__)


class DataReconciliationValidator(BaseValidator):
    """
    Data reconciliation using full row hash comparison
    
    Compares source and target using hash of all data columns (excluding partitions and audit columns).
    Reports rows that exist only in source, only in target, or in both (matches).
    """
    
    @property
    def name(self) -> str:
        return "data_reconciliation"
    
    def validate(
        self, 
        source_df: DataFrame, 
        target_df: DataFrame,
        config: TableConfig
    ) -> ValidatorResult:
        """
        Perform data reconciliation using full row hash comparison
        
        CRITICAL: Excludes partition columns from comparison since they're extracted
        from file paths and don't exist in the original source data.
        
        Returns:
            ValidatorResult with reconciliation metrics
        """
        self.log_start()
        
        logger.info("Running full-row hash reconciliation")
        return self._reconcile(source_df, target_df, config)
    
    def _reconcile(
        self, 
        source_df: DataFrame, 
        target_df: DataFrame,
        config: TableConfig
    ) -> ValidatorResult:
        """
        Full row hash reconciliation
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            config: Table configuration with partition info
        
        Returns:
            ValidatorResult with full-row reconciliation metrics
        """
        logger.info("Creating full row hashes...")
        
        # Create full row hash
        source_hashed = self._create_row_hash(source_df, config)
        target_hashed = self._create_row_hash(target_df, config)
        
        # Cache for multiple operations
        source_hashed.cache()
        target_hashed.cache()
        
        # Total counts
        src_total = source_hashed.count()
        tgt_total = target_hashed.count()
        
        logger.info(f"  Source distinct rows: {src_total:,}")
        logger.info(f"  Target distinct rows: {tgt_total:,}")
        
        logger.info("Computing set differences...")
        
        # A - B: Rows in source but not target (by full row hash)
        src_minus_tgt = source_hashed.exceptAll(target_hashed)
        src_extras = src_minus_tgt.count()
        
        # B - A: Rows in target but not source
        tgt_minus_src = target_hashed.exceptAll(source_hashed)
        tgt_extras = tgt_minus_src.count()
        
        # matches: Rows that exist in both (identical)
        matches = src_total - src_extras
        
        # mismatches: Not applicable for full-row hash (can't track "same entity")
        mismatches = "NA"
        
        # Clean up
        source_hashed.unpersist()
        target_hashed.unpersist()
        
        logger.info(f"Full-row reconciliation results:")
        logger.info(f"  src_extras: {src_extras:,}")
        logger.info(f"  tgt_extras: {tgt_extras:,}")
        logger.info(f"  matches: {matches:,}")
        logger.info(f"  mismatches: {mismatches} (not applicable for full-row comparison)")
        
        # Determine status
        status = "PASSED" if (src_extras == 0 and tgt_extras == 0) else "FAILED"
        
        if status == "FAILED":
            logger.warning("Data reconciliation FAILED - discrepancies found")
        else:
            logger.info("Data reconciliation PASSED - all records match")
        
        result = ValidatorResult(
            status=status,
            metrics={
                "src_extras": src_extras,
                "tgt_extras": tgt_extras,
                "matches": matches,
                "mismatches": mismatches,
                "reconciliation_type": "full_row"
            },
            message=f"src_extras={src_extras}, tgt_extras={tgt_extras}, matches={matches}"
        )
        
        self.log_result(result)
        return result
    
    def _create_row_hash(self, df: DataFrame, config: TableConfig) -> DataFrame:
        """
        Create hash of entire row (all columns)
        
        CRITICAL: Excludes partition columns and audit columns from hash calculation
        
        Args:
            df: Input DataFrame
            config: Table configuration with partition column info
        
        Returns:
            DataFrame with single column: _row_hash
        """
        # CRITICAL: Exclude partition columns (extracted from paths) and audit columns
        partition_cols = set(config.partition_columns) if config.is_partitioned else set()
        audit_cols = {c for c in df.columns if c.startswith('_aud_')}
        exclude_cols = partition_cols | audit_cols
        
        all_cols = sorted([c for c in df.columns if c not in exclude_cols])
        
        if partition_cols:
            logger.info(f"  Excluding partition columns from hash: {partition_cols}")
        if audit_cols:
            logger.info(f"  Excluding audit columns from hash: {audit_cols}")
        
        row_hash = md5(concat_ws("||", *[
            coalesce(col(c).cast("string"), lit("__NULL__")) 
            for c in all_cols
        ]))
        
        return df.select(row_hash.alias("_row_hash")).distinct()


