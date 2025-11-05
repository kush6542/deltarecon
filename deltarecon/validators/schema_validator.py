"""
Schema validator for column names and data types

Compares:
- Column names (missing, extra)
- Data types for common columns
- Excludes _aud_* columns from target

Note: Loaded via %run - BaseValidator, TableConfig, ValidatorResult, logger available in namespace
"""

from pyspark.sql import DataFrame
from typing import Set, Dict

from deltarecon.validators.base_validator import BaseValidator
from deltarecon.models.table_config import TableConfig
from deltarecon.models.validation_result import ValidatorResult
from deltarecon.utils.logger import get_logger

logger = get_logger(__name__)

class SchemaValidator(BaseValidator):
    """
    Validates schema compatibility between source and target
    
    Checks:
    - Column names match
    - Data types match for common columns
    - Reports missing/extra columns
    """
    
    @property
    def name(self) -> str:
        return "schema"
    
    def validate(
        self, 
        source_df: DataFrame, 
        target_df: DataFrame,
        config: TableConfig
    ) -> ValidatorResult:
        """
        Validate schema with strict type checking
        
        Returns:
            ValidatorResult with schema comparison details
        """
        
        self.log_start()
        
        # Get schemas
        # CRITICAL: Exclude both audit columns AND partition columns from comparison
        # Partition columns are added during source extraction and don't exist in original source
        partition_cols_to_exclude = set(config.partition_columns) if config.is_partitioned else set()
        
        source_schema = {
            f.name: f.dataType 
            for f in source_df.schema.fields 
            if f.name not in partition_cols_to_exclude  # Exclude extracted partition columns
        }
        
        target_schema = {
            f.name: f.dataType 
            for f in target_df.schema.fields 
            if not f.name.startswith('_aud_')  # Exclude audit columns
            and f.name not in partition_cols_to_exclude  # Also exclude partition columns
        }
        
        logger.info(f"Source columns: {len(source_schema)} (excluding {len(partition_cols_to_exclude)} partition columns)")
        logger.info(f"Target columns: {len(target_schema)} (excluding _aud_* and {len(partition_cols_to_exclude)} partition columns)")
        
        issues = []
        
        # Check column names
        source_cols = set(source_schema.keys())
        target_cols = set(target_schema.keys())
        common_cols = source_cols & target_cols
        
        missing_in_target = source_cols - target_cols
        extra_in_target = target_cols - source_cols
        
        logger.info(f"Common columns: {len(common_cols)}")
        
        if missing_in_target:
            logger.warning(f"Missing in target: {missing_in_target}")
            issues.append(f"Missing in target: {list(missing_in_target)}")
        
        if extra_in_target:
            logger.warning(f"Extra in target: {extra_in_target}")
            issues.append(f"Extra in target: {list(extra_in_target)}")
        
        # Check data types for common columns
        type_mismatches = []
        
        for col in sorted(common_cols):
            src_type = source_schema[col].simpleString()
            tgt_type = target_schema[col].simpleString()
            
            if src_type != tgt_type:
                type_mismatches.append({
                    'column': col,
                    'source_type': src_type,
                    'target_type': tgt_type
                })
                logger.warning(
                    f"Type mismatch - {col}: {src_type} (source) vs {tgt_type} (target)"
                )
        
        if type_mismatches:
            issues.append(f"Type mismatches: {len(type_mismatches)} columns")
        
        # Determine status
        col_name_status = "PASSED" if (not missing_in_target and not extra_in_target) else "FAILED"
        data_type_status = "PASSED" if not type_mismatches else "FAILED"
        overall_status = "PASSED" if len(issues) == 0 else "FAILED"
        
        logger.info(f"Column name comparison: {col_name_status}")
        logger.info(f"Data type comparison: {data_type_status}")
        
        if overall_status == "PASSED":
            logger.info("Schema validation passed")
        
        result = ValidatorResult(
            status=overall_status,
            metrics={
                "source_column_count": len(source_schema),
                "target_column_count": len(target_schema),
                "common_columns": len(common_cols),
                "missing_in_target_count": len(missing_in_target),
                "extra_in_target_count": len(extra_in_target),
                "type_mismatches_count": len(type_mismatches),
                "col_name_status": col_name_status,
                "data_type_status": data_type_status
            },
            details={
                "missing_in_target": list(missing_in_target),
                "extra_in_target": list(extra_in_target),
                "type_mismatches": type_mismatches
            },
            message=f"{len(issues)} schema issues found" if issues else "Schema matches"
        )
        
        self.log_result(result)
        return result

