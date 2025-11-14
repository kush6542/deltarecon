"""
Schema validator for column names and data types

Compares:
- Column names (missing, extra)
- Data types for common columns
- Excludes _aud_* columns from target
- Uses DESCRIBE EXTENDED for target schema to bypass masking functions

Note: Loaded via %run - BaseValidator, TableConfig, ValidatorResult, logger available in namespace
"""

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, FloatType, 
    BooleanType, DateType, TimestampType, DecimalType, BinaryType, 
    ShortType, ByteType, DataType, StructField, StructType
)
from typing import Set, Dict
import re

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
    
    Note: Uses DESCRIBE EXTENDED to get original target table schema,
    bypassing masking function signatures that change apparent data types.
    """
    
    @property
    def name(self) -> str:
        return "schema"
    
    def _parse_datatype_string(self, dtype_str: str) -> DataType:
        """
        Parse a string datatype (from DESCRIBE EXTENDED) into PySpark DataType
        
        Handles both primitive types (string, int) and complex types (array<string>, map<string,int>, struct<...>)
        
        Args:
            dtype_str: String representation of datatype (e.g., 'int', 'array<string>', 'map<string,int>')
        
        Returns:
            Corresponding PySpark DataType object
        """
        dtype_str = dtype_str.strip()
        
        # FIRST: Try PySpark's internal parser (handles ALL types including complex)
        try:
            from pyspark.sql.types import _parse_datatype_string as pyspark_parse
            parsed_type = pyspark_parse(dtype_str)
            return parsed_type
        except Exception as e:
            # If PySpark parser fails, fall back to manual parsing
            logger.debug(f"PySpark parser failed for '{dtype_str}': {e}. Using fallback parser.")
        
        # FALLBACK: Manual parsing for backward compatibility
        dtype_lower = dtype_str.lower().strip()
        
        # Handle decimal types with precision/scale
        decimal_match = re.match(r'decimal\((\d+),\s*(\d+)\)', dtype_lower)
        if decimal_match:
            precision = int(decimal_match.group(1))
            scale = int(decimal_match.group(2))
            return DecimalType(precision, scale)
        
        # Map common types
        type_mapping = {
            'string': StringType(),
            'int': IntegerType(),
            'integer': IntegerType(),
            'bigint': LongType(),
            'long': LongType(),
            'double': DoubleType(),
            'float': FloatType(),
            'boolean': BooleanType(),
            'bool': BooleanType(),
            'date': DateType(),
            'timestamp': TimestampType(),
            'binary': BinaryType(),
            'short': ShortType(),
            'smallint': ShortType(),
            'byte': ByteType(),
            'tinyint': ByteType(),
        }
        
        result = type_mapping.get(dtype_lower)
        if result:
            return result
        
        # If still not found, log warning and default to StringType
        logger.warning(f"Unable to parse datatype '{dtype_str}', defaulting to StringType()")
        return StringType()
    
    def _get_original_target_schema(self, table_name: str, target_df: DataFrame) -> Dict[str, DataType]:
        """
        Get target table's original schema using DESCRIBE EXTENDED
        
        This bypasses masking function signatures and returns actual column types
        as defined in the table metadata.
        
        Args:
            table_name: Fully qualified table name
            target_df: Target DataFrame (used as fallback)
        
        Returns:
            Dictionary mapping column names to PySpark DataType objects
        """
        try:
            logger.info(f"Fetching original schema for '{table_name}' using DESCRIBE EXTENDED")
            
            # Get SparkSession from DataFrame
            spark = target_df.sparkSession
            
            # Run DESCRIBE EXTENDED
            describe_df = spark.sql(f"DESCRIBE EXTENDED {table_name}")
            
            # Collect results
            rows = describe_df.collect()
            
            schema_dict = {}
            in_partition_section = False
            
            for row in rows:
                col_name = row['col_name'].strip() if row['col_name'] else ""
                data_type = row['data_type'].strip() if row['data_type'] else ""
                
                # Stop when we hit the partition information section
                if col_name.startswith('#') or col_name == '':
                    in_partition_section = True
                    continue
                
                # Skip partition columns section
                if in_partition_section:
                    continue
                
                # Skip audit columns
                if col_name.startswith('_aud_'):
                    continue
                
                # Only process regular data columns
                if data_type and data_type != '':
                    parsed_type = self._parse_datatype_string(data_type)
                    schema_dict[col_name] = parsed_type
            
            logger.info(f"Extracted {len(schema_dict)} columns from DESCRIBE EXTENDED")
            return schema_dict
            
        except Exception as e:
            logger.warning(f"Failed to get original schema via DESCRIBE EXTENDED: {e}")
            logger.warning("Falling back to DataFrame schema (may show masked types)")
            
            # Fallback to DataFrame schema (exclude _aud_* columns)
            # Note: Partition columns will be excluded later in validate() method
            return {
                f.name: f.dataType 
                for f in target_df.schema.fields 
                if not f.name.startswith('_aud_')
            }
    
    def validate(
        self, 
        source_df: DataFrame, 
        target_df: DataFrame,
        config: TableConfig
    ) -> ValidatorResult:
        """
        Validate schema with strict type checking
        
        Uses DESCRIBE EXTENDED to get target schema, bypassing masking functions
        that change apparent data types.
        
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
        
        # Get original target schema using DESCRIBE EXTENDED (bypasses masking functions)
        target_schema_all = self._get_original_target_schema(config.table_name, target_df)
        
        # Exclude partition columns from target schema
        target_schema = {
            col: dtype
            for col, dtype in target_schema_all.items()
            if col not in partition_cols_to_exclude
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

