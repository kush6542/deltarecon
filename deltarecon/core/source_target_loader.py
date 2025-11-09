"""
Source and target data loader

Loads:
- Source data from multiple formats (ORC, CSV, Text) with partition extraction
- Target data from Delta tables with batch filtering

CRITICAL: Includes verification that partition extraction doesn't lose/add rows.

Note: Loaded via %run - TableConfig, exceptions, logger available in namespace
"""

from typing import List, Tuple, Dict, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.types import StructType

from deltarecon.models.table_config import TableConfig
from deltarecon.utils.exceptions import DataLoadError, DataConsistencyError
from deltarecon.utils.logger import get_logger

logger = get_logger(__name__)


def parse_schema_string(schema_str: str) -> StructType:
    """
    Parse schema string from source_file_options into PySpark StructType
    
    Example schema strings:
    - "col1 string, col2 int, col3 date"
    - "name STRING, age INT, salary DOUBLE"
    
    Args:
        schema_str: Schema definition string
    
    Returns:
        StructType object
    
    Raises:
        ValueError: If schema string is invalid
    """
    try:
        # PySpark can parse DDL-style schema strings directly
        return StructType.fromDDL(schema_str)
    except Exception as e:
        raise ValueError(f"Failed to parse schema string: {schema_str}. Error: {str(e)}")

class SourceTargetLoader:
    """
    Loads source and target data with verification
    
    Source: ORC/CSV/Text files with partition extraction
    Target: Delta tables with batch filtering
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize loader
        
        Args:
            spark: SparkSession
        """
        self.spark = spark
        self.logger = logger
    
    def load_source_and_target(
        self,
        config: TableConfig,
        batch_id: str,
        source_file_paths: List[str]
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Load source data (ORC/CSV/Text) and target Delta data for a single batch
        
        Args:
            config: Table configuration
            batch_id: Batch ID to validate (single batch for batch-level auditing)
            source_file_paths: Source file paths for this batch (format specified in config)
        
        Returns:
            Tuple of (source_df, target_df)
        
        Raises:
            DataLoadError: If data loading fails
            DataConsistencyError: If verification fails
        """
        self.logger.set_table_context(config.table_name)
        self.logger.info(f"Loading data for batch: {batch_id}")
        self.logger.info(f"  Source files: {len(source_file_paths)}")
        self.logger.info(f"  Source format: {config.source_file_format}")
        
        # Load source
        source_df = self._load_source_data(source_file_paths, config)
        
        # Load target
        target_df = self._load_target_delta(config.table_name, batch_id)
        
        # Verify batch IDs match
        self._verify_batch_consistency(source_df, target_df, batch_id, config.table_name)
        
        self.logger.clear_table_context()
        return source_df, target_df
    
    def _load_source_data(self, file_paths: List[str], config: TableConfig) -> DataFrame:
        """
        Load source data files with format detection and partition extraction
        
        Supports: ORC, CSV, Text
        
        Args:
            file_paths: List of source file paths
            config: Table configuration with format and options
        
        Returns:
            DataFrame with partition columns added
        
        Raises:
            DataLoadError: If data loading fails
            DataConsistencyError: If partition extraction verification fails
        """
        file_format = config.source_file_format.lower()
        self.logger.info(f"Loading source data files (format: {file_format})...")
        
        try:
            # Load files based on format
            if file_format == "orc":
                df_original = self._load_orc(file_paths)
            elif file_format == "csv":
                df_original = self._load_csv(file_paths, config.source_file_options)
            elif file_format == "text":
                df_original = self._load_text(file_paths, config.source_file_options)
            else:
                self.logger.warning(
                    f"Unsupported source file format: {file_format}. "
                    f"Supported formats: orc, csv, text. Skipping table."
                )
                raise DataLoadError(f"Unsupported file format: {file_format}")
            
            original_count = df_original.count()
            original_cols = set(df_original.columns)
            
            self.logger.info(f"  Original data: {original_count:,} rows, {len(original_cols)} columns")
            
            # Extract partitions if configured
            if config.is_partitioned:
                self.logger.info(f"  Extracting partition columns: {config.partition_columns}")
                df_with_partitions = self._extract_partitions(df_original, config)
                
                # CRITICAL: Verify partition extraction didn't lose/add rows
                final_count = df_with_partitions.count()
                
                if final_count != original_count:
                    error_msg = (
                        f"Partition extraction changed row count! "
                        f"Before: {original_count:,}, After: {final_count:,}. "
                        f"This is a critical bug."
                    )
                    self.logger.error(error_msg)
                    raise DataConsistencyError(error_msg)
                
                # Verify partition columns were added
                final_cols = set(df_with_partitions.columns)
                added_cols = final_cols - original_cols
                
                if set(config.partition_columns) != added_cols:
                    error_msg = (
                        f"Partition extraction mismatch! "
                        f"Expected to add: {config.partition_columns}, "
                        f"Actually added: {added_cols}"
                    )
                    self.logger.error(error_msg)
                    raise DataConsistencyError(error_msg)
                
                # Verify partition columns have no nulls
                for part_col in config.partition_columns:
                    null_count = df_with_partitions.filter(col(part_col).isNull()).count()
                    if null_count > 0:
                        error_msg = (
                            f"Partition column '{part_col}' has {null_count:,} NULL values! "
                            f"Extraction from file path failed."
                        )
                        self.logger.error(error_msg)
                        raise DataConsistencyError(error_msg)
                
                self.logger.info(f"  Partition extraction verified: added {config.partition_columns}")
                return df_with_partitions
            else:
                self.logger.info("  No partitions to extract")
                return df_original
                
        except DataConsistencyError:
            self.logger.clear_table_context()
            raise
        except Exception as e:
            self.logger.clear_table_context()
            error_msg = f"Failed to load source data files: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            raise DataLoadError(error_msg)
    
    def _load_orc(self, file_paths: List[str]) -> DataFrame:
        """
        Load ORC files
        
        Args:
            file_paths: List of ORC file paths
        
        Returns:
            DataFrame
        """
        return self.spark.read.format("orc").load(file_paths)
    
    def _load_csv(self, file_paths: List[str], options: Dict) -> DataFrame:
        """
        Load CSV files with options
        
        Args:
            file_paths: List of CSV file paths
            options: CSV read options (header, sep, schema, quote, escape, etc.)
        
        Returns:
            DataFrame
        """
        reader = self.spark.read.format("csv")
        
        # Apply common CSV options
        if "header" in options:
            reader = reader.option("header", options["header"])
        if "sep" in options:
            reader = reader.option("sep", options["sep"])
        if "quote" in options:
            reader = reader.option("quote", options["quote"])
        if "escape" in options:
            reader = reader.option("escape", options["escape"])
        if "multiline" in options:
            reader = reader.option("multiline", options["multiline"])
        
        # Apply schema if provided
        if "schema" in options:
            try:
                schema = parse_schema_string(options["schema"])
                reader = reader.schema(schema)
                self.logger.info(f"  Applied explicit schema from options")
            except ValueError as e:
                self.logger.warning(f"Failed to parse schema: {e}. Will infer schema.")
        
        return reader.load(file_paths)
    
    def _load_text(self, file_paths: List[str], options: Dict) -> DataFrame:
        """
        Load text files with options (essentially CSV with different defaults)
        
        Args:
            file_paths: List of text file paths
            options: Text read options (header, sep, schema, quote, escape, etc.)
        
        Returns:
            DataFrame
        """
        # Text files are just CSV with different separators
        # Reuse CSV loader
        return self._load_csv(file_paths, options)
    
    def _extract_partitions(self, df: DataFrame, config: TableConfig) -> DataFrame:
        """
        Extract partition columns from file paths
        
        Args:
            df: Original DataFrame
            config: Table configuration with partition info
        
        Returns:
            DataFrame with partition columns added
        """
        result_df = df
        
        for part_col in config.partition_columns:
            # Extract from _metadata.file_path
            # Pattern: partition_col=value
            pattern = f"{part_col}=([^/]+)"
            result_df = result_df.withColumn(
                part_col,
                regexp_extract(col("_metadata.file_path"), pattern, 1)
            )
            
            # Verify extraction worked (show sample)
            sample_values = result_df.select(part_col).distinct().limit(5).collect()
            sample_values = [row[part_col] for row in sample_values if row[part_col]]
            
            self.logger.info(f"    Extracted '{part_col}' values (sample): {sample_values}")
            
            # Cast to correct datatype
            target_dtype = config.partition_datatypes.get(part_col, 'string')
            if target_dtype != 'string':
                result_df = result_df.withColumn(
                    part_col,
                    col(part_col).cast(target_dtype)
                )
                self.logger.info(f"    Cast '{part_col}' to {target_dtype}")
        
        return result_df
    
    def _load_target_delta(self, table_name: str, batch_id: str) -> DataFrame:
        """
        Load target Delta table with batch filtering for a single batch
        
        Args:
            table_name: Fully qualified table name
            batch_id: Batch ID to filter (single batch)
        
        Returns:
            Filtered DataFrame
        
        Raises:
            DataLoadError: If Delta loading fails
        """
        self.logger.info(f"Loading target Delta table")
        
        try:
            # Read Delta table
            df = self.spark.table(table_name)
            
            # Apply batch filter for single batch
            self.logger.info(f"  Applying filter: _aud_batch_load_id = '{batch_id}'")
            
            df_filtered = df.filter(f"_aud_batch_load_id = '{batch_id}'")
            
            target_count = df_filtered.count()
            self.logger.info(f"  Target data: {target_count:,} rows")
            
            return df_filtered
            
        except Exception as e:
            self.logger.clear_table_context()
            error_msg = f"Failed to load target Delta table {table_name}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            raise DataLoadError(error_msg)
    
    def _verify_batch_consistency(
        self,
        source_df: DataFrame,
        target_df: DataFrame,
        expected_batch: str,
        table_name: str
    ):
        """
        Verify target Delta has only the expected batch
        
        CRITICAL: Ensures filter worked correctly for single batch.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            expected_batch: Expected batch ID (single batch)
            table_name: Table name for logging
        
        Raises:
            DataConsistencyError: If batch doesn't match
        """
        try:
            # Verify target batch
            target_batches = target_df.select("_aud_batch_load_id").distinct().collect()
            actual_batches = [row['_aud_batch_load_id'] for row in target_batches]
            
            if len(actual_batches) == 0:
                error_msg = (
                    f"Target has no data for batch {expected_batch} in {table_name}! "
                    f"Filter not working correctly."
                )
                self.logger.error(error_msg)
                raise DataConsistencyError(error_msg)
            
            if len(actual_batches) > 1:
                error_msg = (
                    f"Target has multiple batches for {table_name}! "
                    f"Expected: {expected_batch}, Got: {actual_batches}. "
                    f"Filter not working correctly."
                )
                self.logger.error(error_msg)
                raise DataConsistencyError(error_msg)
            
            if actual_batches[0] != expected_batch:
                error_msg = (
                    f"Target batch mismatch for {table_name}! "
                    f"Expected: {expected_batch}, Got: {actual_batches[0]}. "
                    f"Filter not working correctly."
                )
                self.logger.error(error_msg)
                raise DataConsistencyError(error_msg)
            
            self.logger.info(f"  Target batch verification passed: {expected_batch}")
            
        except DataConsistencyError:
            raise
        except Exception as e:
            self.logger.warning(f"Could not verify target batch (non-critical): {str(e)}")

