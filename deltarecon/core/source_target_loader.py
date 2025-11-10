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
        target_df = self._load_target_delta(config, batch_id)
        
        # Verify batch IDs match
        self._verify_batch_consistency(source_df, target_df, batch_id, config)
        
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
            
            # Verify extraction worked (show sample) - optimized with take() instead of distinct().collect()
            # take(5) is much faster as it doesn't require full scan + distinct
            sample_rows = result_df.select(part_col).take(5)
            sample_values = list(set([row[part_col] for row in sample_rows if row[part_col]]))[:5]
            
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
    
    def _load_target_delta(self, config: TableConfig, batch_id: str) -> DataFrame:
        """
        Load target Delta table with appropriate filtering based on write mode
        
        Logic:
        - overwrite mode: Load ALL data (no batch filter) - entire table IS the batch
        - append/partition_overwrite/merge: Filter by batch_id - batches coexist
        
        Uses SQL WHERE clause for predicate pushdown and Delta data skipping.
        
        Args:
            config: Table configuration (includes write_mode, partition info)
            batch_id: Batch ID to validate
        
        Returns:
            Filtered DataFrame
        
        Raises:
            DataLoadError: If loading fails
            ConfigurationError: If partition_overwrite used without partitions
        """
        self.logger.info(f"Loading target Delta table")
        self.logger.info(f"  Write mode: {config.write_mode}")
        self.logger.info(f"  Partitioned: {config.is_partitioned}")
        
        try:
            # Validation: partition_overwrite requires partitions
            if config.write_mode == "partition_overwrite" and not config.is_partitioned:
                from deltarecon.utils.exceptions import ConfigurationError
                error_msg = (
                    f"Invalid configuration for {config.table_name}: "
                    f"write_mode='partition_overwrite' requires partition_columns to be defined. "
                    f"Either add partition_columns or change write_mode."
                )
                self.logger.error(error_msg)
                raise ConfigurationError(error_msg)
            
            # Determine filtering strategy based on write mode
            if config.write_mode == "overwrite":
                # OVERWRITE mode: Entire table is replaced each ingestion
                # The current table IS the latest batch - no filtering needed
                self.logger.info(f"  Overwrite mode: Loading ALL data (entire table = batch)")
                
                df_filtered = self.spark.sql(f"SELECT * FROM {config.table_name}")
                
                # Safety check: log actual batch IDs in table
                actual_batches = [row['_aud_batch_load_id'] for row in 
                                df_filtered.select("_aud_batch_load_id").distinct().limit(5).collect()]
                self.logger.info(f"  Actual batch_ids in table: {actual_batches}")
                
                if batch_id not in actual_batches:
                    self.logger.warning(
                        f"Validating batch '{batch_id}' but table contains {actual_batches}. "
                        f"This is expected for overwrite mode - entire current table represents this batch."
                    )
            
            elif config.write_mode in ["append", "merge", "partition_overwrite"]:
                # APPEND/MERGE/PARTITION_OVERWRITE: Multiple batches coexist
                # Filter by batch_id to isolate this batch's data
                self.logger.info(f"  {config.write_mode.upper()} mode: Filtering by batch_id = '{batch_id}'")
                
                # Use SQL WHERE clause for predicate pushdown and data skipping
                df_filtered = self.spark.sql(f"""
                    SELECT * FROM {config.table_name}
                    WHERE _aud_batch_load_id = '{batch_id}'
                """)
                
                # For partition_overwrite, log caveat
                if config.write_mode == "partition_overwrite":
                    self.logger.info(f"  Partition columns: {config.partition_columns}")
                    self.logger.info(
                        f"  Note: If this batch's partitions were overwritten by a later batch, "
                        f"the filter will return incomplete data. Ensure batches are validated "
                        f"before their partitions are overwritten."
                    )
            
            else:
                error_msg = f"Unsupported write_mode: {config.write_mode}"
                self.logger.error(error_msg)
                raise DataLoadError(error_msg)
            
            target_count = df_filtered.count()
            self.logger.info(f"  Target data: {target_count:,} rows")
            
            # Safety check: warn if no data found
            if target_count == 0:
                self.logger.warning(
                    f"No data found in target table for batch '{batch_id}'. "
                    f"Possible causes: "
                    f"1) Batch was overwritten (if overwrite/partition_overwrite mode), "
                    f"2) Ingestion failed, "
                    f"3) Filter mismatch"
                )
            
            return df_filtered
            
        except Exception as e:
            self.logger.clear_table_context()
            error_msg = f"Failed to load target Delta table {config.table_name}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            raise DataLoadError(error_msg)
    
    def _verify_batch_consistency(
        self,
        source_df: DataFrame,
        target_df: DataFrame,
        expected_batch: str,
        config: TableConfig
    ):
        """
        Verify target Delta has expected batch data
        
        For overwrite mode: Skip verification (entire table is the batch)
        For other modes: Verify only expected batch exists
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            expected_batch: Expected batch ID (single batch)
            config: Table configuration
        
        Raises:
            DataConsistencyError: If batch doesn't match
        """
        # Skip verification for overwrite mode
        if config.write_mode == "overwrite":
            self.logger.info(f"  Overwrite mode: Skipping batch consistency check (entire table = batch)")
            return
        
        try:
            # Verify target batch for append/merge/partition_overwrite
            target_batches = target_df.select("_aud_batch_load_id").distinct().collect()
            actual_batches = [row['_aud_batch_load_id'] for row in target_batches]
            
            if len(actual_batches) == 0:
                error_msg = (
                    f"Target has no data for batch {expected_batch} in {config.table_name}! "
                    f"Filter not working correctly."
                )
                self.logger.error(error_msg)
                raise DataConsistencyError(error_msg)
            
            if len(actual_batches) > 1:
                error_msg = (
                    f"Target has multiple batches for {config.table_name}! "
                    f"Expected: {expected_batch}, Got: {actual_batches}. "
                    f"Filter not working correctly."
                )
                self.logger.error(error_msg)
                raise DataConsistencyError(error_msg)
            
            if actual_batches[0] != expected_batch:
                error_msg = (
                    f"Target batch mismatch for {config.table_name}! "
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

