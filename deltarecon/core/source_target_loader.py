"""
Source and target data loader

Loads:
- Source data from ORC files with partition extraction
- Target data from Delta tables with batch filtering

CRITICAL: Includes verification that partition extraction doesn't lose/add rows.

Note: Loaded via %run - TableConfig, exceptions, logger available in namespace
"""

from typing import List, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, regexp_extract

from deltarecon.models.table_config import TableConfig
from deltarecon.utils.exceptions import DataLoadError, DataConsistencyError
from deltarecon.utils.logger import get_logger

logger = get_logger(__name__)

class SourceTargetLoader:
    """
    Loads source and target data with verification
    
    Source: ORC files with partition extraction
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
        batch_ids: List[str],
        orc_paths: List[str]
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Load source ORC and target Delta data
        
        Args:
            config: Table configuration
            batch_ids: Batch IDs to validate
            orc_paths: ORC file paths for these batches
        
        Returns:
            Tuple of (source_df, target_df)
        
        Raises:
            DataLoadError: If data loading fails
            DataConsistencyError: If verification fails
        """
        self.logger.info(f"Loading data for {config.table_name}")
        self.logger.info(f"  Batches: {batch_ids}")
        self.logger.info(f"  ORC files: {len(orc_paths)}")
        
        # Load source
        source_df = self._load_source_orc(orc_paths, config)
        
        # Load target
        target_df = self._load_target_delta(config.table_name, batch_ids)
        
        # Verify batch IDs match
        self._verify_batch_consistency(source_df, target_df, batch_ids, config.table_name)
        
        return source_df, target_df
    
    def _load_source_orc(self, orc_paths: List[str], config: TableConfig) -> DataFrame:
        """
        Load source ORC files with partition extraction
        
        Args:
            orc_paths: List of ORC file paths
            config: Table configuration
        
        Returns:
            DataFrame with partition columns added
        
        Raises:
            DataLoadError: If ORC loading fails
            DataConsistencyError: If partition extraction verification fails
        """
        self.logger.info("Loading source ORC files...")
        
        try:
            # Read ORC files
            df_original = self.spark.read.format("orc").load(orc_paths)
            original_count = df_original.count()
            original_cols = set(df_original.columns)
            
            self.logger.info(f"  Original ORC data: {original_count:,} rows, {len(original_cols)} columns")
            
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
            raise
        except Exception as e:
            error_msg = f"Failed to load source ORC files: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            raise DataLoadError(error_msg)
    
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
    
    def _load_target_delta(self, table_name: str, batch_ids: List[str]) -> DataFrame:
        """
        Load target Delta table with batch filtering
        
        Args:
            table_name: Fully qualified table name
            batch_ids: Batch IDs to filter
        
        Returns:
            Filtered DataFrame
        
        Raises:
            DataLoadError: If Delta loading fails
        """
        self.logger.info(f"Loading target Delta table: {table_name}")
        
        try:
            # Read Delta table
            df = self.spark.table(table_name)
            
            # Apply batch filter
            batch_filter = " OR ".join([f"_aud_batch_load_id = '{bid}'" for bid in batch_ids])
            self.logger.info(f"  Applying filter: {batch_filter}")
            
            df_filtered = df.filter(batch_filter)
            
            target_count = df_filtered.count()
            self.logger.info(f"  Target data: {target_count:,} rows")
            
            return df_filtered
            
        except Exception as e:
            error_msg = f"Failed to load target Delta table {table_name}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            raise DataLoadError(error_msg)
    
    def _verify_batch_consistency(
        self,
        source_df: DataFrame,
        target_df: DataFrame,
        expected_batches: List[str],
        table_name: str
    ):
        """
        Verify target Delta has only expected batches
        
        CRITICAL: Ensures filter worked correctly.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            expected_batches: Expected batch IDs
            table_name: Table name for logging
        
        Raises:
            DataConsistencyError: If batches don't match
        """
        try:
            # Verify target batches
            target_batches = target_df.select("_aud_batch_load_id").distinct().collect()
            actual_batches = [row['_aud_batch_load_id'] for row in target_batches]
            
            expected_set = set(expected_batches)
            actual_set = set(actual_batches)
            
            if expected_set != actual_set:
                error_msg = (
                    f"Target batch mismatch for {table_name}! "
                    f"Expected: {expected_set}, Got: {actual_set}. "
                    f"Filter not working correctly."
                )
                self.logger.error(error_msg)
                raise DataConsistencyError(error_msg)
            
            self.logger.info("  Target batch verification passed")
            
        except DataConsistencyError:
            raise
        except Exception as e:
            self.logger.warning(f"Could not verify target batches (non-critical): {str(e)}")

