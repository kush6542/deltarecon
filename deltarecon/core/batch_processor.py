"""
Batch processor - Identifies which batches need validation

Logic:
- For append mode: Process all unvalidated COMPLETED batches
- For partition_overwrite mode: Process all unvalidated COMPLETED batches 
- For merge mode: Process all unvalidated COMPLETED batches
- For overwrite mode: Process only latest COMPLETED batch
- Skip batches that are already validated successfully
- Supports batch-level auditing: returns batch-to-path mapping

Note: Loaded via %run - constants, TableConfig, exceptions, logger available in namespace
"""

from typing import List, Tuple, Dict, Optional
from pyspark.sql import SparkSession

from deltarecon.config import constants
from deltarecon.models.table_config import TableConfig
from deltarecon.utils.exceptions import BatchProcessingError, DataConsistencyError
from deltarecon.utils.logger import get_logger

logger = get_logger(__name__)

class BatchProcessor:
    """
    Identifies which batches need validation
    
    Uses ingestion_audit and validation_log to determine unprocessed batches.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize batch processor
        
        Args:
            spark: SparkSession
        """
        self.spark = spark
        self.logger = logger
    
    def get_unprocessed_batches(self, config: TableConfig) -> Tuple[List[str], List[str]]:
        """
        Get batches that need validation
        
        Args:
            config: Table configuration
        
        Returns:
            Tuple of (batch_ids, source_file_paths)
        
        Raises:
            BatchProcessingError: If batch query fails
        """
        self.logger.set_table_context(config.table_name)
        self.logger.info(f"Identifying unprocessed batches")
        self.logger.info(f"  Write mode: {config.write_mode}")
        
        try:
            # Query to get unprocessed batches
            query = f"""
                SELECT
                    t1.source_file_path,
                    t2.batch_load_id
                FROM {constants.INGESTION_METADATA_TABLE} t1
                INNER JOIN {constants.INGESTION_AUDIT_TABLE} t2
                    ON (t1.table_name = t2.target_table_name 
                        AND t1.batch_load_id = t2.batch_load_id)
                INNER JOIN {constants.INGESTION_CONFIG_TABLE} t4
                    ON t1.table_name = concat_ws('.', t4.target_catalog, t4.target_schema, t4.target_table)
                    AND t2.group_name = t4.group_name
                LEFT JOIN (
                    SELECT 
                        batch_load_id,
                        validation_run_status,
                        row_number() OVER(PARTITION BY batch_load_id ORDER BY validation_run_start_time DESC) as rnk
                    FROM {constants.VALIDATION_LOG_TABLE}
                ) t3
                    ON t3.batch_load_id = t2.batch_load_id AND t3.rnk = 1
                WHERE t2.status = 'COMPLETED'
                    AND t1.table_name = '{config.table_name}'
                    AND (
                        -- For overwrite mode: only latest batch
                        (t4.write_mode = 'overwrite' AND t2.batch_load_id = (
                            SELECT max(batch_load_id)
                            FROM {constants.INGESTION_AUDIT_TABLE}
                            WHERE status = 'COMPLETED'
                                AND target_table_name = '{config.table_name}'
                        ))
                        -- For append/partition_overwrite/merge modes: all batches
                        OR t4.write_mode IN ('append', 'partition_overwrite', 'merge')
                    )
                    AND (
                        -- Batch not validated OR last validation was not SUCCESS
                        (t3.validation_run_status <> 'SUCCESS' AND t3.rnk = 1)
                        OR t3.batch_load_id IS NULL
                    )
            """
            
            result_df = self.spark.sql(query)
            rows = result_df.collect()
            
            if not rows:
                self.logger.info("No unprocessed batches found")
                self.logger.clear_table_context()
                return [], []
            
            # Extract batch IDs and source file paths
            batch_ids = list(set([row.batch_load_id for row in rows]))
            source_file_paths = [row.source_file_path for row in rows]
            
            self.logger.info(f"Found {len(batch_ids)} unprocessed batch(es): {batch_ids}")
            self.logger.info(f"Total source files: {len(source_file_paths)}")
            
            # Verify batch-to-path mapping
            self._verify_batch_mapping(config.table_name, batch_ids, source_file_paths)
            
            self.logger.clear_table_context()
            return batch_ids, source_file_paths
            
        except Exception as e:
            self.logger.clear_table_context()
            error_msg = f"Failed to get unprocessed batches for {config.table_name}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            raise BatchProcessingError(error_msg)
    
    def _verify_batch_mapping(self, table_name: str, batch_ids: List[str], source_file_paths: List[str]):
        """
        Verify that source file paths actually map to the expected batches
        
        This is a CRITICAL check to ensure we're validating the right data.
        
        Args:
            table_name: Table name
            batch_ids: Expected batch IDs
            source_file_paths: Source file paths
        
        Raises:
            DataConsistencyError: If batch mapping doesn't match
        """
        try:
            # Query metadata to verify paths map to expected batches
            paths_str = "', '".join(source_file_paths[:100])  # Check first 100 paths
            
            query = f"""
                SELECT DISTINCT batch_load_id
                FROM {constants.INGESTION_METADATA_TABLE}
                WHERE source_file_path IN ('{paths_str}')
                    AND table_name = '{table_name}'
            """
            
            result_df = self.spark.sql(query)
            actual_batches = [row.batch_load_id for row in result_df.collect()]
            
            # Verify all expected batches are present
            expected_set = set(batch_ids)
            actual_set = set(actual_batches)
            
            if expected_set != actual_set:
                error_msg = (
                    f"Batch verification failed! "
                    f"Expected batches: {expected_set}, "
                    f"Actual batches from paths: {actual_set}"
                )
                self.logger.error(error_msg)
                raise DataConsistencyError(error_msg)
            
            self.logger.info("Batch-to-path mapping verified")
            
        except DataConsistencyError:
            raise
        except Exception as e:
            self.logger.warning(
                f"Could not verify batch mapping (non-critical): {str(e)}"
            )
    
    def _extract_partitions_from_path(
        self, 
        file_path: str, 
        partition_columns: List[str]
    ) -> Optional[Tuple[str, ...]]:
        """
        Extract partition values from Hive-style partitioned file path
        
        Examples:
            Single partition:
                path: '/data/partition_date=2025-11-05/file.orc'
                columns: ['partition_date']
                returns: ('2025-11-05',)
            
            Multiple partitions:
                path: '/data/year=2025/month=11/day=05/file.orc'
                columns: ['year', 'month', 'day']
                returns: ('2025', '11', '05')
        
        Args:
            file_path: Full path to source file
            partition_columns: List of partition column names
        
        Returns:
            Tuple of partition values in order, or None if extraction fails
        """
        import re
        
        partition_values = []
        
        for part_col in partition_columns:
            pattern = f"{part_col}=([^/]+)"
            match = re.search(pattern, file_path)
            
            if match:
                partition_values.append(match.group(1))
            else:
                # Partition column not found in path - return None
                return None
        
        return tuple(partition_values)
    
    def get_latest_batch_per_partition(
        self, 
        config: TableConfig
    ) -> Dict[Tuple[str, ...], str]:
        """
        Get the latest batch for each partition combination
        
        This is used for partition_overwrite mode to identify which batches
        should be validated (only the latest per partition - Option A).
        
        Args:
            config: Table configuration with partition info
        
        Returns:
            Dict mapping partition_tuple -> latest_batch_id
            
            Examples:
                Single partition:
                    {('2025-11-05',): '202511070612',
                     ('2025-11-06',): '202511060606'}
                
                Multiple partitions:
                    {('2025', '11', '05'): '202511070612',
                     ('2025', '11', '06'): '202511060606'}
        
        Raises:
            BatchProcessingError: If query fails
        """
        if config.write_mode != "partition_overwrite":
            return {}
        
        if not config.is_partitioned:
            self.logger.warning(
                f"partition_overwrite mode but no partitions defined for {config.table_name}"
            )
            return {}
        
        self.logger.info(
            f"Building latest batch mapping for partitions: {config.partition_columns}"
        )
        
        try:
            # Query all completed batches with their file paths
            query = f"""
                SELECT 
                    t2.batch_load_id,
                    t1.source_file_path
                FROM {constants.INGESTION_METADATA_TABLE} t1
                INNER JOIN {constants.INGESTION_AUDIT_TABLE} t2
                    ON t1.table_name = t2.target_table_name 
                    AND t1.batch_load_id = t2.batch_load_id
                WHERE t2.status = 'COMPLETED'
                    AND t1.table_name = '{config.table_name}'
                ORDER BY t2.batch_load_id DESC
            """
            
            rows = self.spark.sql(query).collect()
            
            if not rows:
                self.logger.info("No completed batches found")
                return {}
            
            # Build mapping: partition_tuple -> latest_batch_id
            partition_to_latest_batch = {}
            batch_to_partitions = {}  # For logging
            
            for row in rows:
                batch_id = row.batch_load_id
                file_path = row.source_file_path
                
                # Extract partition values from file path
                partition_tuple = self._extract_partitions_from_path(
                    file_path,
                    config.partition_columns
                )
                
                if partition_tuple:
                    # Track which partitions this batch wrote to
                    if batch_id not in batch_to_partitions:
                        batch_to_partitions[batch_id] = set()
                    batch_to_partitions[batch_id].add(partition_tuple)
                    
                    # Update latest batch for this partition
                    if partition_tuple not in partition_to_latest_batch:
                        partition_to_latest_batch[partition_tuple] = batch_id
                    elif batch_id > partition_to_latest_batch[partition_tuple]:
                        partition_to_latest_batch[partition_tuple] = batch_id
            
            self.logger.info(
                f"Found {len(partition_to_latest_batch)} unique partition(s) "
                f"across {len(batch_to_partitions)} batch(es)"
            )
            
            return partition_to_latest_batch
            
        except Exception as e:
            error_msg = f"Failed to build partition mapping for {config.table_name}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            raise BatchProcessingError(error_msg)
    
    def _is_batch_latest_for_its_partitions(
        self,
        batch_id: str,
        batch_file_paths: List[str],
        latest_per_partition: Dict[Tuple[str, ...], str],
        config: TableConfig
    ) -> bool:
        """
        Check if this batch is the latest for ALL partitions it wrote to
        
        A batch should only be validated if it's the latest for ALL its partitions.
        If ANY partition was overwritten by a later batch, skip this batch.
        
        Args:
            batch_id: Batch ID to check
            batch_file_paths: List of file paths in this batch
            latest_per_partition: Mapping of partition -> latest batch
            config: Table configuration
        
        Returns:
            True if batch is latest for ALL its partitions, False otherwise
        """
        batch_partitions = set()
        
        # Extract all unique partitions this batch wrote to
        for file_path in batch_file_paths:
            partition_tuple = self._extract_partitions_from_path(
                file_path,
                config.partition_columns
            )
            if partition_tuple:
                batch_partitions.add(partition_tuple)
        
        if not batch_partitions:
            self.logger.warning(
                f"Could not extract partition values from batch {batch_id} file paths. "
                f"Including batch for validation (safe default)."
            )
            return True  # If we can't determine, include the batch (safe default)
        
        # Check if this batch is latest for ALL its partitions
        overwritten_partitions = []
        
        for partition_tuple in batch_partitions:
            latest_batch = latest_per_partition.get(partition_tuple)
            
            if latest_batch != batch_id:
                # This partition was overwritten by a later batch
                overwritten_partitions.append(
                    (partition_tuple, latest_batch)
                )
        
        if overwritten_partitions:
            self.logger.info(
                f"  ⏭️  Batch {batch_id} - SKIPPED (partitions overwritten):"
            )
            for part_tuple, latest_batch in overwritten_partitions[:3]:  # Show max 3
                self.logger.info(
                    f"      Partition {part_tuple} overwritten by {latest_batch}"
                )
            if len(overwritten_partitions) > 3:
                self.logger.info(
                    f"      ... and {len(overwritten_partitions) - 3} more partition(s)"
                )
            return False
        
        return True
    
    def get_unprocessed_batches_with_mapping(self, config: TableConfig) -> Dict[str, List[str]]:
        """
        Get batches that need validation with batch-to-path mapping
        
        For partition_overwrite mode: Applies Option A filtering (only latest batch per partition)
        For other modes: Returns all unprocessed batches
        
        This method is designed for batch-level auditing where each batch is validated separately.
        
        Args:
            config: Table configuration
        
        Returns:
            Dictionary mapping batch_id to list of ORC paths
            Example: {'batch1': ['path1.orc', 'path2.orc'], 'batch2': ['path3.orc']}
        
        Raises:
            BatchProcessingError: If batch query fails
        """
        self.logger.set_table_context(config.table_name)
        self.logger.info(f"Identifying unprocessed batches with path mapping")
        self.logger.info(f"  Write mode: {config.write_mode}")
        
        try:
            # Query to get ALL unprocessed batches (initial selection)
            query = f"""
                SELECT
                    t1.source_file_path,
                    t2.batch_load_id
                FROM {constants.INGESTION_METADATA_TABLE} t1
                INNER JOIN {constants.INGESTION_AUDIT_TABLE} t2
                    ON (t1.table_name = t2.target_table_name 
                        AND t1.batch_load_id = t2.batch_load_id)
                INNER JOIN {constants.INGESTION_CONFIG_TABLE} t4
                    ON t1.table_name = concat_ws('.', t4.target_catalog, t4.target_schema, t4.target_table)
                    AND t2.group_name = t4.group_name
                LEFT JOIN (
                    SELECT 
                        batch_load_id,
                        validation_run_status,
                        row_number() OVER(PARTITION BY batch_load_id ORDER BY validation_run_start_time DESC) as rnk
                    FROM {constants.VALIDATION_LOG_TABLE}
                ) t3
                    ON t3.batch_load_id = t2.batch_load_id AND t3.rnk = 1
                WHERE t2.status = 'COMPLETED'
                    AND t1.table_name = '{config.table_name}'
                    AND (
                        -- For overwrite mode: only latest batch
                        (t4.write_mode = 'overwrite' AND t2.batch_load_id = (
                            SELECT max(batch_load_id)
                            FROM {constants.INGESTION_AUDIT_TABLE}
                            WHERE status = 'COMPLETED'
                                AND target_table_name = '{config.table_name}'
                        ))
                        -- For append/partition_overwrite/merge modes: all batches
                        OR t4.write_mode IN ('append', 'partition_overwrite', 'merge')
                    )
                    AND (
                        -- Batch not validated OR last validation was not SUCCESS
                        (t3.validation_run_status <> 'SUCCESS' AND t3.rnk = 1)
                        OR t3.batch_load_id IS NULL
                    )
            """
            
            result_df = self.spark.sql(query)
            rows = result_df.collect()
            
            if not rows:
                self.logger.info("No unprocessed batches found")
                self.logger.clear_table_context()
                return {}
            
            # Build batch-to-path mapping
            batch_mapping = {}
            for row in rows:
                batch_id = row.batch_load_id
                orc_path = row.source_file_path
                
                if batch_id not in batch_mapping:
                    batch_mapping[batch_id] = []
                batch_mapping[batch_id].append(orc_path)
            
            self.logger.info(
                f"Found {len(batch_mapping)} unprocessed batch(es): {list(batch_mapping.keys())}"
            )
            
            # OPTION A: For partition_overwrite, filter to only latest per partition
            if config.write_mode == "partition_overwrite" and config.is_partitioned:
                self.logger.info("Applying Option A filter: keeping only latest batch per partition")
                
                # Get latest batch for each partition
                latest_per_partition = self.get_latest_batch_per_partition(config)
                
                if not latest_per_partition:
                    self.logger.warning(
                        "Could not determine latest batches per partition. "
                        "Proceeding with all batches (may have false failures)."
                    )
                else:
                    # Filter batches: keep only those that are latest for their partitions
                    filtered_mapping = {}
                    skipped_count = 0
                    
                    for batch_id, file_paths in batch_mapping.items():
                        if self._is_batch_latest_for_its_partitions(
                            batch_id, 
                            file_paths, 
                            latest_per_partition, 
                            config
                        ):
                            filtered_mapping[batch_id] = file_paths
                            self.logger.info(f" {batch_id} - latest for its partition(s)")
                        else:
                            skipped_count += 1
                    
                    self.logger.info(
                        f"Option A filtering: {len(filtered_mapping)} batch(es) kept, "
                        f"{skipped_count} batch(es) skipped"
                    )
                    
                    batch_mapping = filtered_mapping
            
            # Log final batch counts
            for batch_id, paths in batch_mapping.items():
                self.logger.info(f"  {batch_id}: {len(paths)} file(s)")
            
            self.logger.clear_table_context()
            return batch_mapping
            
        except Exception as e:
            self.logger.clear_table_context()
            error_msg = f"Failed to get unprocessed batches with mapping for {config.table_name}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            raise BatchProcessingError(error_msg)

