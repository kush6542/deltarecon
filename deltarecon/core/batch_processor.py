"""
Batch processor - Identifies which batches need validation

Logic:
- For append mode: Process all unvalidated COMPLETED batches
- For partition_overwrite mode: Process all unvalidated COMPLETED batches
- For overwrite mode: Process only latest COMPLETED batch
- Skip batches that are already validated successfully
- Supports batch-level auditing: returns batch-to-path mapping

Note: Loaded via %run - constants, TableConfig, exceptions, logger available in namespace
"""

from typing import List, Tuple, Dict
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
            Tuple of (batch_ids, orc_paths)
        
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
                        -- For append/partition_overwrite modes: all batches
                        OR t4.write_mode <> 'overwrite'
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
            
            # Extract batch IDs and ORC paths
            batch_ids = list(set([row.batch_load_id for row in rows]))
            orc_paths = [row.source_file_path for row in rows]
            
            self.logger.info(f"Found {len(batch_ids)} unprocessed batch(es): {batch_ids}")
            self.logger.info(f"Total ORC files: {len(orc_paths)}")
            
            # Verify batch-to-path mapping
            self._verify_batch_mapping(config.table_name, batch_ids, orc_paths)
            
            self.logger.clear_table_context()
            return batch_ids, orc_paths
            
        except Exception as e:
            self.logger.clear_table_context()
            error_msg = f"Failed to get unprocessed batches for {config.table_name}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            raise BatchProcessingError(error_msg)
    
    def _verify_batch_mapping(self, table_name: str, batch_ids: List[str], orc_paths: List[str]):
        """
        Verify that ORC paths actually map to the expected batches
        
        This is a CRITICAL check to ensure we're validating the right data.
        
        Args:
            table_name: Table name
            batch_ids: Expected batch IDs
            orc_paths: ORC file paths
        
        Raises:
            DataConsistencyError: If batch mapping doesn't match
        """
        try:
            # Query metadata to verify paths map to expected batches
            paths_str = "', '".join(orc_paths[:100])  # Check first 100 paths
            
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
                    f"Actual batches from ORC paths: {actual_set}"
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
    
    def get_unprocessed_batches_with_mapping(self, config: TableConfig) -> Dict[str, List[str]]:
        """
        Get batches that need validation with batch-to-path mapping
        
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
            # Same query as get_unprocessed_batches but we preserve the relationship
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
                        -- For append/partition_overwrite modes: all batches
                        OR t4.write_mode <> 'overwrite'
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
            
            self.logger.info(f"Found {len(batch_mapping)} unprocessed batch(es): {list(batch_mapping.keys())}")
            for batch_id, paths in batch_mapping.items():
                self.logger.info(f"  {batch_id}: {len(paths)} ORC file(s)")
            
            self.logger.clear_table_context()
            return batch_mapping
            
        except Exception as e:
            self.logger.clear_table_context()
            error_msg = f"Failed to get unprocessed batches with mapping for {config.table_name}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            raise BatchProcessingError(error_msg)

