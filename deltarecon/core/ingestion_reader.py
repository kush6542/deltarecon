"""
Ingestion metadata reader

Reads configuration from:
- validation_mapping (validation configs)
- ingestion_config (write_mode, partitions, source file format and options)
"""

import json
from typing import List
from pyspark.sql import SparkSession

from deltarecon.config import constants
from deltarecon.models.table_config import TableConfig
from deltarecon.utils.exceptions import ConfigurationError
from deltarecon.utils.logger import get_logger

logger = get_logger(__name__)


class IngestionConfigReader:
    """
    Reads ingestion metadata and validation mapping configurations
    
    Combines data from:
    - validation_mapping: validation-specific configs
    - ingestion_config: source system configs
    """
    
    def __init__(self, spark: SparkSession, table_group: str):
        """
        Initialize reader
        
        Args:
            spark: SparkSession
            table_group: Table group to read configs for
        """
        self.spark = spark
        self.table_group = table_group
        self.logger = logger
    
    def get_tables_in_group(self) -> List[TableConfig]:
        """
        Get all active tables in this table_group
        
        Returns:
            List of TableConfig objects
        
        Raises:
            ConfigurationError: If no tables found or query fails
        """
        self.logger.info(f"Reading table configurations for group: {self.table_group}")
        
        try:
            # Query validation_mapping joined with ingestion_config
            query = f"""
                SELECT 
                    vm.table_group,
                    vm.table_family,
                    vm.src_table,
                    vm.tgt_table,
                    vm.tgt_primary_keys,
                    vm.mismatch_exclude_fields,
                    vm.validation_is_active,
                    ic.write_mode,
                    ic.partition_column,
                    ic.source_file_format,
                    ic.source_file_options
                FROM {constants.VALIDATION_MAPPING_TABLE} vm
                INNER JOIN {constants.INGESTION_CONFIG_TABLE} ic
                    ON vm.tgt_table = concat_ws('.', ic.target_catalog, ic.target_schema, ic.target_table)
                WHERE vm.table_group = '{self.table_group}'
                    AND vm.validation_is_active = true
            """
            
            result_df = self.spark.sql(query)
            rows = result_df.collect()
            
            if not rows:
                raise ConfigurationError(
                    f"No active tables found for table_group: {self.table_group}"
                )
            
            self.logger.info(f"Found {len(rows)} active tables in group '{self.table_group}'")
            
            # Convert to TableConfig objects
            table_configs = []
            for row in rows:
                config = self._row_to_table_config(row)
                table_configs.append(config)
                self.logger.info(f"  - {config.table_name}")
            
            return table_configs
            
        except Exception as e:
            error_msg = f"Failed to read table configurations for group '{self.table_group}': {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            raise ConfigurationError(error_msg)
    
    def _row_to_table_config(self, row) -> TableConfig:
        """
        Convert SQL row to TableConfig object
        
        Args:
            row: Row from validation_mapping + ingestion_config query
        
        Returns:
            TableConfig object
        """
        # Parse primary keys (pipe-separated)
        primary_keys = []
        if row.tgt_primary_keys:
            primary_keys = [pk.strip() for pk in row.tgt_primary_keys.split('|') if pk.strip()]
        
        # Parse partition columns (comma-separated)
        partition_columns = []
        if row.partition_column:
            partition_columns = [pc.strip() for pc in row.partition_column.split(',') if pc.strip()]
        
        # Parse exclude fields
        exclude_fields = []
        if row.mismatch_exclude_fields:
            exclude_fields = [f.strip() for f in row.mismatch_exclude_fields.split(',') if f.strip()]
        
        # Parse source file format (REQUIRED - no default)
        if not row.source_file_format:
            raise ConfigurationError(
                f"source_file_format is missing for table {row.tgt_table}. "
                f"This is a required field. Please specify 'orc', 'csv', or 'text'."
            )
        source_file_format = row.source_file_format.lower().strip()
        
        # Parse source file options (JSON string to dict)
        source_file_options = {}
        if row.source_file_options:
            try:
                source_file_options = json.loads(row.source_file_options)
                self.logger.info(f"  Parsed source_file_options for {row.tgt_table}: {source_file_options}")
            except json.JSONDecodeError as e:
                self.logger.warning(
                    f"Failed to parse source_file_options for {row.tgt_table}: {e}. "
                    f"Will use empty options."
                )
                source_file_options = {}
        
        # Get partition datatypes if partitions exist
        partition_datatypes = {}
        if partition_columns:
            partition_datatypes = self._get_partition_datatypes(
                row.src_table, 
                partition_columns
            )
        
        return TableConfig(
            table_group=row.table_group,
            table_family=row.table_family,
            table_name=row.tgt_table,
            source_table=row.src_table,
            write_mode=row.write_mode,
            source_file_format=source_file_format,
            source_file_options=source_file_options,
            primary_keys=primary_keys,
            partition_columns=partition_columns,
            partition_datatypes=partition_datatypes,
            mismatch_exclude_fields=exclude_fields,
            is_active=row.validation_is_active
        )
    
    def _get_partition_datatypes(self, source_table: str, partition_columns: List[str]) -> dict:
        """
        Get partition column datatypes from source_table_partition_mapping
        
        Args:
            source_table: Source table name (e.g., 'source_system.schema.table')
            partition_columns: List of partition column names
        
        Returns:
            Dict mapping column name to datatype
        """
        try:
            # Clean source table name (remove source_system. prefix if present)
            clean_table = source_table.replace('source_system.', '')
            
            query = f"""
                SELECT 
                    partition_column_name,
                    datatype
                FROM {constants.INGESTION_SRC_TABLE_PARTITION_MAPPING}
                WHERE concat_ws('.', schema_name, table_name) = '{clean_table}'
                ORDER BY index
            """
            
            result_df = self.spark.sql(query)
            rows = result_df.collect()
            
            partition_datatypes = {row.partition_column_name: row.datatype for row in rows}
            
            # Verify all partition columns have datatypes
            missing = set(partition_columns) - set(partition_datatypes.keys())
            if missing:
                self.logger.warning(
                    f"Partition datatypes not found for columns: {missing}. "
                    f"Will use 'string' as default."
                )
                for col in missing:
                    partition_datatypes[col] = 'string'
            
            return partition_datatypes
            
        except Exception as e:
            self.logger.warning(
                f"Failed to get partition datatypes for {source_table}: {str(e)}. "
                f"Using 'string' as default for all partitions."
            )
            return {col: 'string' for col in partition_columns}

