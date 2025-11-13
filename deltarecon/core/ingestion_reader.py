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
        
        PARTITION COLUMN PRIORITIZATION (mirrors ingestion framework):
        1. First, try to get partitions from source_table_partition_mapping (authoritative)
        2. If not found, fall back to serving_ingestion_config.partition_column
        3. If mismatch detected, log warning and use source_table_partition_mapping
        
        Args:
            row: Row from validation_mapping + ingestion_config query
        
        Returns:
            TableConfig object
        """
        # Parse primary keys (pipe-separated)
        primary_keys = []
        if row.tgt_primary_keys:
            primary_keys = [pk.strip() for pk in row.tgt_primary_keys.split('|') if pk.strip()]
        
        # Parse partition columns from config (for fallback)
        config_partition_columns = []
        if row.partition_column:
            config_partition_columns = [pc.strip() for pc in row.partition_column.split(',') if pc.strip()]
        
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
        
        # ============================================================================
        # PARTITION COLUMN RESOLUTION (NEW LOGIC - mirrors ingestion framework)
        # ============================================================================
        
        # Step 1: Try to get partition columns from source_table_partition_mapping (authoritative)
        actual_partition_columns, partition_datatypes = self._get_partition_columns_and_datatypes(
            row.src_table
        )
        
        # Step 2: Decide which partition columns to use
        if actual_partition_columns:
            # Mapping has data → use it (prioritize mapping over config)
            partition_columns = actual_partition_columns
            
            # Check for mismatch with config
            if config_partition_columns:
                if set(actual_partition_columns) != set(config_partition_columns):
                    missing_in_config = set(actual_partition_columns) - set(config_partition_columns)
                    extra_in_config = set(config_partition_columns) - set(actual_partition_columns)
                    
                    self.logger.warning(
                        f"PARTITION CONFIGURATION MISMATCH for {row.tgt_table}!"
                    )
                    self.logger.warning(f"   Config (serving_ingestion_config): {config_partition_columns}")
                    self.logger.warning(f"   Actual (source_table_partition_mapping): {actual_partition_columns}")
                    if missing_in_config:
                        self.logger.warning(f" Missing in config: {list(missing_in_config)}")
                    if extra_in_config:
                        self.logger.warning(f" Extra in config: {list(extra_in_config)}")
                    self.logger.warning(
                        f" Using actual partitions from source_table_partition_mapping."
                    )
                    self.logger.warning(
                        f" Recommendation: Update serving_ingestion_config.partition_column "
                        f"to match: {','.join(actual_partition_columns)}"
                    )
            else:
                # Config had no partitions, but mapping does
                self.logger.info(
                    f"✓ No partitions in config, but found in source_table_partition_mapping: "
                    f"{actual_partition_columns}"
                )
        else:
            # Mapping empty → fallback to config (same behavior as ingestion framework)
            partition_columns = config_partition_columns
            
            if partition_columns:
                self.logger.info(
                    f"Using partition columns from serving_ingestion_config for {row.tgt_table}: "
                    f"{partition_columns} (not found in source_table_partition_mapping)"
                )
                # Get datatypes separately using old method
                partition_datatypes = self._get_partition_datatypes(
                    row.src_table, 
                    partition_columns
                )
            else:
                # No partitions in either config or mapping
                self.logger.debug(f"No partition columns configured for {row.tgt_table}")
                partition_datatypes = {}
        
        # Step 3: Validate for partition_overwrite mode
        if row.write_mode == "partition_overwrite" and not partition_columns:
            error_msg = (
                f"partition_overwrite mode requires partition_columns for {row.tgt_table}. "
                f"Not found in source_table_partition_mapping OR serving_ingestion_config.partition_column. "
                f"Either add partition columns to configuration or change write_mode."
            )
            self.logger.error(error_msg)
            raise ConfigurationError(error_msg)
        
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
    
    def _get_partition_columns_and_datatypes(self, source_table: str) -> tuple:
        """
        Get partition columns AND datatypes from source_table_partition_mapping
        
        Mirrors ingestion framework behavior: automigrate/common/config.py::get_partition_col_datatypes()
        
        This method queries the source_table_partition_mapping table (Hive metadata) to get
        the authoritative list of partition columns and their datatypes.
        
        Args:
            source_table: Source table name (e.g., 'connectivity_home.table_name')
        
        Returns:
            Tuple of (partition_columns_list, partition_datatypes_dict)
            - partition_columns_list: List of partition column names in index order
            - partition_datatypes_dict: Dict mapping column name to datatype
            
            Returns ([], {}) if:
            - Table not found in mapping
            - Query returns no rows
            - Query fails (graceful fallback)
        
        Examples:
            >>> _get_partition_columns_and_datatypes('connectivity_home.my_table')
            (['partition_date', 'd_customer_product_type'], 
             {'partition_date': 'date', 'd_customer_product_type': 'string'})
            
            >>> _get_partition_columns_and_datatypes('schema.table_not_in_mapping')
            ([], {})
        """
        try:
            # Parse source table name
            clean_table = source_table.replace('source_system.', '')
            parts = clean_table.split('.')
            
            if len(parts) != 2:
                self.logger.warning(
                    f"Unexpected source table format: {source_table}. "
                    f"Expected 'schema.table', got {len(parts)} parts."
                )
                return [], {}
            
            schema_name, table_name = parts
            
            # Query source_table_partition_mapping (same as ingestion framework)
            query = f"""
                SELECT DISTINCT 
                    partition_column_name, 
                    derived_datatype,
                    index
                FROM {constants.INGESTION_SRC_TABLE_PARTITION_MAPPING}
                WHERE LOWER(TRIM(schema_name)) = LOWER(TRIM('{schema_name}'))
                  AND LOWER(TRIM(table_name)) = LOWER(TRIM('{table_name}'))
                ORDER BY index ASC
            """
            
            self.logger.debug(f"Querying source_table_partition_mapping for: {schema_name}.{table_name}")
            
            result_df = self.spark.sql(query)
            rows = result_df.collect()
            
            if not rows:
                # Empty result - table not in mapping (this is OK, not an error)
                self.logger.debug(
                    f"No partition data found in source_table_partition_mapping for {source_table}. "
                    f"Will use partition_column from serving_ingestion_config."
                )
                return [], {}
            
            # Build both list and dict (maintains order via index column)
            partition_columns = []
            partition_datatypes = {}
            
            for row in rows:
                col_name = row.partition_column_name
                datatype = row.derived_datatype
                partition_columns.append(col_name)
                partition_datatypes[col_name] = datatype
            
            self.logger.info(
                f"✓ Loaded {len(partition_columns)} partition columns from source_table_partition_mapping "
                f"for {source_table}: {partition_columns}"
            )
            
            return partition_columns, partition_datatypes
            
        except Exception as e:
            # Query error - log and fall back gracefully (same as ingestion framework)
            self.logger.error(
                f"ERROR querying source_table_partition_mapping for {source_table}: {str(e)}. "
                f"Will fallback to serving_ingestion_config.partition_column"
            )
            self.logger.debug("Exception details:", exc_info=True)
            return [], {}

