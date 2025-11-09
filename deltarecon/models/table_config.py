"""
Data models for table configuration

TableConfig holds all configuration needed to validate a single table.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict

from deltarecon.utils.exceptions import ConfigurationError


@dataclass
class TableConfig:
    """
    Configuration for a single table validation
    
    This class holds all information needed to:
    - Identify the table
    - Read from correct sources
    - Apply correct validation logic
    """
    
    # Identity
    table_group: str                    # e.g., "sales"
    table_family: str                   # e.g., "sales_orders"
    table_name: str                     # e.g., "catalog.schema.sales_orders"
    source_table: str                   # e.g., "source_system.schema.sales_orders"
    
    # Processing mode
    write_mode: str                     # "append", "overwrite", "partition_overwrite", or "merge"
    
    # Source file configuration
    source_file_format: str             # REQUIRED: "orc", "csv", "text", etc.
    source_file_options: Optional[Dict] = field(default_factory=dict)  # Format-specific options
    
    # Validation configuration
    primary_keys: Optional[List[str]] = field(default_factory=list)
    partition_columns: Optional[List[str]] = field(default_factory=list)
    partition_datatypes: Optional[Dict[str, str]] = field(default_factory=dict)
    mismatch_exclude_fields: Optional[List[str]] = field(default_factory=list)
    
    # Control flags
    is_active: bool = True
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        self.validate()
    
    def validate(self):
        """
        Validate configuration values
        
        Raises:
            ConfigurationError: If configuration is invalid
        """
        if not self.table_group:
            raise ConfigurationError("table_group cannot be empty")
        
        if not self.table_name:
            raise ConfigurationError("table_name cannot be empty")
        
        if not self.source_table:
            raise ConfigurationError("source_table cannot be empty")
        
        if self.write_mode not in ["append", "overwrite", "partition_overwrite", "merge"]:
            raise ConfigurationError(
                f"Invalid write_mode: {self.write_mode}. "
                f"Must be 'append', 'overwrite', 'partition_overwrite', or 'merge'"
            )
        
        if not self.source_file_format:
            raise ConfigurationError("source_file_format cannot be empty")
        
        if self.partition_columns and not isinstance(self.partition_columns, list):
            raise ConfigurationError("partition_columns must be a list")
        
        if self.primary_keys and not isinstance(self.primary_keys, list):
            raise ConfigurationError("primary_keys must be a list")
    
    @property
    def has_primary_keys(self) -> bool:
        """Check if primary keys are defined"""
        return bool(self.primary_keys and len(self.primary_keys) > 0)
    
    @property
    def is_partitioned(self) -> bool:
        """Check if table is partitioned"""
        return bool(self.partition_columns and len(self.partition_columns) > 0)
    
    def __repr__(self) -> str:
        """String representation for logging"""
        return (
            f"TableConfig(table_group='{self.table_group}', "
            f"table_name='{self.table_name}', "
            f"write_mode='{self.write_mode}', "
            f"has_pk={self.has_primary_keys}, "
            f"is_partitioned={self.is_partitioned})"
        )

