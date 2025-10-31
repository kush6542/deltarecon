"""
Data models for validation results

Contains:
- ValidatorResult: Result from a single validator
- ValidationResult: Aggregated result from all validators
- ValidationLogRecord: Record for validation_log table
- ValidationSummaryRecord: Record for validation_summary table
"""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from datetime import datetime


@dataclass
class ValidatorResult:
    """
    Result from a single validator
    
    Each validator (row_count, schema, pk) returns this structure.
    """
    
    status: str  # PASSED, FAILED, SKIPPED, ERROR
    metrics: Dict[str, Any]
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """
    Aggregated result from all validators for a single table
    
    This is the complete validation result after running all validators.
    """
    
    table_name: str
    table_family: str
    batch_load_ids: List[str]
    iteration_name: str
    overall_status: str  # SUCCESS if all PASSED, else FAILED
    validator_results: Dict[str, ValidatorResult]  # {validator_name: result}
    start_time: datetime
    end_time: datetime
    spot_check_result: Optional[Dict[str, Any]] = None
    
    @property
    def duration_seconds(self) -> float:
        """Calculate validation duration in seconds"""
        return (self.end_time - self.start_time).total_seconds()


@dataclass
class ValidationLogRecord:
    """
    Record for validation_log table
    
    Tracks each validation run at table level.
    """
    
    iteration_name: str
    workflow_name: str
    table_family: str
    src_table: str  # Source table name
    tgt_table: str  # Target table name
    batch_load_ids: List[str]
    status: str  # IN_PROGRESS, SUCCESS, FAILED
    start_time: datetime
    end_time: Optional[datetime] = None
    exception: Optional[str] = None


@dataclass
class ValidationSummaryRecord:
    """
    Record for validation_summary table
    
    Contains detailed metrics and status for each validation check.
    """
    
    iteration_name: str
    workflow_name: str
    table_family: str
    src_table: str
    tgt_table: str
    batch_load_ids: List[str]
    
    # Status fields (one per check type)
    row_count_match_status: str
    schema_match_status: str
    primary_key_compliance_status: str
    col_name_compare_status: str
    data_type_compare_status: str
    overall_status: str
    
    # Metrics
    src_records: int
    tgt_records: int
    src_extras: Optional[int] = None
    tgt_extras: Optional[int] = None
    mismatches: Optional[int] = None
    matches: Optional[int] = None
    
    # Additional metadata
    spot_check_sample_size: Optional[int] = None
    spot_check_mismatches: Optional[int] = None
    framework_version: str = "v1.0.0"
    
    @classmethod
    def from_validation_result(
        cls,
        validation_result: ValidationResult,
        table_config,
        workflow_name: str
    ) -> 'ValidationSummaryRecord':
        """
        Create ValidationSummaryRecord from ValidationResult
        
        This method extracts metrics from validator results and formats them
        for the validation_summary table.
        """
        # Extract row count metrics
        row_count_result = validation_result.validator_results.get('row_count')
        src_records = row_count_result.metrics.get('src_records', 0) if row_count_result else 0
        tgt_records = row_count_result.metrics.get('tgt_records', 0) if row_count_result else 0
        row_count_status = row_count_result.status if row_count_result else 'SKIPPED'
        
        # Extract schema metrics
        schema_result = validation_result.validator_results.get('schema')
        col_name_status = schema_result.metrics.get('col_name_status', 'SKIPPED') if schema_result else 'SKIPPED'
        data_type_status = schema_result.metrics.get('data_type_status', 'SKIPPED') if schema_result else 'SKIPPED'
        schema_status = schema_result.status if schema_result else 'SKIPPED'
        
        # Extract PK metrics
        pk_result = validation_result.validator_results.get('pk_validation')
        pk_status = pk_result.status if pk_result else 'SKIPPED'
        
        return cls(
            iteration_name=validation_result.iteration_name,
            workflow_name=workflow_name,
            table_family=validation_result.table_family,
            src_table=table_config.source_table,
            tgt_table=table_config.table_name,
            batch_load_ids=validation_result.batch_load_ids,
            row_count_match_status=row_count_status,
            schema_match_status=schema_status,
            primary_key_compliance_status=pk_status,
            col_name_compare_status=col_name_status,
            data_type_compare_status=data_type_status,
            overall_status=validation_result.overall_status,
            src_records=src_records,
            tgt_records=tgt_records,
            spot_check_sample_size=validation_result.spot_check_result.get('sample_size') if validation_result.spot_check_result else None,
            spot_check_mismatches=validation_result.spot_check_result.get('mismatches') if validation_result.spot_check_result else None
        )

