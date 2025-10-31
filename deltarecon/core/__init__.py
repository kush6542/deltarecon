"""Core module - exports core components (for internal use)"""

from deltarecon.core.ingestion_reader import IngestionConfigReader
from deltarecon.core.batch_processor import BatchProcessor
from deltarecon.core.source_target_loader import SourceTargetLoader
from deltarecon.core.validation_engine import ValidationEngine
from deltarecon.core.metadata_writer import MetadataWriter

__all__ = [
    "IngestionConfigReader",
    "BatchProcessor",
    "SourceTargetLoader",
    "ValidationEngine",
    "MetadataWriter",
]

