"""RAG Document AI Worker package.

Temporal Python SDK worker for multimodal OCR, Docling, and segment extraction
on doc-ai-task-queue with zero dual-write heartbeat checkpoints (ADR-001, ADR-003).
"""

from .activity import DispatchDocAIExtractActivity, build_parquet_slice
from .models import (
    ExtractSegmentRequest,
    ExtractSegmentResponse,
    HeartbeatDetails,
    MultimodalModality,
    PartitionSlice,
    PipelineConfig,
)

__all__ = [
    "DispatchDocAIExtractActivity",
    "build_parquet_slice",
    "ExtractSegmentRequest",
    "ExtractSegmentResponse",
    "HeartbeatDetails",
    "MultimodalModality",
    "PartitionSlice",
    "PipelineConfig",
]
