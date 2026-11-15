"""Pydantic v2 domain models matching api/proto/v1/types.proto and coordinator.proto."""

from __future__ import annotations

from enum import IntEnum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class MultimodalModality(IntEnum):
    """Enumeration of multimodal payload modalities."""

    UNSPECIFIED = 0
    DOCUMENT_PDF = 1
    AUDIO = 2
    VIDEO = 3
    TABULAR = 4


class PipelineConfig(BaseModel):
    """Tenant-scoped chunking and extraction configuration."""

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(default="default-multimodal-v1")
    tenant_id: str = Field(default="")
    name: str = Field(default="")
    chunk_size_tokens: int = Field(default=300)
    chunk_overlap_tokens: int = Field(default=50)
    ocr_enabled: bool = Field(default=True)


class PartitionSlice(BaseModel):
    """Deterministic partition slice assigned to a child workflow DAG."""

    model_config = ConfigDict(populate_by_name=True)

    slice_id: str
    slice_index: int
    total_slices: int
    tenant_id: str
    document_id: str
    content_hash: str
    s3_input_bucket: str
    s3_input_key: str
    s3_output_parquet_key: str
    modality: MultimodalModality = MultimodalModality.DOCUMENT_PDF
    start_page: int = Field(default=0)
    end_page: int = Field(default=0)
    start_time_seconds: float = Field(default=0.0)
    end_time_seconds: float = Field(default=0.0)
    overlap_seconds: float = Field(default=0.0)
    pipeline_config_id: Optional[str] = Field(default=None)
    pipeline_config: Optional[PipelineConfig] = Field(default=None)


class HeartbeatDetails(BaseModel):
    """Native Temporal activity heartbeat checkpoint payload (ADR-003)."""

    model_config = ConfigDict(populate_by_name=True)

    slice_id: str
    slice_index: int
    last_processed_unit: int = Field(default=0)
    processed_units: int = Field(default=0)
    total_units: int = Field(default=0)
    intermediate_s3_key: str = Field(default="")
    updated_at_unix: int = Field(default=0)


class ExtractSegmentRequest(BaseModel):
    """Polyglot activity input contract for doc-ai-task-queue."""

    model_config = ConfigDict(populate_by_name=True)

    slice: PartitionSlice
    presigned_input_get_url: str = Field(default="")
    presigned_output_put_url: str = Field(default="")
    pipeline_config: PipelineConfig = Field(default_factory=PipelineConfig)
    resume_checkpoint: Optional[HeartbeatDetails] = Field(default=None)


class ExtractSegmentResponse(BaseModel):
    """Polyglot activity output contract returned to Go child workflow."""

    model_config = ConfigDict(populate_by_name=True)

    slice_id: str
    slice_index: int
    s3_output_bucket: str
    s3_output_parquet_key: str
    extracted_pages: int = Field(default=0)
    extracted_segments: int = Field(default=0)
    parquet_bytes: int = Field(default=0)

