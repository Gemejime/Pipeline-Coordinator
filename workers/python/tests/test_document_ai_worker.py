"""Unit tests for RAG Document AI Worker: models, heartbeat recovery, and Parquet generation."""

from __future__ import annotations

import dataclasses
import io
import json
import unittest
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
from temporalio.testing import ActivityEnvironment

from workers.python.rag_document_ai_worker.activity import (
    PARQUET_SCHEMA,
    DispatchDocAIExtractActivity,
    build_parquet_slice,
)
from workers.python.rag_document_ai_worker.models import (
    ExtractSegmentRequest,
    ExtractSegmentResponse,
    HeartbeatDetails,
    MultimodalModality,
    PartitionSlice,
    PipelineConfig,
)


class TestDocumentAIModels(unittest.TestCase):
    """Verifies Pydantic v2 schemas and validation contracts."""

    def test_pipeline_config_defaults(self):
        """Verifies default values and field assignment for PipelineConfig."""
        cfg = PipelineConfig(tenant_id="tenant-corp-01")
        self.assertEqual(cfg.id, "default-multimodal-v1")
        self.assertEqual(cfg.chunk_size_tokens, 300)
        self.assertEqual(cfg.chunk_overlap_tokens, 50)
        self.assertTrue(cfg.ocr_enabled)

    def test_partition_slice_validation(self):
        """Verifies PartitionSlice field parsing and modality mapping."""
        data = {
            "slice_id": "slice-001",
            "slice_index": 1,
            "total_slices": 5,
            "tenant_id": "tenant-corp-01",
            "document_id": "doc-01",
            "content_hash": "a" * 64,
            "s3_input_bucket": "rag-ingestion-bucket",
            "s3_input_key": "raw/tenant-corp-01/hash/doc.pdf",
            "s3_output_parquet_key": "artifacts/tenant-corp-01/hash/slice_1_50.parquet",
            "modality": 1,
            "start_page": 1,
            "end_page": 50,
        }
        slice_obj = PartitionSlice.model_validate(data)
        self.assertEqual(slice_obj.slice_id, "slice-001")
        self.assertEqual(slice_obj.modality, MultimodalModality.DOCUMENT_PDF)
        self.assertEqual(slice_obj.start_page, 1)
        self.assertEqual(slice_obj.end_page, 50)

    def test_heartbeat_details_roundtrip(self):
        """Verifies serializing and deserializing HeartbeatDetails preserves fields."""
        hb = HeartbeatDetails(
            slice_id="slice-001",
            slice_index=1,
            last_processed_unit=25,
            processed_units=25,
            total_units=50,
            intermediate_s3_key="intermediate/tenant/hash/unit_25.tmp",
            updated_at_unix=1794039600,
        )
        payload = hb.model_dump()
        restored = HeartbeatDetails.model_validate(payload)
        self.assertEqual(restored.last_processed_unit, 25)
        self.assertEqual(restored.total_units, 50)


class TestParquetGeneration(unittest.TestCase):
    """Verifies PyArrow Parquet table construction and column schema."""

    def test_build_parquet_slice(self):
        """Verifies PyArrow Parquet table construction and column schema."""
        slice_obj = PartitionSlice(
            slice_id="slice-test-01",
            slice_index=1,
            total_slices=2,
            tenant_id="tenant-01",
            document_id="doc-01",
            content_hash="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            s3_input_bucket="rag-bucket",
            s3_input_key="raw/tenant-01/hash/file.pdf",
            s3_output_parquet_key="artifacts/tenant-01/hash/slice_1_10.parquet",
            start_page=1,
            end_page=10,
        )

        units = [
            {
                "unit_index": 1,
                "content": "Page 1 sample content extracted by OCR",
                "metadata": {"page": 1, "has_images": True},
                "tokens_estimate": 45,
            },
            {
                "unit_index": 2,
                "content": "Page 2 sample content extracted by OCR",
                "metadata": {"page": 2, "has_images": False},
                "tokens_estimate": 42,
            },
        ]

        buf = build_parquet_slice(slice_obj, units)
        self.assertGreater(len(buf), 0)

        # Verify Parquet table deserialization
        reader = pq.read_table(io.BytesIO(buf))
        self.assertEqual(reader.num_rows, 2)
        self.assertEqual(len(reader.column_names), 6)
        self.assertIn("slice_id", reader.column_names)
        self.assertIn("tokens_estimate", reader.column_names)

        # Verify column schema types
        self.assertEqual(reader.schema.field("slice_id").type, PARQUET_SCHEMA.field("slice_id").type)
        self.assertEqual(reader.schema.field("tokens_estimate").type, PARQUET_SCHEMA.field("tokens_estimate").type)

        # Verify content
        rows = reader.to_pylist()
        self.assertEqual(rows[0]["slice_id"], "slice-test-01")
        self.assertEqual(rows[0]["unit_index"], 1)
        self.assertEqual(rows[1]["unit_index"], 2)


class TestDispatchDocAIExtractActivity(unittest.IsolatedAsyncioTestCase):
    """Verifies DispatchDocAIExtractActivity execution and heartbeat resumption."""

    async def test_activity_fresh_execution(self):
        """Verifies activity execution from unit 1 when no heartbeat checkpoint exists."""
        env = ActivityEnvironment()
        heartbeats = []
        env.on_heartbeat = lambda *args: heartbeats.append(args)

        req = ExtractSegmentRequest(
            slice=PartitionSlice(
                slice_id="slice-fresh-01",
                slice_index=1,
                total_slices=1,
                tenant_id="tenant-corp-01",
                document_id="doc-01",
                content_hash="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                s3_input_bucket="rag-bucket",
                s3_input_key="raw/tenant-corp-01/hash/file.pdf",
                s3_output_parquet_key="artifacts/tenant-corp-01/hash/slice_1_3.parquet",
                start_page=1,
                end_page=3,
            ),
            presigned_input_get_url="mock://input-url",
            presigned_output_put_url="mock://output-url",
        )

        resp_dict = await env.run(DispatchDocAIExtractActivity, req.model_dump())
        resp = ExtractSegmentResponse.model_validate(resp_dict)

        self.assertEqual(resp.slice_id, "slice-fresh-01")
        self.assertEqual(resp.extracted_pages, 3)
        self.assertEqual(resp.extracted_segments, 3)
        self.assertGreater(resp.parquet_bytes, 0)
        self.assertGreaterEqual(len(heartbeats), 3)

    async def test_activity_heartbeat_checkpoint_resumption(self):
        """Verifies activity resumes from last_processed_unit + 1 (ADR-003)."""
        env = ActivityEnvironment()
        heartbeats = []
        env.on_heartbeat = lambda *args: heartbeats.append(args)

        # Simulate existing checkpoint at unit 2 of 5
        checkpoint = HeartbeatDetails(
            slice_id="slice-resume-01",
            slice_index=1,
            last_processed_unit=2,
            processed_units=2,
            total_units=5,
            intermediate_s3_key="intermediate/tenant/hash/unit_2.tmp",
            updated_at_unix=1794039600,
        )
        env.info = dataclasses.replace(
            env.info,
            heartbeat_details=[checkpoint.model_dump()],
        )

        req = ExtractSegmentRequest(
            slice=PartitionSlice(
                slice_id="slice-resume-01",
                slice_index=1,
                total_slices=1,
                tenant_id="tenant-corp-01",
                document_id="doc-01",
                content_hash="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                s3_input_bucket="rag-bucket",
                s3_input_key="raw/tenant-corp-01/hash/file.pdf",
                s3_output_parquet_key="artifacts/tenant-corp-01/hash/slice_1_5.parquet",
                start_page=1,
                end_page=5,
            ),
            presigned_input_get_url="mock://input-url",
            presigned_output_put_url="mock://output-url",
        )

        resp_dict = await env.run(DispatchDocAIExtractActivity, req.model_dump())
        resp = ExtractSegmentResponse.model_validate(resp_dict)

        # Processed units 3, 4, 5 (3 units)
        self.assertEqual(resp.extracted_segments, 3)
        self.assertEqual(resp.extracted_pages, 5)
        # Should have emitted heartbeats for units 3, 4, 5
        self.assertEqual(len(heartbeats), 3)
        last_hb = heartbeats[-1][0]
        self.assertEqual(last_hb["last_processed_unit"], 5)


if __name__ == "__main__":
    unittest.main()
