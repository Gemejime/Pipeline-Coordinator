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

    async def test_activity_resume_checkpoint_fallback_across_represign(self):
        """Verifies activity resumes from req.resume_checkpoint when heartbeat_details are absent."""
        env = ActivityEnvironment()
        heartbeats = []
        env.on_heartbeat = lambda *args: heartbeats.append(args)

        checkpoint = HeartbeatDetails(
            slice_id="slice-represign-resume-01",
            slice_index=2,
            last_processed_unit=4,
            processed_units=4,
            total_units=6,
            intermediate_s3_key="intermediate/tenant/hash/unit_4.tmp",
            updated_at_unix=1794039650,
        )

        req = ExtractSegmentRequest(
            slice=PartitionSlice(
                slice_id="slice-represign-resume-01",
                slice_index=2,
                total_slices=1,
                tenant_id="tenant-corp-01",
                document_id="doc-01",
                content_hash="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                s3_input_bucket="rag-bucket",
                s3_input_key="raw/tenant-corp-01/hash/file.pdf",
                s3_output_parquet_key="artifacts/tenant-corp-01/hash/slice_1_6.parquet",
                start_page=1,
                end_page=6,
            ),
            presigned_input_get_url="mock://input-url-attempt-2",
            presigned_output_put_url="mock://output-url-attempt-2",
            resume_checkpoint=checkpoint,
        )

        resp_dict = await env.run(DispatchDocAIExtractActivity, req.model_dump())
        resp = ExtractSegmentResponse.model_validate(resp_dict)

        # Processed units 5, 6 (2 units) without repeating 1..4
        self.assertEqual(resp.extracted_segments, 2)
        self.assertEqual(resp.extracted_pages, 6)
        self.assertEqual(len(heartbeats), 2)
        self.assertEqual(heartbeats[0][0]["last_processed_unit"], 5)
        self.assertEqual(heartbeats[1][0]["last_processed_unit"], 6)


class TestWorkerDotEnv(unittest.TestCase):
    """Verifies Python worker .env loading respects process env precedence."""

    def test_load_dotenv_sets_missing_and_preserves_existing(self):
        import os
        import tempfile
        from pathlib import Path
        from workers.python.rag_document_ai_worker.worker import load_dotenv

        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text(
                "PY_DOTENV_HOST=temporal.example:7233 # comment\n"
                "export PY_DOTENV_NS='staging'\n"
                "PY_DOTENV_EXISTING=from-file\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"PY_DOTENV_EXISTING": "from-env"}, clear=False):
                os.environ.pop("PY_DOTENV_HOST", None)
                os.environ.pop("PY_DOTENV_NS", None)
                load_dotenv(str(env_path))
                self.assertEqual(os.environ.get("PY_DOTENV_HOST"), "temporal.example:7233")
                self.assertEqual(os.environ.get("PY_DOTENV_NS"), "staging")
                self.assertEqual(os.environ.get("PY_DOTENV_EXISTING"), "from-env")


class TestSyncEnvToDeploymentsScript(unittest.TestCase):
    """Verifies scripts/sync_env_to_deployments.py drift detection and credential filtering."""

    @classmethod
    def setUpClass(cls):
        import sys
        from pathlib import Path
        repo_root = Path(__file__).resolve().parents[3]
        scripts_dir = str(repo_root / "scripts")
        if scripts_dir not in sys.path:
            sys.path.insert(0, scripts_dir)

    def test_sensitive_filtering(self):
        import sync_env_to_deployments as sync_script

        self.assertTrue(sync_script.is_sensitive("AUTH_HMAC_SECRET"))
        self.assertTrue(sync_script.is_sensitive("POSTGRES_PASSWORD"))
        self.assertTrue(sync_script.is_sensitive("AWS_ACCESS_KEY_ID"))
        self.assertTrue(sync_script.is_sensitive("AWS_SESSION_TOKEN"))
        self.assertTrue(sync_script.is_sensitive("POSTGRES_DSN"))
        self.assertFalse(sync_script.is_sensitive("secretName"))
        self.assertFalse(sync_script.is_sensitive("LOG_LEVEL"))
        self.assertFalse(sync_script.is_sensitive("PORT"))
        self.assertFalse(sync_script.is_sensitive("S3_BUCKET"))

    def test_check_drift_on_current_workspace_passes(self):
        from pathlib import Path
        repo_root = Path(__file__).resolve().parents[3]
        env_prod = repo_root / ".env.prod"
        values_yaml = repo_root / "deployments" / "helm" / "values.yaml"
        import sync_env_to_deployments as sync_script

        clean, drifts = sync_script.check_drift(env_prod, values_yaml)
        self.assertTrue(clean, f"Expected zero drift, got: {drifts}")
        self.assertEqual(len(drifts), 0)


class TestInjectK8sSecretsScript(unittest.TestCase):
    """Verifies scripts/inject_k8s_secrets.py validation, masking, and manifest generation."""

    @classmethod
    def setUpClass(cls):
        import sys
        from pathlib import Path
        repo_root = Path(__file__).resolve().parents[3]
        scripts_dir = str(repo_root / "scripts")
        if scripts_dir not in sys.path:
            sys.path.insert(0, scripts_dir)

    def test_secret_validation_and_manifest_generation(self):
        import inject_k8s_secrets as inject_mod

        # Missing required secrets
        valid, errors = inject_mod.validate_secrets({})
        self.assertFalse(valid)
        self.assertTrue(any("POSTGRES_DSN" in err for err in errors))
        self.assertTrue(any("AUTH_HMAC_SECRET" in err for err in errors))

        # Too short HMAC secret
        valid, errors = inject_mod.validate_secrets({
            "POSTGRES_DSN": "postgres://user:pass@localhost:5432/db",
            "AUTH_HMAC_SECRET": "too-short",
        })
        self.assertFalse(valid)
        self.assertTrue(any("minimum security requirement" in err for err in errors))

        # Valid secrets generate proper Kubernetes manifest
        valid_secrets = {
            "POSTGRES_DSN": "postgres://user:pass@localhost:5432/db",
            "AUTH_HMAC_SECRET": "12345678901234567890123456789012",
            "REDIS_PASSWORD": "redis-pass-val",
        }
        valid, errors = inject_mod.validate_secrets(valid_secrets)
        self.assertTrue(valid)
        self.assertEqual(len(errors), 0)

        yaml_str = inject_mod.generate_secret_manifest_yaml(
            "test-secrets", "test-ns", valid_secrets
        )
        self.assertIn("kind: Secret", yaml_str)
        self.assertIn("name: test-secrets", yaml_str)
        self.assertIn("namespace: test-ns", yaml_str)
        self.assertIn("POSTGRES_DSN:", yaml_str)
        self.assertIn("AUTH_HMAC_SECRET:", yaml_str)
        self.assertIn("REDIS_PASSWORD:", yaml_str)
        # Verify base64 encoding (not cleartext)
        self.assertNotIn("postgres://user:pass", yaml_str)


if __name__ == "__main__":
    unittest.main()
