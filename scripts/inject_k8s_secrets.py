#!/usr/bin/env python3
"""CI/CD Kubernetes Secret Injection Helper for rag-ingestion-coordinator.

Safely injects required sensitive credentials (POSTGRES_DSN, AUTH_HMAC_SECRET,
REDIS_PASSWORD, etc.) into a target Kubernetes Secret without exposing cleartext
values to CI runner stdout or system logs.
"""

from __future__ import annotations

import argparse
import base64
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Ensure clean UTF-8 console output across operating systems, including Windows.
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

# Sensitive keys supported by rag-ingestion-coordinator
SECRET_KEYS = (
    "POSTGRES_DSN",
    "AUTH_HMAC_SECRET",
    "REDIS_PASSWORD",
    "AUTH_STATIC_TOKENS_JSON",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
)

REQUIRED_SECRET_KEYS = (
    "POSTGRES_DSN",
    "AUTH_HMAC_SECRET",
)

MIN_HMAC_SECRET_LENGTH = 32


def parse_env_file(path: Path) -> Dict[str, str]:
    """Parses .env file into key-value pairs, stripping comments and quotes."""
    if not path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    env_vars: Dict[str, str] = {}
    content = path.read_text(encoding="utf-8")

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue

        key, val = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        val = val.strip()

        if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
            val = val[1:-1]
        else:
            for idx, ch in enumerate(val):
                if ch == "#" and idx > 0 and val[idx - 1] in (" ", "\t"):
                    val = val[:idx].strip()
                    break

        env_vars[key] = val

    return env_vars


def gather_secrets(env_file_path: Optional[Path] = None) -> Dict[str, str]:
    """Gathers sensitive values from environment variables or optional env file."""
    source_vars: Dict[str, str] = {}
    if env_file_path:
        source_vars.update(parse_env_file(env_file_path))

    # Process environment variables take precedence over file
    secrets: Dict[str, str] = {}
    for key in SECRET_KEYS:
        val = os.getenv(key)
        if val is None and key in source_vars:
            val = source_vars[key]
        if val is not None and val.strip() != "":
            secrets[key] = val.strip()

    return secrets


def validate_secrets(secrets: Dict[str, str]) -> Tuple[bool, List[str]]:
    """Validates presence and constraints of collected secrets."""
    errors: List[str] = []

    for req in REQUIRED_SECRET_KEYS:
        if req not in secrets or not secrets[req]:
            errors.append(f"Missing required secret: '{req}'")

    hmac_secret = secrets.get("AUTH_HMAC_SECRET", "")
    if hmac_secret and len(hmac_secret) < MIN_HMAC_SECRET_LENGTH:
        errors.append(
            f"AUTH_HMAC_SECRET length ({len(hmac_secret)}) is below minimum "
            f"security requirement of {MIN_HMAC_SECRET_LENGTH} characters"
        )

    return (len(errors) == 0, errors)


def generate_secret_manifest_yaml(
    secret_name: str,
    namespace: str,
    secrets: Dict[str, str],
) -> str:
    """Generates standard Kubernetes Secret YAML with base64-encoded values."""
    b64_entries: List[str] = []
    for key in sorted(secrets.keys()):
        raw_bytes = secrets[key].encode("utf-8")
        encoded = base64.b64encode(raw_bytes).decode("ascii")
        b64_entries.append(f"  {key}: {encoded}")

    data_block = "\n".join(b64_entries)
    manifest = f"""apiVersion: v1
kind: Secret
metadata:
  name: {secret_name}
  namespace: {namespace}
  labels:
    app.kubernetes.io/name: rag-ingestion-coordinator
    app.kubernetes.io/managed-by: github-actions-ci-cd
type: Opaque
data:
{data_block}
"""
    return manifest


def apply_secret_to_k8s(
    manifest_yaml: str,
    secret_name: str,
    namespace: str,
    dry_run: bool = False,
) -> None:
    """Applies the secret to Kubernetes using kubectl apply without echoing secret values."""
    cmd = ["kubectl", "apply", "-f", "-"]
    if dry_run:
        cmd.extend(["--dry-run=client"])

    print(
        f"[INFO] Executing kubectl apply for Secret '{secret_name}' in namespace '{namespace}' "
        f"(dry_run={dry_run})..."
    )
    result = subprocess.run(
        cmd,
        input=manifest_yaml,
        text=True,
        capture_output=True,
        check=False,
    )

    if result.returncode != 0:
        # Sanitize stderr to ensure no potential token disclosure
        clean_err = result.stderr.strip()
        print(f"[ERROR] Failed to apply Kubernetes secret: {clean_err}", file=sys.stderr)
        sys.exit(result.returncode)

    print(f"[SUCCESS] Kubernetes Secret '{secret_name}' successfully configured.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inject sensitive credentials into Kubernetes Secret for rag-ingestion-coordinator."
    )
    parser.add_argument(
        "--secret-name",
        default=os.getenv("K8S_SECRET_NAME", "rag-ingestion-coordinator-secrets"),
        help="Target Kubernetes Secret resource name (default: rag-ingestion-coordinator-secrets)",
    )
    parser.add_argument(
        "--namespace",
        default=os.getenv("K8S_NAMESPACE", "default"),
        help="Target Kubernetes namespace (default: default)",
    )
    parser.add_argument(
        "--from-env-file",
        type=Path,
        default=None,
        help="Optional path to .env file to load fallback secret values from",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and generate manifest without applying to Kubernetes cluster",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=None,
        help="Optional path to save rendered Secret manifest YAML",
    )

    args = parser.parse_args()

    secrets = gather_secrets(args.from_env_file)
    valid, errors = validate_secrets(secrets)

    # Secure summary: print keys and value lengths, never secret contents
    print("[INFO] Discovered secret keys:")
    for k in sorted(secrets.keys()):
        masked_len = len(secrets[k])
        print(f"  - {k}: [PRESENT, length={masked_len} chars]")

    if not valid:
        print("\n[VALIDATION FAILED] Secret injection aborted:", file=sys.stderr)
        for err in errors:
            print(f"  * {err}", file=sys.stderr)
        sys.exit(1)

    print("\n[OK] All secret validation constraints satisfied.")

    manifest_yaml = generate_secret_manifest_yaml(args.secret_name, args.namespace, secrets)

    if args.output_file:
        args.output_file.write_text(manifest_yaml, encoding="utf-8")
        print(f"[INFO] Rendered Secret manifest written to: {args.output_file}")

    if not args.dry_run and not args.output_file:
        apply_secret_to_k8s(manifest_yaml, args.secret_name, args.namespace, dry_run=False)
    elif args.dry_run:
        print(f"[DRY-RUN] Secret manifest verified successfully for '{args.secret_name}'.")


if __name__ == "__main__":
    main()

