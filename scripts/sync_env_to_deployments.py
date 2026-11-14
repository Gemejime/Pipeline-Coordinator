#!/usr/bin/env python3
"""Synchronizes deployment configuration from .env.prod to deployments/helm/values.yaml

and enforces zero configuration drift before commits.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Ensure clean UTF-8 console output across operating systems, including Windows.
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENV_PROD_PATH = REPO_ROOT / ".env.prod"
DEFAULT_VALUES_PATH = REPO_ROOT / "deployments" / "helm" / "values.yaml"

SENSITIVE_SUBSTRINGS = ("PASSWORD", "SECRET", "TOKEN", "KEY", "DSN")


def is_sensitive(key: str) -> bool:
    """Returns True if key name matches any known credential or sensitive pattern."""
    if key == "secretName":
        return False
    upper = key.upper()
    return any(sub in upper for sub in SENSITIVE_SUBSTRINGS)


def parse_env_file(path: Path) -> Dict[str, str]:
    """Parses .env file into key-value pairs, stripping comments and quotes."""
    if not path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    env_vars: Dict[str, str] = {}
    content = path.read_text(encoding="utf-8")

    for line_num, raw_line in enumerate(content.splitlines(), start=1):
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

        # Handle quoted values
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
            val = val[1:-1]
        else:
            # Strip unquoted comments (preceded by whitespace)
            for idx, ch in enumerate(val):
                if ch == "#" and idx > 0 and val[idx - 1] in (" ", "\t"):
                    val = val[:idx].strip()
                    break

        env_vars[key] = val

    return env_vars


def filter_non_sensitive_env(env_vars: Dict[str, str]) -> Dict[str, str]:
    """Filters out sensitive credentials, returning only non-secret runtime configs."""
    return {k: v for k, v in env_vars.items() if not is_sensitive(k)}


def parse_values_env_block(values_path: Path) -> Tuple[Dict[str, str], Optional[str]]:
    """Extracts the existing env (or config) dictionary and preserved secretName from values.yaml."""
    if not values_path.is_file():
        raise FileNotFoundError(f"Helm values file not found: {values_path}")

    import yaml

    content = values_path.read_text(encoding="utf-8")
    data = yaml.safe_load(content) or {}

    env_block = data.get("env")
    if env_block is None and "config" in data:
        # Backward compatibility with legacy config: block
        env_block = data.get("config")

    if not isinstance(env_block, dict):
        return {}, None

    secret_name = env_block.get("secretName")
    return {str(k): str(v) for k, v in env_block.items()}, secret_name


def update_values_yaml(
    values_path: Path,
    non_sensitive_vars: Dict[str, str],
    preserved_secret_name: Optional[str] = None,
) -> None:
    """Updates the env: section in deployments/helm/values.yaml while keeping all other blocks and comments intact."""
    import yaml

    content = values_path.read_text(encoding="utf-8")
    existing_data = yaml.safe_load(content) or {}

    secret_name = preserved_secret_name
    if not secret_name:
        if isinstance(existing_data.get("env"), dict):
            secret_name = existing_data["env"].get("secretName")
        if not secret_name and isinstance(existing_data.get("secrets"), dict):
            secret_name = existing_data["secrets"].get("existingSecret")
        if not secret_name:
            secret_name = "rag-ingestion-coordinator-secrets"

    port = non_sensitive_vars.get("PORT", "8080")
    grpc_port = non_sensitive_vars.get("GRPC_PORT", "50051")

    # Build the formatted env block lines
    lines: List[str] = [
        "# 12-Factor non-secret runtime environment (synced automatically from .env.prod).",
        "env:",
        f'  secretName: "{secret_name}"',
        f'  PORT: "{port}"',
        f'  GRPC_PORT: "{grpc_port}"',
    ]

    for key in sorted(non_sensitive_vars.keys()):
        if key in ("secretName", "PORT", "GRPC_PORT"):
            continue
        val = non_sensitive_vars[key]
        # Escape internal quotes if any
        escaped_val = val.replace('"', '\\"')
        lines.append(f'  {key}: "{escaped_val}"')

    new_env_block = "\n".join(lines) + "\n"

    # Match existing env: or legacy config: section up to the next top-level YAML key
    pattern = re.compile(
        r"(?:#[^\n]*\n)*^(?:env|config):.*?(?=^[a-zA-Z0-9_-]+:|\Z)",
        re.MULTILINE | re.DOTALL,
    )

    if pattern.search(content):
        updated_content = pattern.sub(new_env_block + "\n", content, count=1)
    else:
        # If neither exists, append env: block before secrets: if present, else at end
        secrets_match = re.search(r"^(secrets:)", content, re.MULTILINE)
        if secrets_match:
            idx = secrets_match.start()
            updated_content = content[:idx] + new_env_block + "\n" + content[idx:]
        else:
            updated_content = content + "\n" + new_env_block

    # Clean up any excessive newlines created during replacement
    updated_content = re.sub(r"\n{3,}", "\n\n", updated_content)

    # Validate that resulting document remains syntactically valid YAML
    verified_data = yaml.safe_load(updated_content)
    if not isinstance(verified_data, dict) or "env" not in verified_data:
        raise ValueError("Failed to render valid YAML with env block in values.yaml")

    values_path.write_text(updated_content, encoding="utf-8")


def check_drift(
    env_prod_path: Path,
    values_path: Path,
) -> Tuple[bool, List[str]]:
    """Compares .env.prod non-sensitive variables against values.yaml env: block and detects drift."""
    prod_vars = parse_env_file(env_prod_path)
    non_sensitive_prod = filter_non_sensitive_env(prod_vars)
    values_env, _ = parse_values_env_block(values_path)

    drift_messages: List[str] = []

    if not values_env:
        drift_messages.append("Missing or empty 'env:' block in deployments/helm/values.yaml")
        return False, drift_messages

    # Security check: Ensure no sensitive keys leaked into values.yaml
    for key in values_env.keys():
        if is_sensitive(key):
            drift_messages.append(
                f"SECURITY VIOLATION: Sensitive key {key!r} detected in deployments/helm/values.yaml"
            )

    # Check for missing or mismatched values from .env.prod
    for key, expected_val in sorted(non_sensitive_prod.items()):
        if key not in values_env:
            drift_messages.append(f"Missing key in values.yaml [env]: {key}")
        elif values_env[key] != expected_val:
            drift_messages.append(
                f"Value mismatch for {key}: .env.prod={expected_val!r} vs values.yaml={values_env[key]!r}"
            )

    # Check for stale extra keys in values.yaml
    allowed_extras = {"secretName", "PORT", "GRPC_PORT"}
    for key in sorted(values_env.keys()):
        if key not in non_sensitive_prod and key not in allowed_extras:
            drift_messages.append(f"Stale extra key in values.yaml [env] not in .env.prod: {key}")

    is_clean = len(drift_messages) == 0
    return is_clean, drift_messages


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Automated environment synchronization and drift detection (.env.prod -> Helm values.yaml)"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--sync",
        action="store_true",
        help="Synchronize non-sensitive variables from .env.prod into deployments/helm/values.yaml",
    )
    group.add_argument(
        "--check-only",
        action="store_true",
        help="Verify zero drift between .env.prod and deployments/helm/values.yaml (exit 0 clean, 1 drift)",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_PROD_PATH,
        help="Path to .env.prod (default: .env.prod in repo root)",
    )
    parser.add_argument(
        "--values-file",
        type=Path,
        default=DEFAULT_VALUES_PATH,
        help="Path to values.yaml (default: deployments/helm/values.yaml)",
    )

    args = parser.parse_args()
    env_path: Path = args.env_file
    values_path: Path = args.values_file

    if args.sync:
        if not env_path.is_file():
            print(f"[ERROR] Source environment file not found: {env_path}", file=sys.stderr)
            return 1
        raw_env = parse_env_file(env_path)
        non_sensitive = filter_non_sensitive_env(raw_env)
        _, secret_name = parse_values_env_block(values_path) if values_path.is_file() else ({}, None)
        update_values_yaml(values_path, non_sensitive, secret_name)
        print(f"[SUCCESS] Synchronized {len(non_sensitive)} non-sensitive variables from {env_path.name} to {values_path}.")
        return 0

    if args.check_only:
        if not env_path.is_file():
            print(f"[ERROR] Source environment file not found: {env_path}", file=sys.stderr)
            return 1
        clean, drifts = check_drift(env_path, values_path)
        if clean:
            print(f"[OK] Zero drift detected between {env_path.name} and {values_path.name}.")
            return 0
        else:
            print(f"[DRIFT DETECTED] {len(drifts)} discrepancy(ies) between {env_path.name} and {values_path.name}:", file=sys.stderr)
            for d in drifts:
                print(f"  - {d}", file=sys.stderr)
            print("\nRun 'python scripts/sync_env_to_deployments.py --sync' to resolve drift.", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
