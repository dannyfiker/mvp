#!/usr/bin/env python3

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


def env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def http_get_json(url: str) -> dict:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body)


def http_get_text(url: str) -> str:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req) as resp:
        body = resp.read().decode("utf-8")
    return body


def safe_path_segment(seg: str) -> str:
    # Keep filesystem-friendly names, but preserve dots/dashes/underscores.
    return re.sub(r"[^A-Za-z0-9._-]+", "_", seg)


def artifact_to_relpath(artifact_id: str) -> Path:
    """
    Debezium/Connect artifact IDs in this repo look like:
      pg1.nbe_mock.accounts-key
      pg1.nbe_mock.accounts-value

        We store value-only as:
            nbe_mock/accounts.avsc

        If key schemas are exported too, they are stored as:
            nbe_mock/accounts.key.avsc

    Fallback:
      artifacts/<artifact_id>.avsc
    """
    # For this MVP, we route Oracle table topics to `raw-<TABLE>`.
    # When using Apicurio's Avro converter, artifact IDs will be `<topic>-value`.
    # Map these to a stable per-table schema file path.
    m_raw = re.match(r"^raw-(?P<table>.+)-(?P<kind>key|value)$", artifact_id)
    if m_raw:
        table = safe_path_segment(m_raw.group("table"))
        kind = safe_path_segment(m_raw.group("kind"))

        # Oracle tables in this connector are under schema ESW.
        if kind == "value":
            return Path("ESW") / f"{table}.avsc"
        return Path("ESW") / f"{table}.key.avsc"

    m = re.match(r"^(?P<prefix>[^.]+)\.(?P<schema>[^.]+)\.(?P<table>[^-]+)-(?P<kind>key|value)$", artifact_id)
    if m:
        schema = safe_path_segment(m.group("schema"))
        table = safe_path_segment(m.group("table"))
        kind = safe_path_segment(m.group("kind"))

        if kind == "value":
            return Path(schema) / f"{table}.avsc"

        # key
        return Path(schema) / f"{table}.key.avsc"

    return Path("artifacts") / f"{safe_path_segment(artifact_id)}.avsc"


def main() -> int:
    parser = argparse.ArgumentParser(description="Export AVRO schemas from Apicurio Registry into versioned .avsc files")
    parser.add_argument(
        "--registry",
        default=env("APICURIO_URL", "http://localhost:8081/apis/registry/v2"),
        help="Apicurio v2 base URL (default: http://localhost:8081/apis/registry/v2)",
    )
    parser.add_argument(
        "--group",
        default=env("APICURIO_GROUP", "pg1"),
        help="Apicurio group id to export (default: pg1)",
    )
    parser.add_argument(
        "--out",
        default=env("AVRO_DIR", "data-lakehouse/avro"),
        help="Output root directory (default: data-lakehouse/avro)",
    )
    parser.add_argument(
        "--include-keys",
        action="store_true",
        help="Also export *-key artifacts (default: export value-only)",
    )
    args = parser.parse_args()

    registry = args.registry.rstrip("/")
    group = args.group
    out_root = Path(args.out)

    artifacts_url = f"{registry}/groups/{urllib.parse.quote(group, safe='')}/artifacts?limit=1000"
    try:
        listing = http_get_json(artifacts_url)
    except urllib.error.HTTPError as e:
        print(f"ERROR: failed to list artifacts: {e} ({artifacts_url})", file=sys.stderr)
        return 2

    artifacts = listing.get("artifacts") or []
    if not artifacts:
        print(f"No artifacts found in group '{group}'.")
        return 0

    written = 0
    for artifact in artifacts:
        artifact_id = artifact.get("id")
        if not artifact_id:
            continue

        if not args.include_keys and artifact_id.endswith("-key"):
            continue

        # With single-schema-per-topic we only track the value schema files.
        if not args.include_keys and not artifact_id.endswith("-value"):
            continue

        # In this Apicurio deployment, GET /groups/{group}/artifacts/{id} returns the schema content.
        content_url = f"{registry}/groups/{urllib.parse.quote(group, safe='')}/artifacts/{urllib.parse.quote(artifact_id, safe='')}"
        try:
            content = http_get_text(content_url)
        except urllib.error.HTTPError as e:
            print(f"WARN: failed to fetch {group}/{artifact_id}: {e}")
            continue

        rel = artifact_to_relpath(artifact_id)
        target = out_root / safe_path_segment(group) / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        written += 1

    print(f"Exported {written} schema file(s) into {out_root}/{group}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
