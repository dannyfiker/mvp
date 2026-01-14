#!/usr/bin/env python3

import argparse
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


def env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def http_post(url: str, body: bytes, headers: dict[str, str]) -> int:
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req) as resp:
        return resp.getcode()


def main() -> int:
    parser = argparse.ArgumentParser(description="Import .avsc files into Apicurio Registry (seed/update schemas)")
    parser.add_argument(
        "--registry",
        default=env("APICURIO_URL", "http://localhost:8081/apis/registry/v2"),
        help="Apicurio v2 base URL (default: http://localhost:8081/apis/registry/v2)",
    )
    parser.add_argument(
        "--group",
        default=env("APICURIO_GROUP", "pg1"),
        help="Apicurio group id to import into (default: pg1)",
    )
    parser.add_argument(
        "--in",
        dest="in_dir",
        default=env("AVRO_DIR", "data-lakehouse/avro"),
        help="Input root directory (default: data-lakehouse/avro)",
    )
    args = parser.parse_args()

    registry = args.registry.rstrip("/")
    group = args.group
    in_root = Path(args.in_dir) / group

    if not in_root.exists():
        print(f"ERROR: input folder not found: {in_root}", file=sys.stderr)
        return 2

    files = sorted([p for p in in_root.rglob("*.avsc") if p.is_file()])
    if not files:
        print(f"No .avsc files found under {in_root}")
        return 0

    ok = 0
    failed = 0
    for path in files:
        # Derive artifactId from path. We intentionally keep it stable and explicit:
        #   <group>/<schema>/<table>.avsc  =>  <group>.<schema>.<table>-value
        rel = path.relative_to(in_root)
        parts = rel.parts
        if len(parts) == 2 and parts[-1].endswith(".avsc"):
            schema = parts[0]
            table = path.stem
            artifact_id = f"{group}.{schema}.{table}-value"
        else:
            # Fallback: use filename without extension.
            artifact_id = path.stem

        create_url = f"{registry}/groups/{urllib.parse.quote(group, safe='')}/artifacts"
        versions_url = (
            f"{registry}/groups/{urllib.parse.quote(group, safe='')}/artifacts/"
            f"{urllib.parse.quote(artifact_id, safe='')}/versions"
        )

        body = path.read_bytes()
        create_headers = {
            "Content-Type": "application/json",
            "X-Registry-ArtifactType": "AVRO",
            "X-Registry-ArtifactId": artifact_id,
        }

        version_headers = {
            "Content-Type": "application/json",
            "X-Registry-ArtifactType": "AVRO",
        }

        try:
            code = http_post(create_url, body, create_headers)
            if code in (200, 201):
                ok += 1
                continue

            failed += 1
            print(f"FAIL {artifact_id}: HTTP {code}")
        except urllib.error.HTTPError as e:
            # Apicurio returns 409 if the artifact already exists; create a new version instead.
            if e.code == 409:
                try:
                    vcode = http_post(versions_url, body, version_headers)
                    if vcode in (200, 201):
                        ok += 1
                        continue

                    failed += 1
                    print(f"FAIL {artifact_id}: HTTP {vcode} on versions")
                    continue
                except urllib.error.HTTPError as ve:
                    failed += 1
                    detail = ve.read().decode("utf-8", errors="replace") if hasattr(ve, "read") else ""
                    print(f"FAIL {artifact_id}: HTTP {ve.code} {ve.reason} {detail}")
                    continue
                except Exception as ve:
                    failed += 1
                    print(f"FAIL {artifact_id}: {ve}")
                    continue

            failed += 1
            detail = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
            print(f"FAIL {artifact_id}: HTTP {e.code} {e.reason} {detail}")
        except Exception as e:
            failed += 1
            print(f"FAIL {artifact_id}: {e}")

    print(f"Imported {ok} schema file(s) into group '{group}' ({failed} failed)")
    return 0 if failed == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
