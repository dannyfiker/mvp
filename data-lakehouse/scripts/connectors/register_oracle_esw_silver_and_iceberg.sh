#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

dirname "${BASH_SOURCE[0]}" >/dev/null 2>&1 || true
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

ORACLE_SOURCE_JSON="${ORACLE_SOURCE_JSON:-${REPO_ROOT}/data-lakehouse-ops/connectors/oracle-esw.json}"
ICEBERG_SINK_JSON="${ICEBERG_SINK_JSON:-${REPO_ROOT}/data-lakehouse-ops/connectors/iceberg-sink-oracle-esw.json}"

render_with_oracle_overrides() {
  local in_json="$1"
  local out_json="$2"

  python3 - "$in_json" "$out_json" <<'PY'
import json
import os
import sys

in_path, out_path = sys.argv[1], sys.argv[2]
with open(in_path, "r", encoding="utf-8") as f:
  doc = json.load(f)

cfg = doc.get("config")
if cfg is None:
  # Support config-only JSON as well.
  cfg = doc

def set_if(env_name: str, key: str) -> None:
  value = os.getenv(env_name)
  if value:
    cfg[key] = value

# Common overrides (match OracleProbe.java env names where possible)
set_if("ORACLE_URL", "database.url")
set_if("ORACLE_USER", "database.user")
set_if("ORACLE_PASSWORD", "database.password")

# Optional fine-grained overrides
set_if("ORACLE_HOSTNAME", "database.hostname")
set_if("ORACLE_PORT", "database.port")
set_if("ORACLE_DBNAME", "database.dbname")
set_if("ORACLE_PDB_NAME", "database.pdb.name")

with open(out_path, "w", encoding="utf-8") as f:
  json.dump(doc, f, indent=2, sort_keys=False)
  f.write("\n")
PY
}

post_connector() {
  local json_path="$1"
  local name
  name="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "$json_path")"

  local effective_json_path="$json_path"
  local tmp_json=""

  # Allow overriding Oracle DB connection settings without editing committed JSON.
  # Only applies to the oracle-esw source connector.
  if [[ "$name" == "oracle-esw" ]] && (
    [[ -n "${ORACLE_URL:-}" ]] ||
    [[ -n "${ORACLE_USER:-}" ]] ||
    [[ -n "${ORACLE_PASSWORD:-}" ]] ||
    [[ -n "${ORACLE_HOSTNAME:-}" ]] ||
    [[ -n "${ORACLE_PORT:-}" ]] ||
    [[ -n "${ORACLE_DBNAME:-}" ]] ||
    [[ -n "${ORACLE_PDB_NAME:-}" ]]
  ); then
    tmp_json="$(mktemp -t oracle-esw-connector.XXXXXX.json)"
    render_with_oracle_overrides "$json_path" "$tmp_json"
    effective_json_path="$tmp_json"
  fi

  echo "==> Deleting existing connector (if any): ${name}"
  curl -fsS -X DELETE "${CONNECT_URL}/connectors/${name}" >/dev/null || true

  echo "==> Creating connector: ${name} (${json_path})"
  curl -fsS -X POST "${CONNECT_URL}/connectors" \
    -H 'Content-Type: application/json' \
    --data-binary "@${effective_json_path}" \
    | cat
  echo

  if [[ -n "$tmp_json" ]]; then
    rm -f "$tmp_json" || true
  fi
}

echo "Using Kafka Connect: ${CONNECT_URL}"
echo "Oracle source config: ${ORACLE_SOURCE_JSON}"
echo "Iceberg sink config:  ${ICEBERG_SINK_JSON}"
echo

post_connector "${ORACLE_SOURCE_JSON}"
post_connector "${ICEBERG_SINK_JSON}"

echo "==> Done. Current connectors:"
curl -fsS "${CONNECT_URL}/connectors" | cat
echo
