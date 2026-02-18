import json
import pathlib
import re
from typing import Dict, List, Tuple

ROOT = pathlib.Path(__file__).resolve().parents[2]  # data-lakehouse/
AVRO_DIR = ROOT / "avro" / "oracle-esw" / "ESW"


def load_avsc(path: pathlib.Path) -> Dict:
    return json.loads(path.read_text(encoding="utf-8"))


def find_value_record(schema: Dict) -> Tuple[str, List[Dict]]:
    """Return (fullName, fields) for the nested record named 'Value'."""
    for f in schema.get("fields", []):
        if f.get("name") in ("before", "after"):
            t = f.get("type")
            # Debezium envelope uses union ["null", {record Value...}]
            if isinstance(t, list):
                for u in t:
                    if isinstance(u, dict) and u.get("type") == "record" and u.get("name") == "Value":
                        ns = u.get("namespace") or schema.get("namespace")
                        full = f"{ns}.{u['name']}" if ns else u["name"]
                        return full, u.get("fields", [])
            elif isinstance(t, dict) and t.get("type") == "record" and t.get("name") == "Value":
                ns = t.get("namespace") or schema.get("namespace")
                full = f"{ns}.{t['name']}" if ns else t["name"]
                return full, t.get("fields", [])
    raise ValueError("Could not locate nested Value record")


def table_name_from_namespace(ns: str) -> str:
    # oracle_esw.ESW.TB_CB_LPCO
    return ns.split(".")[-1]


def main() -> None:
    files = sorted(p for p in AVRO_DIR.glob("*.avsc") if p.name.startswith("TB_"))
    rows = []
    for p in files:
        env = load_avsc(p)
        ns = env.get("namespace", "")
        table = table_name_from_namespace(ns)
        _, fields = find_value_record(env)
        cols = [f["name"].lower() for f in fields]
        rows.append((table, cols))

    print("# Proposed silver topics + columns (oracle-esw)\n")
    for table, cols in rows:
        topic = f"silver.oracle_esw.{table}"
        iceberg_table = f"silver.{table.lower()}"
        print(f"## {table}")
        print(f"- bronze topic: raw-{table}")
        print(f"- silver topic: {topic}")
        print(f"- iceberg table: {iceberg_table}")
        print(f"- columns ({len(cols) + 1}):")
        print("  - __iceberg_table")
        for c in cols:
            print(f"  - {c}")
        print()


if __name__ == "__main__":
    main()
