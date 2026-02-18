#!/usr/bin/env python3

import argparse
import base64
import json
import sys
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, Optional

import requests
from fastavro import parse_schema, schemaless_reader

try:
    from confluent_kafka import Consumer  # type: ignore

    _KAFKA_CLIENT = "confluent-kafka"
except Exception:  # pragma: no cover
    Consumer = None  # type: ignore
    _KAFKA_CLIENT = "unavailable"


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return {"__bytes__": base64.b64encode(bytes(obj)).decode("ascii")}
    return str(obj)


def _coerce_debezium(obj: Any) -> Any:
    """Best-effort coercions for Debezium logical types.

    - VariableScaleDecimal: {scale:int, value:bytes} -> string decimal
    - MicroTimestamp: long micros since epoch -> ISO-8601 string (UTC)

    Falls back to the original value when it can't coerce safely.
    """
    if isinstance(obj, dict):
        # Debezium VariableScaleDecimal
        if set(obj.keys()) == {"scale", "value"} and isinstance(obj.get("scale"), int) and isinstance(
            obj.get("value"), (bytes, bytearray)
        ):
            scale = obj["scale"]
            raw = bytes(obj["value"])
            unscaled = int.from_bytes(raw, byteorder="big", signed=True)
            if scale == 0:
                return str(unscaled)
            return str(Decimal(unscaled).scaleb(-scale))

        return {k: _coerce_debezium(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_coerce_debezium(v) for v in obj]

    # Debezium MicroTimestamp is an Avro long; without schema context we can't distinguish
    # from other longs reliably, so we only coerce when a caller passes it through explicitly.
    return obj


def _coerce_debezium_json(obj: Any) -> Any:
    """Coerce Debezium logical types when they appear inside JSON strings.

    In this stack, message keys are often schemaless JSON. Some Debezium logical
    types (notably VariableScaleDecimal) can show up as:

        {"scale": 0, "value": "AQ=="}

    where the bytes are base64-encoded. This helper converts those into the
    same readable string form as `_coerce_debezium`.
    """
    if isinstance(obj, dict):
        if set(obj.keys()) == {"scale", "value"} and isinstance(obj.get("scale"), int) and isinstance(
            obj.get("value"), str
        ):
            try:
                raw = base64.b64decode(obj["value"], validate=True)
                return _coerce_debezium({"scale": obj["scale"], "value": raw})
            except Exception:
                # Not base64 or unexpected format; fall through to recursive coercion.
                pass

        return {k: _coerce_debezium_json(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_coerce_debezium_json(v) for v in obj]

    return obj


def _coerce_microtimestamp(micros: Any) -> Any:
    if not isinstance(micros, int):
        return micros
    try:
        return datetime.fromtimestamp(micros / 1_000_000, tz=UTC).isoformat()
    except Exception:
        return micros


def fetch_avro_schema(registry_url: str, group_id: str, artifact_id: str) -> Dict[str, Any]:
    url = f"{registry_url.rstrip('/')}/groups/{group_id}/artifacts/{artifact_id}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def try_decode_avro(value_bytes: bytes, schema: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    parsed = parse_schema(schema)

    # Apicurio + Confluent-style serializers often prefix a header before the Avro payload.
    # We try a small set of common header lengths.
    candidates: Iterable[int] = (0, 5, 9, 10, 13)

    for header_len in candidates:
        if len(value_bytes) <= header_len:
            continue
        try:
            from io import BytesIO

            bio = BytesIO(value_bytes[header_len:])
            record = schemaless_reader(bio, parsed)
            if isinstance(record, dict):
                return record
            return {"_": record}
        except Exception:
            continue

    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Consume a Kafka topic and decode Apicurio-registered Avro values into JSON. "
            "Keys are printed as UTF-8 (typically JSON in this stack)."
        )
    )
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", required=True, help="Kafka topic to consume")
    parser.add_argument(
        "--registry-url",
        default="http://localhost:8081/apis/registry/v2",
        help="Apicurio Registry v2 base URL (host-side)",
    )
    parser.add_argument("--registry-group", default="oracle-esw", help="Apicurio groupId")
    parser.add_argument(
        "--artifact-id",
        default=None,
        help="Apicurio artifactId for the value schema (defaults to '<topic>-value')",
    )
    parser.add_argument("--max-messages", type=int, default=5, help="Stop after N messages")
    parser.add_argument("--from-beginning", action="store_true", help="Start from earliest")
    parser.add_argument(
        "--print",
        choices=["after", "before", "envelope"],
        default="after",
        help="What to print from the Debezium envelope (default: after)",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="Stop polling after this many seconds if no messages are received (default: 30).",
    )

    args = parser.parse_args()

    if Consumer is None:
        print(
            "ERROR: confluent-kafka is not installed. Activate the repo venv and install it: "
            "`.venv/bin/python -m pip install confluent-kafka`",
            file=sys.stderr,
        )
        return 2

    artifact_id = args.artifact_id or f"{args.topic}-value"

    try:
        schema = fetch_avro_schema(args.registry_url, args.registry_group, artifact_id)
    except Exception as exc:
        print(f"ERROR: failed to fetch schema for {args.registry_group}/{artifact_id}: {exc}", file=sys.stderr)
        return 2

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": f"consume-apicurio-avro-{args.topic}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        }
    )
    consumer.subscribe([args.topic])

    count = 0
    deadline = datetime.now(tz=UTC).timestamp() + max(0.0, float(args.timeout_seconds))
    try:
        while True:
            now = datetime.now(tz=UTC).timestamp()
            if now > deadline and count == 0:
                break

            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # Keep going on non-fatal errors.
                print(f"WARN: kafka poll error: {msg.error()}", file=sys.stderr)
                continue

            count += 1

            raw_key = msg.key()
            raw_value = msg.value()

            key = None
            if raw_key is not None:
                try:
                    key_text = raw_key.decode("utf-8")
                    try:
                        key_json = json.loads(key_text)
                        key = _coerce_debezium_json(key_json)
                    except Exception:
                        key = key_text
                except Exception:
                    key = {"__bytes__": base64.b64encode(raw_key).decode("ascii")}

            decoded = None
            if raw_value is not None:
                decoded = try_decode_avro(raw_value, schema)

            printable_value: Any = decoded
            if isinstance(decoded, dict) and args.print in {"after", "before"}:
                printable_value = decoded.get(args.print)

                # Coerce common Debezium types inside the row record.
                printable_value = _coerce_debezium(printable_value)

                # Heuristic: coerce likely timestamp fields if present.
                if isinstance(printable_value, dict):
                    for field_name, field_value in list(printable_value.items()):
                        if field_name in {"FRST_RGSR_DTTM", "LAST_MOD_DTTM"} or field_name.endswith(
                            ("_DTTM", "_TS")
                        ):
                            printable_value[field_name] = _coerce_microtimestamp(field_value)

            elif isinstance(decoded, dict) and args.print == "envelope":
                printable_value = _coerce_debezium(decoded)

            out = {
                "client": _KAFKA_CLIENT,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp": msg.timestamp()[1] if msg.timestamp() else None,
                "key": key,
                "value": printable_value,
            }

            if decoded is None and raw_value is not None:
                out["value_decode_error"] = "Failed to decode with common header lengths; dumping raw bytes as base64"
                out["value_raw_b64"] = base64.b64encode(raw_value).decode("ascii")

            print(json.dumps(out, ensure_ascii=False, default=_json_default))

            if count >= args.max_messages:
                break
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
