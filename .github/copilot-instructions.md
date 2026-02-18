# Copilot instructions (Lakehouse MVP)

## Big picture
- This repo is a containerized **data lakehouse MVP** orchestrated by Docker Compose (compose files live under the data-lakehouse-ops folder).
- Data flow (happy path): **Postgres CDC (Debezium Connect)** → **Kafka** (Avro values via **Apicurio Registry**) → **Spark jobs** land/transform → **Iceberg** tables on **Ozone S3** → queried by **Trino** and visualized in **Superset**.
- The custom app **gov-aggregator** is a **Java Kafka Streams** service (source under: data-lakehouse → apps → gov-aggregator) that reads multiple source topics and emits unified Avro records.

## Where to change code (and where not to)
- Most MVP iteration happens in:
  - Spark/PySpark jobs: data-lakehouse → scripts → jobs
  - Registry utilities + schema mirroring: data-lakehouse → scripts → registry and data-lakehouse → avro
  - Kafka Streams app: data-lakehouse → apps → gov-aggregator
  - Ops/runtime wiring: data-lakehouse-ops
- The top-level ranger folder is a large Apache Ranger source tree; avoid broad refactors/formatting there unless a change is explicitly requested.

## Local/dev workflows (commands that matter)
- Build the gov-aggregator JAR (also mounted into Kafka Connect as the Apicurio Avro converter):
  - `cd data-lakehouse/apps/gov-aggregator && mvn -DskipTests package`
- Start the stack:
  - `cd data-lakehouse-ops && docker compose up --build`
- Run Spark jobs inside the Spark container (jobs are mounted at `/opt/jobs`):
  - `docker exec -it mvp-spark spark-submit /opt/jobs/kafka_silver_to_iceberg.py`

## Kafka/Avro/Registry conventions
- Kafka Connect is configured for **schemaless JSON keys** + **Avro values** using Apicurio (see the Debezium connector config under: data-lakehouse-ops → connectors).
- Checked-in Avro schemas mirror Apicurio groups under: data-lakehouse → avro (example group `pg1`).
- Use the provided scripts rather than hand-editing registry state:
  - Export from Apicurio → repo: `python3 data-lakehouse/scripts/registry/export_apicurio_schemas.py --group pg1`
  - Import/seed repo → Apicurio: `python3 data-lakehouse/scripts/registry/import_apicurio_schemas.py --group pg1`

## Spark/Iceberg job patterns used here
- Streaming ingest of Kafka “silver” topics is implemented in the Spark job `kafka_silver_to_iceberg.py` under: data-lakehouse → scripts → jobs.
  - Reads topics by regex `TOPIC_PATTERN` (default `silver\\..*`).
  - Writes to per-topic Iceberg tables: `iceberg.silver.<topicSuffix>`.
  - Persists **raw bytes** (`kafka_key`/`payload` as `BINARY`) instead of hardcoding per-table Avro schemas.
  - Prefer the existing env-var style (`env(name, default)`) and keep defaults aligned with the compose network (Kafka `kafka:9092`, Iceberg REST `http://iceberg-rest:8181`, Ozone S3G `http://ozone-s3g:9878`).

## Service naming assumptions
- Compose container names are stable (`mvp-kafka`, `mvp-spark`, `mvp-iceberg-rest`, `mvp-apicurio`, …). Inside the compose network, services typically resolve by alias (notably `kafka`).
