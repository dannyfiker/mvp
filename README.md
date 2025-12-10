# Lakehouse MVP: Data Platform for Government Aggregation

This repository provides a modular, containerized data lakehouse MVP for government data aggregation, analytics, and visualization. It integrates modern open-source components—Apache Kafka, Iceberg, Ozone, Trino, Hive, Spark, Superset, and Apicurio—using Docker Compose for orchestration. The core data pipeline is implemented in Java (Kafka Streams), with schema management via Avro and Apicurio Registry.

---

## Architecture Overview

**Key Components:**

**Ozone**: S3-compatible object store for data lake storage.
**Kafka**: Event streaming backbone.
**Debezium Connect**: CDC from PostgreSQL to Kafka.
**Apicurio Registry**: Avro schema registry for Kafka topics.
**Iceberg**: Table format for lakehouse analytics.
**Hive Metastore**: Metadata catalog for Iceberg.
**Trino**: SQL query engine for federated analytics.
**Spark**: Batch ETL and data transformation.
**Superset**: Data visualization and dashboarding.
**Nginx**: Reverse proxy for Superset and custom APIs.
**Gov Aggregator**: Custom Kafka Streams app for unified event aggregation.
**Apache Ranger**: Centralized security, authorization, and data governance for the lakehouse stack.

---

## Directory Structure

```
setup_gov_aggregator.sh         # Bootstrap script for gov-aggregator app
data-lakehouse/                 # Core lakehouse stack
  apps/gov-aggregator/          # Java Kafka Streams app (source code, Avro schemas, Dockerfile)
  hive/                         # Hive metastore configs
  iceberg/                      # Iceberg jobs, jars
  kafbat/                       # Kafka UI config
  ozone/                        # Ozone configs (core-site.xml, ozone-site.xml)
  scripts/                      # ETL jobs, connectors
  superset/                     # Superset Dockerfile
  trino/                        # Trino configs (catalogs, JVM, node)
data-lakehouse-ops/             # Orchestration, ops, and config
  .env                          # Version pins
  docker-compose.yml            # Main Compose file
  docker-compose.override.yml   # Override for dev/local
  nginx/                        # Nginx config for Superset/phi3
  superset/                     # Superset config and home
 ranger/                        # Apache Ranger stack, plugins, and configs
```

---

## Quickstart

### 1. Prerequisites

- Docker & Docker Compose
- Java 17 (for local builds)
- Maven (for building gov-aggregator)

### 2. Build and Launch

#### a) Build the gov-aggregator app

```bash
cd ~/<<PATH_TO_PROJECT>>/mvp/data-lakehouse/apps/gov-aggregator
mvn -DskipTests package
```

#### b) Start the full stack

```bash
cd ~/<<PATH_TO_PROJECT>>/mvp/data-lakehouse-ops
docker compose up --build
```

This will launch all services: Ozone, Kafka, Hive, Iceberg, Trino, Spark, Superset, Apicurio, Gov Aggregator, and Nginx.

Apache Ranger will also be started for security and governance.

---

## Data Flow

1. **CDC Ingestion**: Debezium Connect reads changes from PostgreSQL and writes JSON events to Kafka topics.
2. **Schema Registry**: Avro schemas for each event type are managed by Apicurio ([AggregatedRecord.avsc](data-lakehouse/apps/gov-aggregator/src/main/avro/AggregatedRecord.avsc), etc.).
3. **Gov Aggregator**: [`App.java`](data-lakehouse/apps/gov-aggregator/src/main/java/et/gov/lakehouse/govaggregator/core/App.java) consumes multiple Kafka topics, transforms records using per-source tasks ([MorTask.java](data-lakehouse/apps/gov-aggregator/src/main/java/et/gov/lakehouse/govaggregator/source/mor/MorTask.java), etc.), and writes unified Avro records to an output topic.
4. **Lakehouse Storage**: Spark jobs (e.g., [`bronze_to_silver_customers.py`](data-lakehouse/scripts/jobs/bronze_to_silver_customers.py)) read raw data from Ozone S3, clean/transform, and write to Iceberg tables.
5. **Analytics**: Trino queries Iceberg tables; Superset provides dashboards and visualization.
6. **Reverse Proxy/API**: Nginx exposes Superset and custom APIs (e.g., phi3 chat).

7. **Security & Governance**: Apache Ranger enforces fine-grained access control, auditing, and policy management across Hive, Trino, and other integrated services.

---

## Configuration

- **Kafka Topics**: Defined in [`Topics.java`](data-lakehouse/apps/gov-aggregator/src/main/java/et/gov/lakehouse/govaggregator/common/Topics.java) and `application.properties`.
- **Avro Schemas**: Located in [`src/main/avro/`](data-lakehouse/apps/gov-aggregator/src/main/avro/).
- **SerDe Wiring**: See [`SerdeFactory.java`](data-lakehouse/apps/gov-aggregator/src/main/java/et/gov/lakehouse/govaggregator/common/SerdeFactory.java).
- **Superset**: Configured via [`superset_config.py`](data-lakehouse-ops/superset/config/superset_config.py).
- **Trino**: Catalogs and node config in [`trino/etc/`](data-lakehouse/trino/etc/).
- **Ozone**: S3 gateway and replication in [`ozone-site.xml`](data-lakehouse/ozone/ozone-site.xml).
 - **Ranger**: Security policies, plugins, and audits configured in [`ranger/`](ranger/), with plugin integration for Hive, Trino, and other services.

---

## Extending the Platform

- **Add new event sources**: Define Avro schema, update gov-aggregator tasks, and configure new Kafka topics.
- **ETL Jobs**: Place Spark/PySpark jobs in [`scripts/jobs/`](data-lakehouse/scripts/jobs/).
- **Analytics**: Use Trino SQL or Superset dashboards for querying and visualization.
- **Custom APIs**: Extend Nginx config and add FastAPI/Flask apps as needed.

- **Security Policies**: Define and manage fine-grained access control and audit policies in Ranger for new data sources and services.

---

## Security & Production Notes

- This MVP is designed for local/dev use. For production:
  - Harden secrets and credentials.
  - Use persistent storage for all volumes.
  - Secure network endpoints.
  - Scale out components as needed.

- Integrate Apache Ranger with all supported services for centralized authorization and auditing.

---

## References

- [Apache Ozone](https://ozone.apache.org/)
- [Apache Kafka](https://kafka.apache.org/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Trino](https://trino.io/)
- [Apache Hive](https://hive.apache.org/)
- [Apache Superset](https://superset.apache.org/)
- [Apicurio Registry](https://www.apicur.io/registry/)
- [Debezium](https://debezium.io/)

- [Apache Ranger](https://ranger.apache.org/)

---

## License

This repository is provided as an MVP template. Please review and comply with the licenses of all included open-source components.
This architecture template was authored by Daniel Fikre.
