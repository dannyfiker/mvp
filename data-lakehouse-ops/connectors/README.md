# Connectors runbook (Oracle ESW → silver → Iceberg)

This folder contains Kafka Connect connector configs used by the lakehouse MVP.

## Run order (recommended)

1) Build the `gov-aggregator` shaded JAR (Connect mounts it for the Apicurio Avro converter implementation):

```bash
cd data-lakehouse/apps/gov-aggregator
mvn -DskipTests package
```

2) Start the stack:

```bash
cd data-lakehouse-ops
docker compose up --build -d
```

Notes:
- The custom Connect image build downloads the Tabular Iceberg sink runtime ZIP (large, ~135MB).
- The Streams app `debezium-to-silver` is configured for Oracle ESW and is already approved (`SILVER_APPROVED=true`).

3) Register the connectors (idempotent):

```bash
# Run from the repo root (or adjust paths accordingly)
cd ..
./data-lakehouse/scripts/connectors/register_oracle_esw_silver_and_iceberg.sh
```

### Oracle credentials (common failure: ORA-01017)

If the Oracle source connector `oracle-esw` shows `FAILED` with `ORA-01017: invalid credential`, the username/password in the connector config do not match the Oracle database.

You can override the Oracle connection settings at registration time (without editing the committed JSON) by exporting env vars before running the registration script:

```bash
export ORACLE_URL='jdbc:oracle:thin:@//host.docker.internal:1521/free'
export ORACLE_USER='ESW_READONLY'
export ORACLE_PASSWORD='***'

# Optional:
export ORACLE_DBNAME='FREE'
export ORACLE_PDB_NAME='ESWTEST'

./data-lakehouse/scripts/connectors/register_oracle_esw_silver_and_iceberg.sh
```

Then confirm:

```bash
curl -fsS http://localhost:8083/connectors/oracle-esw/tasks/0/status | jq
```

Alternative (manual) registration:

```bash
# Oracle Debezium source
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data-binary @data-lakehouse-ops/connectors/oracle-esw.json

# Tabular Iceberg sink (silver.* → Iceberg)
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data-binary @data-lakehouse-ops/connectors/iceberg-sink-oracle-esw.json
```

## Quick health checks

- Connect is up:

```bash
curl -fsS http://localhost:8083/ | head
curl -fsS http://localhost:8083/connectors | cat
```

- Tabular Iceberg sink plugin is visible to Connect:

```bash
curl -fsS http://localhost:8083/connector-plugins | grep -F "io.tabular.iceberg.connect.IcebergSinkConnector" || true
```

- Silver topics are being produced (Kafka UI):
  - Kafka UI: http://localhost:8080
  - Look for: `silver.oracle_esw.TB_CB_LPCO` (and the other `silver.oracle_esw.*` topics)

## Confirm Iceberg tables in Trino

Trino is exposed on http://localhost:8085.

Use the Trino CLI inside the container:

```bash
# List tables created under the `silver` schema
docker exec -it mvp-trino trino --server http://localhost:8080 --catalog iceberg --schema silver -e "SHOW TABLES"

# Describe one of the tables
docker exec -it mvp-trino trino --server http://localhost:8080 --catalog iceberg --schema silver -e "DESCRIBE tb_cb_lpco"

# Sample query
docker exec -it mvp-trino trino --server http://localhost:8080 --catalog iceberg --schema silver -e "SELECT * FROM tb_cb_lpco LIMIT 10"
```

Expected tables (auto-created by the Iceberg sink via `__iceberg_table` routing):
- `silver.tb_cb_lpco`
- `silver.tb_cb_lpco_amdt_attch_doc`
- `silver.tb_cb_lpco_attch_doc`
- `silver.tb_cb_lpco_cmdt`
- `silver.tb_cb_lpco_cmnt`
- `silver.tb_cb_lpco_cncl_attch_doc`
- `silver.tb_cb_lpco_cstms`
- `silver.tb_cb_lpco_mpng`

## If tables don’t show up

- Ensure the source connector is running and producing `raw-*` topics.
- Ensure `mvp-debezium-to-silver` is running and `SILVER_APPROVED=true`.
- Ensure the sink connector status is `RUNNING`:

```bash
curl -fsS http://localhost:8083/connectors/iceberg-sink-oracle-esw-silver/status | cat
```
