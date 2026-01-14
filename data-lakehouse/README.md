
# Data Lakehouse

## Jobs

### Kafka silver topics -> Iceberg tables

Spark Structured Streaming job that consumes all Kafka topics matching `silver.*` and appends into Iceberg tables under `iceberg.silver.<topicSuffix>`.

- Job: [scripts/jobs/kafka_silver_to_iceberg.py](scripts/jobs/kafka_silver_to_iceberg.py)
- Output tables: `iceberg.silver.accounts`, `iceberg.silver.transactions`, ...
- Table schema: Kafka metadata columns + `kafka_key`/`payload` as Avro-encoded bytes (BINARY)

Run inside the Spark container:

- `docker exec -it mvp-spark spark-submit /opt/jobs/kafka_silver_to_iceberg.py`

Optional env vars:

- `KAFKA_BOOTSTRAP_SERVERS` (default `kafka:9092`)
- `TOPIC_PATTERN` (default `silver\\..*`)
- `ICEBERG_REST_URI` (default `http://iceberg-rest:8181`)
- `WAREHOUSE_SILVER_LOCATION` (default `s3a://warehouse/silver/`)
- `CHECKPOINT_LOCATION` (default `/tmp/checkpoints/kafka_silver_to_iceberg`)
- `STARTING_OFFSETS` (`earliest` or `latest`, default `earliest`)

