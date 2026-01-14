import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr


def env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value is not None and value != "" else default


KAFKA_BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_PATTERN = env("TOPIC_PATTERN", r"silver\\..*")
ICEBERG_REST_URI = env("ICEBERG_REST_URI", "http://iceberg-rest:8181")
WAREHOUSE_SILVER_LOCATION = env("WAREHOUSE_SILVER_LOCATION", "s3://warehouse/silver/")
CHECKPOINT_LOCATION = env("CHECKPOINT_LOCATION", "/tmp/checkpoints/kafka_silver_to_iceberg")
STARTING_OFFSETS = env("STARTING_OFFSETS", "earliest")
EXPECTED_TABLES = [t.strip() for t in env("EXPECTED_TABLES", "accounts,banks,branches,customers,transactions").split(",") if t.strip()]

# Iceberg S3FileIO settings (used for Ozone S3 Gateway)
S3_ENDPOINT = env("S3_ENDPOINT", "http://ozone-s3g:9878")
S3_PATH_STYLE_ACCESS = env("S3_PATH_STYLE_ACCESS", "true")
AWS_ACCESS_KEY_ID = env("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = env("AWS_SECRET_ACCESS_KEY", "admin123")
AWS_REGION = env("AWS_REGION", env("AWS_DEFAULT_REGION", "us-east-1"))

spark = (
    SparkSession.builder.appName("kafka silver -> iceberg")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
    .config("spark.sql.catalog.iceberg.uri", ICEBERG_REST_URI)
    # Use Iceberg's S3 client (works with s3:// URIs; avoids Hadoop s3a scheme mismatch)
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.iceberg.s3.endpoint", S3_ENDPOINT)
    .config("spark.sql.catalog.iceberg.s3.path-style-access", S3_PATH_STYLE_ACCESS)
    .config("spark.sql.catalog.iceberg.s3.access-key-id", AWS_ACCESS_KEY_ID)
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", AWS_SECRET_ACCESS_KEY)
    .config("spark.sql.catalog.iceberg.s3.region", AWS_REGION)
    .getOrCreate()
)

spark.sql(
    f"CREATE DATABASE IF NOT EXISTS iceberg.silver LOCATION '{WAREHOUSE_SILVER_LOCATION}'"
)


def ensure_table_exists(table: str) -> None:
    spark.sql(
        f"""
CREATE TABLE IF NOT EXISTS {table} (
  kafka_topic STRING,
  kafka_partition INT,
  kafka_offset LONG,
  kafka_timestamp TIMESTAMP,
  ingested_at TIMESTAMP,
    kafka_key BINARY,
    payload BINARY
)
USING ICEBERG
"""
    )


def topic_to_table(topic: str) -> str:
    # silver.accounts -> iceberg.silver.accounts
    # If the topic doesn't have a dot, fall back to full name.
    if "." in topic:
        suffix = topic.split(".", 1)[1]
    else:
        suffix = topic

    # Spark SQL identifiers: keep it simple; quote each part.
    return f"iceberg.silver.`{suffix}`"


def ensure_expected_tables_exist() -> None:
    # Create the canonical tables up-front so they show up in Iceberg/Trino
    # even if early micro-batches are empty.
    for suffix in EXPECTED_TABLES:
        ensure_table_exists(f"iceberg.silver.`{suffix}`")


ensure_expected_tables_exist()


raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribePattern", TOPIC_PATTERN)
    .option("startingOffsets", STARTING_OFFSETS)
    .load()
)

# Keep the payload as-is (raw Kafka bytes, typically Avro); this avoids hardcoding schemas per table.
# You can later decode/curate into typed Iceberg tables.
records = (
    raw.select(
        col("topic").alias("kafka_topic"),
        col("partition").cast("int").alias("kafka_partition"),
        col("offset").cast("long").alias("kafka_offset"),
        col("timestamp").cast("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at"),
        col("key").alias("kafka_key"),
        col("value").alias("payload"),
    )
    .where(col("payload").isNotNull())
)


def write_to_iceberg(batch_df, batch_id: int) -> None:
    # Lightweight visibility into whether we are actually consuming data.
    per_topic = batch_df.groupBy("kafka_topic").count().collect()
    if not per_topic:
        print(f"batch_id={batch_id} rows=0 topics=[]")
        return

    print(
        "batch_id="
        + str(batch_id)
        + " topics="
        + str([(r["kafka_topic"], int(r["count"])) for r in per_topic])
    )

    topics = [r[0] for r in batch_df.select("kafka_topic").distinct().collect()]
    for topic in topics:
        table = topic_to_table(topic)
        ensure_table_exists(table)
        (
            batch_df.where(col("kafka_topic") == topic)
            .writeTo(table)
            .append()
        )


query = (
    records.writeStream.foreachBatch(write_to_iceberg)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
