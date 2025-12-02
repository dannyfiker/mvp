from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder
  .appName("bronze->silver customers")
  .config("spark.sql.catalog.iceberg","org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.iceberg.catalog-impl","org.apache.iceberg.rest.RESTCatalog")
  .config("spark.sql.catalog.iceberg.uri","http://iceberg-rest:8181")
  .config("spark.sql.catalog.iceberg.io-impl","org.apache.iceberg.hadoop.HadoopFileIO")
  .getOrCreate())

bronze_path = "s3a://warehouse/bronze/pg/public_customers/*/*/*/*"
df = spark.read.json(bronze_path)

clean = (df
  .select(col("after.id").cast("string").alias("id"),
          col("after.name").alias("name"),
          col("after.email").alias("email"),
          col("ts_ms").cast("timestamp").alias("updated_at"))
  .dropna(subset=["id"]))

spark.sql("""
CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location='s3a://warehouse/silver/')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg.silver.customers
(id string, name string, email string, updated_at timestamp)
USING ICEBERG
""")

clean.writeTo("iceberg.silver.customers").append()
print("Wrote rows:", clean.count())
