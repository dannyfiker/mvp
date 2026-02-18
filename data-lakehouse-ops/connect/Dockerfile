# Build a Debezium Kafka Connect image with the Tabular Iceberg sink connector plugin installed.
#
# The requested connector class is:
#   io.tabular.iceberg.connect.IcebergSinkConnector
#
# We install it from the published runtime ZIP on GitHub Releases.

FROM alpine:3.20 AS plugin
WORKDIR /plugin

ARG ICEBERG_KAFKA_CONNECT_VERSION=0.6.19
ARG ICEBERG_KAFKA_CONNECT_DIST=runtime

RUN apk add --no-cache curl unzip ca-certificates \
  && update-ca-certificates

RUN curl -fsSLo connector.zip \
      "https://github.com/databricks/iceberg-kafka-connect/releases/download/v${ICEBERG_KAFKA_CONNECT_VERSION}/iceberg-kafka-connect-${ICEBERG_KAFKA_CONNECT_DIST}-${ICEBERG_KAFKA_CONNECT_VERSION}.zip" \
  && unzip -q connector.zip -d /plugin/iceberg-kafka-connect \
  && rm connector.zip

FROM debezium/connect:2.7.3.Final

# Kafka Connect scans plugin.path (/kafka/connect) for plugins.
COPY --from=plugin /plugin/iceberg-kafka-connect /kafka/connect/iceberg-kafka-connect
