#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/Lake/mvp}"

echo ">> Using ROOT=$ROOT"

# 0) Ensure base dirs exist
mkdir -p "$ROOT/data-lakehouse/apps"
mkdir -p "$ROOT/data-lakehouse-ops"

APP_DIR="$ROOT/data-lakehouse/apps/gov-aggregator"
SRC_MAIN="$APP_DIR/src/main"
JAVA_DIR="$SRC_MAIN/java/et/gov/lakehouse/govaggregator"
RES_DIR="$SRC_MAIN/resources"
AVRO_DIR="$SRC_MAIN/avro"

# 1) Maven skeleton
echo ">> Creating app skeleton at $APP_DIR"
mkdir -p "$JAVA_DIR"/{core,common,source/mor,source/ecc,source/motri,source/nbe,source/moe}
mkdir -p "$RES_DIR" "$AVRO_DIR"

# 2) .gitignore
cat > "$APP_DIR/.gitignore" <<'GIT'
target/
.idea/
*.iml
GIT

# 3) pom.xml
cat > "$APP_DIR/pom.xml" <<'POM'
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                             https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>et.gov.lakehouse</groupId>
  <artifactId>gov-aggregator</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <name>gov-aggregator</name>

  <properties>
    <maven.compiler.source>17</maven.compiler.source>
    <maven.compiler.target>17</maven.compiler.target>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <kafka.streams.version>3.7.1</kafka.streams.version>
    <avro.version>1.11.3</avro.version>
    <apicurio.serdes.version>3.0.5.Final</apicurio.serdes.version>
    <slf4j.version>2.0.13</slf4j.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-streams</artifactId>
      <version>${kafka.streams.version}</version>
    </dependency>

    <dependency>
      <groupId>io.apicurio</groupId>
      <artifactId>apicurio-registry-serdes-avro-serde</artifactId>
      <version>${apicurio.serdes.version}</version>
    </dependency>

    <dependency>
      <groupId>org.apache.avro</groupId>
      <artifactId>avro</artifactId>
      <version>${avro.version}</version>
    </dependency>

    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-simple</artifactId>
      <version>${slf4j.version}</version>
      <scope>runtime</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.2</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <createDependencyReducedPom>false</createDependencyReducedPom>
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>et.gov.lakehouse.govaggregator.core.App</mainClass>
                </transformer>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>

      <plugin>
        <groupId>org.apache.avro</groupId>
        <artifactId>avro-maven-plugin</artifactId>
        <version>${avro.version}</version>
        <executions>
          <execution>
            <phase>generate-sources</phase>
            <goals><goal>schema</goal></goals>
            <configuration>
              <sourceDirectory>${project.basedir}/src/main/avro</sourceDirectory>
              <outputDirectory>${project.build.directory}/generated-sources/avro</outputDirectory>
              <stringType>String</stringType>
              <enableDecimalLogicalType>true</enableDecimalLogicalType>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
POM

# 4) Avro schemas
cat > "$AVRO_DIR/AggregatedRecord.avsc" <<'AVSC'
{
  "type": "record",
  "namespace": "et.gov.lakehouse.govaggregator.avro",
  "name": "AggregatedRecord",
  "fields": [
    {"name":"sourceSystem","type":"string"},
    {"name":"eventType","type":"string"},
    {"name":"eventTime","type":{"type":"long","logicalType":"timestamp-millis"}},
    {"name":"key","type":"string"},
    {"name":"payload","type":"string"}
  ]
}
AVSC

cat > "$AVRO_DIR/MorTaxPayment.avsc" <<'AVSC'
{
  "type": "record",
  "namespace": "et.gov.lakehouse.govaggregator.avro",
  "name": "MorTaxPayment",
  "fields": [
    {"name":"paymentId","type":"string"},
    {"name":"tin","type":"string"},
    {"name":"amount","type":"double"},
    {"name":"currency","type":"string"},
    {"name":"paidAt","type":{"type":"long","logicalType":"timestamp-millis"}}
  ]
}
AVSC

cat > "$AVRO_DIR/EccTradePermit.avsc" <<'AVSC'
{
  "type": "record",
  "namespace": "et.gov.lakehouse.govaggregator.avro",
  "name": "EccTradePermit",
  "fields": [
    {"name":"permitId","type":"string"},
    {"name":"companyName","type":"string"},
    {"name":"commodity","type":"string"},
    {"name":"valueUsd","type":"double"},
    {"name":"issuedAt","type":{"type":"long","logicalType":"timestamp-millis"}}
  ]
}
AVSC

cat > "$AVRO_DIR/MotriTransportPermit.avsc" <<'AVSC'
{
  "type": "record",
  "namespace": "et.gov.lakehouse.govaggregator.avro",
  "name": "MotriTransportPermit",
  "fields": [
    {"name":"permitNo","type":"string"},
    {"name":"vehiclePlate","type":"string"},
    {"name":"route","type":"string"},
    {"name":"validUntil","type":{"type":"long","logicalType":"timestamp-millis"}}
  ]
}
AVSC

cat > "$AVRO_DIR/NbeFxRate.avsc" <<'AVSC'
{
  "type": "record",
  "namespace": "et.gov.lakehouse.govaggregator.avro",
  "name": "NbeFxRate",
  "fields": [
    {"name":"asOf","type":{"type":"long","logicalType":"timestamp-millis"}},
    {"name":"pair","type":"string"},
    {"name":"rate","type":"double"}
  ]
}
AVSC

cat > "$AVRO_DIR/MoeEducationStat.avsc" <<'AVSC'
{
  "type": "record",
  "namespace": "et.gov.lakehouse.govaggregator.avro",
  "name": "MoeEducationStat",
  "fields": [
    {"name":"schoolId","type":"string"},
    {"name":"region","type":"string"},
    {"name":"students","type":"int"},
    {"name":"teachers","type":"int"},
    {"name":"reportedAt","type":{"type":"long","logicalType":"timestamp-millis"}}
  ]
}
AVSC

# 5) application.properties
cat > "$RES_DIR/application.properties" <<'PROPS'
app.name=gov-aggregator
bootstrap.servers=kafka:9092

application.id=gov-aggregator-app
processing.guarantee=at_least_once
replication.factor=1
num.stream.threads=1
cache.max.bytes.buffering=10485760

registry.url=http://apicurio:8080/apis/registry/v2
registry.auto-register=true
registry.find-latest=true

topic.mor=mor.tax.payments
topic.ecc=ecc.trade.permits
topic.motri=motri.transport.permits
topic.nbe=nbe.fx.rates
topic.moe=moe.education.stats
topic.out=gov.aggregates.enriched
PROPS

# 6) Common code
cat > "$JAVA_DIR/common/Topics.java" <<'JAVA'
package et.gov.lakehouse.govaggregator.common;

public final class Topics {
    public static final String MOR = System.getProperty("topic.mor", System.getenv().getOrDefault("TOPIC_MOR","mor.tax.payments"));
    public static final String ECC = System.getProperty("topic.ecc", System.getenv().getOrDefault("TOPIC_ECC","ecc.trade.permits"));
    public static final String MOTRI = System.getProperty("topic.motri", System.getenv().getOrDefault("TOPIC_MOTRI","motri.transport.permits"));
    public static final String NBE = System.getProperty("topic.nbe", System.getenv().getOrDefault("TOPIC_NBE","nbe.fx.rates"));
    public static final String MOE = System.getProperty("topic.moe", System.getenv().getOrDefault("TOPIC_MOE","moe.education.stats"));
    public static final String OUT = System.getProperty("topic.out", System.getenv().getOrDefault("TOPIC_OUT","gov.aggregates.enriched"));
    private Topics() {}
}
JAVA

cat > "$JAVA_DIR/common/SerdeFactory.java" <<'JAVA'
package et.gov.lakehouse.govaggregator.common;

import io.apicurio.registry.serde.SchemaResolverConfig;
import io.apicurio.registry.serde.avro.AvroSerde;
import io.apicurio.registry.serde.config.IdOption;
import io.apicurio.registry.serde.config.SerdeConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

public final class SerdeFactory {
    public static <T> Serde<T> avroSerde(Class<T> cls, boolean isKey) {
        AvroSerde<T> serde = new AvroSerde<>();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(SerdeConfig.REGISTRY_URL, System.getProperty("registry.url", System.getenv().getOrDefault("REGISTRY_URL","http://apicurio:8080/apis/registry/v2")));
        cfg.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, Boolean.parseBoolean(System.getProperty("registry.auto-register","true")));
        cfg.put(SerdeConfig.FIND_LATEST_ARTIFACT, Boolean.parseBoolean(System.getProperty("registry.find-latest","true")));
        cfg.put(SerdeConfig.EXPLICIT_ARTIFACT_GROUP_ID, "gov-aggregator");
        cfg.put(SerdeConfig.USE_ID, IdOption.contentId.name());
        cfg.put(SchemaResolverConfig.ARTIFACT_RESOLVER_STRATEGY, "io.apicurio.registry.serde.strategy.SimpleTopicIdStrategy");
        serde.configure(cfg, isKey);
        return serde;
    }
    public static Serde<String> stringSerde() { return Serdes.String(); }
    private SerdeFactory() {}
}
JAVA

# 7) Source tasks
cat > "$JAVA_DIR/source/mor/MorTask.java" <<'JAVA'
package et.gov.lakehouse.govaggregator.source.mor;

import et.gov.lakehouse.govaggregator.avro.AggregatedRecord;
import et.gov.lakehouse.govaggregator.avro.MorTaxPayment;
import org.apache.kafka.streams.kstream.KStream;

public final class MorTask {
    public static KStream<String, AggregatedRecord> build(KStream<String, MorTaxPayment> in) {
        return in.mapValues(v -> new AggregatedRecord("MoR","tax_payment", v.getPaidAt(), v.getPaymentId(),
                "{\"tin\":\""+v.getTin()+"\",\"amount\":"+v.getAmount()+",\"currency\":\""+v.getCurrency()+"\"}"));
    }
    private MorTask() {}
}
JAVA

cat > "$JAVA_DIR/source/ecc/EccTask.java" <<'JAVA'
package et.gov.lakehouse.govaggregator.source.ecc;

import et.gov.lakehouse.govaggregator.avro.AggregatedRecord;
import et.gov.lakehouse.govaggregator.avro.EccTradePermit;
import org.apache.kafka.streams.kstream.KStream;

public final class EccTask {
    public static KStream<String, AggregatedRecord> build(KStream<String, EccTradePermit> in) {
        return in.mapValues(v -> new AggregatedRecord("ECC","trade_permit", v.getIssuedAt(), v.getPermitId(),
                "{\"company\":\""+v.getCompanyName()+"\",\"commodity\":\""+v.getCommodity()+"\",\"valueUsd\":"+v.getValueUsd()+"}"));
    }
    private EccTask() {}
}
JAVA

cat > "$JAVA_DIR/source/motri/MotriTask.java" <<'JAVA'
package et.gov.lakehouse.govaggregator.source.motri;

import et.gov.lakehouse.govaggregator.avro.AggregatedRecord;
import et.gov.lakehouse.govaggregator.avro.MotriTransportPermit;
import org.apache.kafka.streams.kstream.KStream;

public final class MotriTask {
    public static KStream<String, AggregatedRecord> build(KStream<String, MotriTransportPermit> in) {
        return in.mapValues(v -> new AggregatedRecord("MoTRI","transport_permit", v.getValidUntil(), v.getPermitNo(),
                "{\"plate\":\""+v.getVehiclePlate()+"\",\"route\":\""+v.getRoute()+"\"}"));
    }
    private MotriTask() {}
}
JAVA

cat > "$JAVA_DIR/source/nbe/NbeTask.java" <<'JAVA'
package et.gov.lakehouse.govaggregator.source.nbe;

import et.gov.lakehouse.govaggregator.avro.AggregatedRecord;
import et.gov.lakehouse.govaggregator.avro.NbeFxRate;
import org.apache.kafka.streams.kstream.KStream;

public final class NbeTask {
    public static KStream<String, AggregatedRecord> build(KStream<String, NbeFxRate> in) {
        return in.mapValues(v -> new AggregatedRecord("NBE","fx_rate", v.getAsOf(), v.getPair(),
                "{\"rate\":"+v.getRate()+"}"));
    }
    private NbeTask() {}
}
JAVA

cat > "$JAVA_DIR/source/moe/MoeTask.java" <<'JAVA'
package et.gov.lakehouse.govaggregator.source.moe;

import et.gov.lakehouse.govaggregator.avro.AggregatedRecord;
import et.gov.lakehouse.govaggregator.avro.MoeEducationStat;
import org.apache.kafka.streams.kstream.KStream;

public final class MoeTask {
    public static KStream<String, AggregatedRecord> build(KStream<String, MoeEducationStat> in) {
        return in.mapValues(v -> new AggregatedRecord("MoE","education_stat", v.getReportedAt(), v.getSchoolId(),
                "{\"region\":\""+v.getRegion()+"\",\"students\":"+v.getStudents()+",\"teachers\":"+v.getTeachers()+"}"));
    }
    private MoeTask() {}
}
JAVA

# 8) App (Topology)
cat > "$JAVA_DIR/core/App.java" <<'JAVA'
package et.gov.lakehouse.govaggregator.core;

import et.gov.lakehouse.govaggregator.avro.*;
import et.gov.lakehouse.govaggregator.common.SerdeFactory;
import et.gov.lakehouse.govaggregator.common.Topics;
import et.gov.lakehouse.govaggregator.source.mor.MorTask;
import et.gov.lakehouse.govaggregator.source.ecc.EccTask;
import et.gov.lakehouse.govaggregator.source.motri.MotriTask;
import et.gov.lakehouse.govaggregator.source.nbe.NbeTask;
import et.gov.lakehouse.govaggregator.source.moe.MoeTask;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public final class App {
    public static void main(String[] args) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, System.getProperty("application.id","gov-aggregator-app"));
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("bootstrap.servers","kafka:9092"));
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, System.getProperty("processing.guarantee","at_least_once"));
        p.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, System.getProperty("cache.max.bytes.buffering","10485760"));
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "5000");

        StreamsBuilder b = new StreamsBuilder();

        Serde<String> stringSerde = SerdeFactory.stringSerde();
        Serde<AggregatedRecord> outSerde = SerdeFactory.avroSerde(AggregatedRecord.class, false);

        Serde<MorTaxPayment> morSerde = SerdeFactory.avroSerde(MorTaxPayment.class, false);
        Serde<EccTradePermit> eccSerde = SerdeFactory.avroSerde(EccTradePermit.class, false);
        Serde<MotriTransportPermit> motriSerde = SerdeFactory.avroSerde(MotriTransportPermit.class, false);
        Serde<NbeFxRate> nbeSerde = SerdeFactory.avroSerde(NbeFxRate.class, false);
        Serde<MoeEducationStat> moeSerde = SerdeFactory.avroSerde(MoeEducationStat.class, false);

        KStream<String, MorTaxPayment> mor = b.stream(Topics.MOR, Consumed.with(stringSerde, morSerde));
        KStream<String, EccTradePermit> ecc = b.stream(Topics.ECC, Consumed.with(stringSerde, eccSerde));
        KStream<String, MotriTransportPermit> motri = b.stream(Topics.MOTRI, Consumed.with(stringSerde, motriSerde));
        KStream<String, NbeFxRate> nbe = b.stream(Topics.NBE, Consumed.with(stringSerde, nbeSerde));
        KStream<String, MoeEducationStat> moe = b.stream(Topics.MOE, Consumed.with(stringSerde, moeSerde));

        KStream<String, AggregatedRecord> morAgg  = MorTask.build(mor);
        KStream<String, AggregatedRecord> eccAgg  = EccTask.build(ecc);
        KStream<String, AggregatedRecord> motriAgg= MotriTask.build(motri);
        KStream<String, AggregatedRecord> nbeAgg  = NbeTask.build(nbe);
        KStream<String, AggregatedRecord> moeAgg  = MoeTask.build(moe);

        KStream<String, AggregatedRecord> unified = morAgg.merge(eccAgg).merge(motriAgg).merge(nbeAgg).merge(moeAgg);
        unified.to(Topics.OUT, Produced.with(stringSerde, outSerde));

        Topology topology = b.build();
        KafkaStreams streams = new KafkaStreams(topology, p);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
JAVA

# 9) Dockerfile
cat > "$APP_DIR/Dockerfile" <<'DOCKER'
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q -e -U -B dependency:go-offline
COPY src ./src
RUN mvn -q -e -B -DskipTests package

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/target/gov-aggregator-*-SNAPSHOT.jar /app/app.jar
ENV JAVA_OPTS="-Xms256m -Xmx512m"
ENTRYPOINT ["sh","-lc","exec java $JAVA_OPTS -jar /app/app.jar"]
DOCKER

# 10) Compose override (Apicurio, Superset, gov-aggregator)
cat > "$ROOT/data-lakehouse-ops/docker-compose.override.yml" <<'YML'
services:
  apicurio:
    image: apicurio/apicurio-registry-kafkasql:latest
    container_name: mvp-apicurio
    depends_on:
      - kafka
    environment:
      QUARKUS_PROFILE: prod
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_SECURITY_PROTOCOL: PLAINTEXT
      APPLICATION_SERVER: http://apicurio:8080
    ports:
      - "8081:8080"
    restart: unless-stopped

  superset-db:
    image: postgres:16-alpine
    container_name: mvp-superset-db
    environment:
      POSTGRES_DB: superset
      POSTGRES_USER: superset
      POSTGRES_PASSWORD: superset
    volumes:
      - superset_db:/var/lib/postgresql/data
    restart: unless-stopped

  superset:
    image: apache/superset:latest
    container_name: mvp-superset
    depends_on:
      - superset-db
    environment:
      SUPERSET_SECRET_KEY: "change-me-please-very-long"
      SUPERSET_DATABASE_URI: postgresql+psycopg2://superset:superset@superset-db:5432/superset
    ports:
      - "8088:8088"
    command:
      [
        "/bin/bash","-lc",
        "superset db upgrade && \
         superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin || true && \
         superset init && \
         gunicorn --workers 2 --timeout 120 -b 0.0.0.0:8088 'superset.app:create_app()'"
      ]
    restart: unless-stopped

  gov-aggregator:
    build:
      context: ../data-lakehouse/apps/gov-aggregator
      dockerfile: Dockerfile
    container_name: mvp-gov-aggregator
    depends_on:
      - kafka
      - apicurio
    environment:
      bootstrap.servers: kafka:9092
      registry.url: http://apicurio:8080/apis/registry/v2
      registry.auto-register: "true"
      registry.find-latest: "true"
      topic.mor: mor.tax.payments
      topic.ecc: ecc.trade.permits
      topic.motri: motri.transport.permits
      topic.nbe: nbe.fx.rates
      topic.moe: moe.education.stats
      topic.out: gov.aggregates.enriched
    restart: unless-stopped

volumes:
  superset_db:
YML

# 11) README quickstart
cat > "$APP_DIR/README.md" <<'MD'
# gov-aggregator (Kafka Streams + Apicurio Avro)

## Build
```bash
cd ~/Lake/mvp/data-lakehouse/apps/gov-aggregator
mvn -DskipTests package
