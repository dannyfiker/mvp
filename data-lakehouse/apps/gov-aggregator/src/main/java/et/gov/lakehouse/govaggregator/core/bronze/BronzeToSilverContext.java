package et.gov.lakehouse.govaggregator.core.bronze;

import et.gov.lakehouse.govaggregator.common.SerdeFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/** Runtime config + shared SerDes for bronze->silver tasks. */
public final class BronzeToSilverContext {

    public final Properties streamsProps;

    public final List<String> bronzeTopics;

    public final String silverTopicPrefix;
    public final String silverStripPrefix;
    public final String silverNameStyle;

    public final String registryUrl;
    public final String bronzeGroupId;
    public final String silverGroupId;

    public final String icebergNamespace;

    public final String silverRecordNamespace;

    public final boolean approved;

    public final Serde<byte[]> keySerde;
    public final Serde<GenericRecord> bronzeValueSerde;
    public final Serde<GenericRecord> silverValueSerde;

    private BronzeToSilverContext(
            Properties streamsProps,
            List<String> bronzeTopics,
            String silverTopicPrefix,
            String silverStripPrefix,
            String silverNameStyle,
            String registryUrl,
            String bronzeGroupId,
            String silverGroupId,
            String icebergNamespace,
            String silverRecordNamespace,
            boolean approved,
            Serde<byte[]> keySerde,
            Serde<GenericRecord> bronzeValueSerde,
            Serde<GenericRecord> silverValueSerde
    ) {
        this.streamsProps = streamsProps;
        this.bronzeTopics = bronzeTopics;
        this.silverTopicPrefix = silverTopicPrefix;
        this.silverStripPrefix = silverStripPrefix;
        this.silverNameStyle = silverNameStyle;
        this.registryUrl = registryUrl;
        this.bronzeGroupId = bronzeGroupId;
        this.silverGroupId = silverGroupId;
        this.icebergNamespace = icebergNamespace;
        this.silverRecordNamespace = silverRecordNamespace;
        this.approved = approved;
        this.keySerde = keySerde;
        this.bronzeValueSerde = bronzeValueSerde;
        this.silverValueSerde = silverValueSerde;
    }

    public static BronzeToSilverContext fromEnv() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG,
                sysOrEnv("application.id", "APPLICATION_ID", "debezium-to-silver"));
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                sysOrEnv("bootstrap.servers", "BOOTSTRAP_SERVERS", "kafka:9092"));
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                sysOrEnv("auto.offset.reset", "AUTO_OFFSET_RESET", "earliest"));

        // Keep it simple: no state stores, no EOS required
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                sysOrEnv("processing.guarantee", "PROCESSING_GUARANTEE", "at_least_once"));

        String bronzeTopicsCsv = sysOrEnv("bronze.topics", "BRONZE_TOPICS", "");
        List<String> bronzeTopics = parseCsv(bronzeTopicsCsv);
        if (bronzeTopics.isEmpty()) {
            throw new IllegalArgumentException("No input topics configured. Set BRONZE_TOPICS=topic1,topic2,...");
        }

        String silverTopicPrefix = sysOrEnv("silver.topic.prefix", "SILVER_TOPIC_PREFIX", "silver.");
        String silverStripPrefix = sysOrEnv("silver.strip.prefix", "SILVER_STRIP_PREFIX", "");
        String silverNameStyle = sysOrEnv("silver.name.style", "SILVER_NAME_STYLE", "full");

        String registryUrl = sysOrEnv("apicurio.registry.url", "APICURIO_URL", "http://apicurio:8080/apis/registry/v2");

        String bronzeGroupId = sysOrEnv(
                "apicurio.registry.group.bronze",
                "BRONZE_APICURIO_GROUP_ID",
                sysOrEnv("apicurio.registry.group", "APICURIO_GROUP_ID", "debezium")
        );

        String silverGroupId = sysOrEnv(
                "apicurio.registry.group.silver",
                "SILVER_APICURIO_GROUP_ID",
                bronzeGroupId + "-silver"
        );

        String icebergNamespace = sysOrEnv("iceberg.namespace", "ICEBERG_NAMESPACE", "silver");
        String silverRecordNamespace = sysOrEnv("silver.record.namespace", "SILVER_RECORD_NAMESPACE", "silver.oracle_esw");

        boolean approved = isTruthy(sysOrEnv("silver.approved", "SILVER_APPROVED", "false"));

        Serde<byte[]> keySerde = Serdes.ByteArray();
        Serde<GenericRecord> bronzeValueSerde = SerdeFactory.avroSerde(registryUrl, bronzeGroupId, true);
        Serde<GenericRecord> silverValueSerde = SerdeFactory.avroSerde(registryUrl, silverGroupId, true);

        return new BronzeToSilverContext(
                p,
                bronzeTopics,
                silverTopicPrefix,
                silverStripPrefix,
                silverNameStyle,
                registryUrl,
                bronzeGroupId,
                silverGroupId,
                icebergNamespace,
                silverRecordNamespace,
                approved,
                keySerde,
                bronzeValueSerde,
                silverValueSerde
        );
    }

    private static boolean isTruthy(String v) {
        return v != null && ("true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v) || "1".equals(v));
    }

    private static String sysOrEnv(String sysKey, String envKey, String defVal) {
        String v = System.getProperty(sysKey);
        if (v != null && !v.isBlank()) return v;
        v = System.getenv(envKey);
        return (v != null && !v.isBlank()) ? v : defVal;
    }

    private static List<String> parseCsv(String csv) {
        if (csv == null || csv.isBlank()) return List.of();
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toList());
    }
}
