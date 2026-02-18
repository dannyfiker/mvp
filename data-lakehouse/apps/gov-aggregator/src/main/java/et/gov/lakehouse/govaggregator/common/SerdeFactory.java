package et.gov.lakehouse.govaggregator.common;

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.apicurio.registry.serde.avro.AvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

/** Apicurio Avro serdes without compile-time config deps. */
public final class SerdeFactory {
    private SerdeFactory() {}

    // Apicurio config keys as strings (stable across minor versions)
    private static final String REGISTRY_URL      = "apicurio.registry.url";
    private static final String AUTO_REGISTER     = "apicurio.registry.auto-register";
    private static final String FIND_LATEST       = "apicurio.registry.find-latest";
    private static final String ARTIFACT_GROUP_ID = "apicurio.registry.artifact.group-id";

    // When Kafka messages are produced using Apicurio's AvroConverter in Confluent-compat mode
    // (magic byte + 4-byte id), the Streams SerDe must also be configured for Confluent mode.
    private static final String AS_CONFLUENT      = "apicurio.registry.as-confluent";
    private static final String USE_ID            = "apicurio.registry.use-id";
    private static final String ID_HANDLER        = "apicurio.registry.id-handler";
    private static final String HEADERS_ENABLED   = "apicurio.registry.headers.enabled";

    // This stack uses Apicurio's Confluent compatibility mode with Legacy4ByteIdHandler + contentId.
    // The 4-byte schema id in the Kafka message is the Apicurio *contentId*.
    private static final String DEFAULT_USE_ID_CONTENT_ID = "contentId";
    private static final String DEFAULT_LEGACY_4BYTE_ID_HANDLER = "io.apicurio.registry.serde.Legacy4ByteIdHandler";

    private static Map<String, Object> baseConfig(String registryUrl, String groupId) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(REGISTRY_URL, registryUrl);
        cfg.put(AUTO_REGISTER, true);
        cfg.put(FIND_LATEST, true);
        if (groupId != null && !groupId.isBlank()) {
            cfg.put(ARTIFACT_GROUP_ID, groupId);
        }
        return cfg;
    }

    private static Map<String, Object> baseConfig(
            String registryUrl,
            String groupId,
            boolean asConfluent
    ) {
        Map<String, Object> cfg = baseConfig(registryUrl, groupId);
        if (asConfluent) {
            cfg.put(AS_CONFLUENT, true);
            cfg.put(USE_ID, DEFAULT_USE_ID_CONTENT_ID);
            cfg.put(ID_HANDLER, DEFAULT_LEGACY_4BYTE_ID_HANDLER);
            cfg.put(HEADERS_ENABLED, false);
        }
        return cfg;
    }

    /** Alias used by your App.java */
    public static Serde<String> stringSerde() {
        return Serdes.String();
    }

    /** Generic Avro Serde for Streams (no class needed). */
    public static <T> Serde<T> avroSerde(String registryUrl, String groupId) {
        return avroSerde(registryUrl, groupId, false);
    }

    /**
     * Generic Avro Serde for Streams.
     * Set asConfluent=true when consuming/producing topics written by Apicurio AvroConverter
     * with confluent-compat framing.
     */
    public static <T> Serde<T> avroSerde(String registryUrl, String groupId, boolean asConfluent) {
        AvroSerde<T> serde = new AvroSerde<>();
        serde.configure(baseConfig(registryUrl, groupId, asConfluent), false);
        return serde;
    }

    /** Overload to match existing call sites that pass a Class<T>. */
    public static <T> Serde<T> avroSerde(Class<T> ignored, String registryUrl, String groupId) {
        return avroSerde(registryUrl, groupId);
    }

    /** Optional helpers for plain Producer/Consumer (non-Streams). */
    public static AvroKafkaSerializer avroValueSerializer(String registryUrl, String groupId) {
        AvroKafkaSerializer ser = new AvroKafkaSerializer();
        ser.configure(baseConfig(registryUrl, groupId), false);
        return ser;
    }

    public static AvroKafkaDeserializer avroValueDeserializer(String registryUrl, String groupId) {
        AvroKafkaDeserializer de = new AvroKafkaDeserializer();
        de.configure(baseConfig(registryUrl, groupId), false);
        return de;
    }
}
