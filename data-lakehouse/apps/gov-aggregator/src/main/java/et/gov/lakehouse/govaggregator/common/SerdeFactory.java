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

    /** Alias used by your App.java */
    public static Serde<String> stringSerde() {
        return Serdes.String();
    }

    /** Generic Avro Serde for Streams (no class needed). */
    public static <T> Serde<T> avroSerde(String registryUrl, String groupId) {
        AvroSerde<T> serde = new AvroSerde<>();
        serde.configure(baseConfig(registryUrl, groupId), false);
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
