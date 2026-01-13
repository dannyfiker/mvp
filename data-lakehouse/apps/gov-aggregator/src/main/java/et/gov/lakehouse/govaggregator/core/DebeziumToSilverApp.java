package et.gov.lakehouse.govaggregator.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * Minimal "bronze -> silver" pipeline for Debezium JSON topics.
 *
 * Input: Kafka Connect JsonConverter with schemas disabled (your connect config), producing JSON like:
 *  {"before":..., "after":{...}, "op":"c|u|d|r", ...}
 * or wrapped as {"payload":{...}}.
 *
 * Output: a new topic per input, containing only the "after" JSON object (as a JSON string).
 * Deletes (op=d) are dropped by default because "after" is null.
 */
public final class DebeziumToSilverApp {

    private static final ObjectMapper MAPPER = new ObjectMapper();

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

    private static String silverTopicFor(String inputTopic, String silverPrefix, String stripPrefix) {
        return silverTopicFor(inputTopic, silverPrefix, stripPrefix, "full");
    }

    private static String silverTopicFor(String inputTopic, String silverPrefix, String stripPrefix, String nameStyle) {
        if ("last-segment".equalsIgnoreCase(nameStyle)) {
            int idx = inputTopic.lastIndexOf('.');
            String last = (idx >= 0 && idx + 1 < inputTopic.length()) ? inputTopic.substring(idx + 1) : inputTopic;
            return silverPrefix + last;
        }

        String t = inputTopic;
        if (stripPrefix != null && !stripPrefix.isBlank() && t.startsWith(stripPrefix)) {
            t = t.substring(stripPrefix.length());
        }
        return silverPrefix + t;
    }

    private static String extractAfterJson(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) return null;
        try {
            JsonNode root = MAPPER.readTree(rawJson);
            JsonNode payload = root.has("payload") ? root.get("payload") : root;
            JsonNode after = payload.get("after");
            if (after == null || after.isNull()) return null;
            return MAPPER.writeValueAsString(after);
        } catch (Exception e) {
            // If parsing fails, drop the record (keeps the pipeline resilient)
            return null;
        }
    }

    public static void main(String[] args) {
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

        String silverPrefix = sysOrEnv("silver.topic.prefix", "SILVER_TOPIC_PREFIX", "silver.");
        String stripPrefix = sysOrEnv("silver.strip.prefix", "SILVER_STRIP_PREFIX", "");
        String nameStyle = sysOrEnv("silver.name.style", "SILVER_NAME_STYLE", "full");

        StreamsBuilder b = new StreamsBuilder();

        for (String inputTopic : bronzeTopics) {
            String outputTopic = silverTopicFor(inputTopic, silverPrefix, stripPrefix, nameStyle);

            KStream<String, String> src = b.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
            src.mapValues(DebeziumToSilverApp::extractAfterJson)
               .filter((k, v) -> v != null)
               .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        }

        Topology topology = b.build();
        KafkaStreams streams = new KafkaStreams(topology, p);

        streams.setStateListener((newState, oldState) ->
                System.out.println("[debezium-to-silver] state " + oldState + " -> " + newState));
        streams.setUncaughtExceptionHandler(e -> {
            System.err.println("[debezium-to-silver] Uncaught exception");
            e.printStackTrace(System.err);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        streams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
