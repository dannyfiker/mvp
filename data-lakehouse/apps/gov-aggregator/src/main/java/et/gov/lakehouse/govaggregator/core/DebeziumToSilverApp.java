package et.gov.lakehouse.govaggregator.core;

import et.gov.lakehouse.govaggregator.core.bronze.BronzeToSilverContext;
import et.gov.lakehouse.govaggregator.core.bronze.BronzeToSilverPipeline;
import et.gov.lakehouse.govaggregator.core.bronze.BronzeToSilverTask;
import et.gov.lakehouse.govaggregator.core.bronze.EswBronzeToSilverTask;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Orchestrates per-source "bronze -> silver" tasks for Debezium topics serialized with AvroConverter.
 */
public final class DebeziumToSilverApp {

    private static void sleepForever() {
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        BronzeToSilverContext ctx = BronzeToSilverContext.fromEnv();
        StreamsBuilder b = new StreamsBuilder();

        List<BronzeToSilverTask> tasks = new ArrayList<>();
        tasks.add(new EswBronzeToSilverTask());

        if (!ctx.approved) {
            System.out.println("[debezium-to-silver] SILVER_APPROVED is false; refusing to emit silver topics.");
            System.out.println("[debezium-to-silver] Proposed outputs:");
            for (String inputTopic : ctx.bronzeTopics) {
                String sourceTable = BronzeToSilverPipeline.deriveSourceTableFromTopic(inputTopic, ctx.silverStripPrefix);
                String outputTopic = BronzeToSilverPipeline.silverTopicFor(
                        inputTopic,
                        ctx.silverTopicPrefix,
                        ctx.silverStripPrefix,
                        ctx.silverNameStyle
                );
                String icebergTable = BronzeToSilverPipeline.icebergTableFor(ctx.icebergNamespace, sourceTable);
                System.out.println("  - " + inputTopic + " -> " + outputTopic + " (iceberg table: " + icebergTable + ")");
            }
            System.out.println("[debezium-to-silver] Set SILVER_APPROVED=true after you approve the schema/columns.");
            // Keep container alive (compose restart policy is usually unless-stopped).
            sleepForever();
            return;
        }

        for (BronzeToSilverTask task : tasks) {
            System.out.println("[debezium-to-silver] configuring source task: " + task.source());
            task.configure(b, ctx);
        }

        Topology topology = b.build();
        KafkaStreams streams = new KafkaStreams(topology, ctx.streamsProps);

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
