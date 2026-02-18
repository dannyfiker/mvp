package et.gov.lakehouse.govaggregator.core.bronze;

import org.apache.kafka.streams.StreamsBuilder;

/**
 * A per-source Bronze -> Silver wiring unit.
 *
 * Pattern:
 * - One class per source system (e.g. ESW).
 * - A small configure(...) method that calls multiple private configureX(...) methods.
 */
public interface BronzeToSilverTask {
    /** A short stable source id (e.g. "oracle-esw"). */
    String source();

    /** Wire the Kafka Streams topology for this source. */
    void configure(StreamsBuilder builder, BronzeToSilverContext ctx);
}
