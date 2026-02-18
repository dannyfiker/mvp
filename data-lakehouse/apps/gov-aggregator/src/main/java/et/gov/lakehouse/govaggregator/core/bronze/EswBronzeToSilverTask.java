package et.gov.lakehouse.govaggregator.core.bronze;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Set;

/**
 * Oracle ESW Debezium topics (routed to raw-<TABLE>) -> silver.oracle_esw.<TABLE>.
 *
 * Pattern intentionally mirrors "one configure method + many configureX methods".
 */
public final class EswBronzeToSilverTask implements BronzeToSilverTask {

    private static final String TOPIC_LPCO = "raw-TB_CB_LPCO";
    private static final String TOPIC_LPCO_AMDT_ATTCH_DOC = "raw-TB_CB_LPCO_AMDT_ATTCH_DOC";
    private static final String TOPIC_LPCO_ATTCH_DOC = "raw-TB_CB_LPCO_ATTCH_DOC";
    private static final String TOPIC_LPCO_CMDT = "raw-TB_CB_LPCO_CMDT";
    private static final String TOPIC_LPCO_CMNT = "raw-TB_CB_LPCO_CMNT";
    private static final String TOPIC_LPCO_CNCL_ATTCH_DOC = "raw-TB_CB_LPCO_CNCL_ATTCH_DOC";
    private static final String TOPIC_LPCO_CSTMS = "raw-TB_CB_LPCO_CSTMS";
    private static final String TOPIC_LPCO_MPNG = "raw-TB_CB_LPCO_MPNG";

    private static final Set<String> ALL_TOPICS = Set.of(
            TOPIC_LPCO,
            TOPIC_LPCO_AMDT_ATTCH_DOC,
            TOPIC_LPCO_ATTCH_DOC,
            TOPIC_LPCO_CMDT,
            TOPIC_LPCO_CMNT,
            TOPIC_LPCO_CNCL_ATTCH_DOC,
            TOPIC_LPCO_CSTMS,
            TOPIC_LPCO_MPNG
    );

    @Override
    public String source() {
        return "oracle-esw";
    }

    @Override
    public void configure(StreamsBuilder builder, BronzeToSilverContext ctx) {
        configureTbCbLpco(builder, ctx);
        configureTbCbLpcoAmdtAttchDoc(builder, ctx);
        configureTbCbLpcoAttchDoc(builder, ctx);
        configureTbCbLpcoCmdt(builder, ctx);
        configureTbCbLpcoCmnt(builder, ctx);
        configureTbCbLpcoCnclAttchDoc(builder, ctx);
        configureTbCbLpcoCstms(builder, ctx);
        configureTbCbLpcoMpng(builder, ctx);
    }

    public boolean hasAnyConfiguredTopics(BronzeToSilverContext ctx) {
        for (String t : ctx.bronzeTopics) {
            if (ALL_TOPICS.contains(t)) return true;
        }
        return false;
    }

    private void configureTbCbLpco(StreamsBuilder builder, BronzeToSilverContext ctx) {
        if (!ctx.bronzeTopics.contains(TOPIC_LPCO)) {
            return;
        }

        final String sourceTopic = TOPIC_LPCO;
        final String tableName = "TB_CB_LPCO";
        final String destinationTopic = BronzeToSilverPipeline.silverTopicFor(
            sourceTopic,
                ctx.silverTopicPrefix,
                ctx.silverStripPrefix,
                ctx.silverNameStyle
        );
        final String icebergTable = BronzeToSilverPipeline.icebergTableFor(ctx.icebergNamespace, tableName);

        builder.stream(sourceTopic, Consumed.with(ctx.keySerde, ctx.bronzeValueSerde))
            .mapValues(BronzeToSilverPipeline::extractAfter)
            .filter((k, after) -> after != null)
            .mapValues(after -> createTbCbLpcoSilverEvent(after, ctx, icebergTable))
            .to(destinationTopic, Produced.with(ctx.keySerde, ctx.silverValueSerde));
    }

    private void configureTbCbLpcoAmdtAttchDoc(StreamsBuilder builder, BronzeToSilverContext ctx) {
        if (!ctx.bronzeTopics.contains(TOPIC_LPCO_AMDT_ATTCH_DOC)) {
            return;
        }

        final String sourceTopic = TOPIC_LPCO_AMDT_ATTCH_DOC;
        final String tableName = "TB_CB_LPCO_AMDT_ATTCH_DOC";
        final String destinationTopic = BronzeToSilverPipeline.silverTopicFor(
            sourceTopic,
                ctx.silverTopicPrefix,
                ctx.silverStripPrefix,
                ctx.silverNameStyle
        );
        final String icebergTable = BronzeToSilverPipeline.icebergTableFor(ctx.icebergNamespace, tableName);

        builder.stream(sourceTopic, Consumed.with(ctx.keySerde, ctx.bronzeValueSerde))
            .mapValues(BronzeToSilverPipeline::extractAfter)
            .filter((k, after) -> after != null)
            .mapValues(after -> createTbCbLpcoAmdtAttchDocSilverEvent(after, ctx, icebergTable))
            .to(destinationTopic, Produced.with(ctx.keySerde, ctx.silverValueSerde));
    }

    private void configureTbCbLpcoAttchDoc(StreamsBuilder builder, BronzeToSilverContext ctx) {
        if (!ctx.bronzeTopics.contains(TOPIC_LPCO_ATTCH_DOC)) {
            return;
        }

        final String sourceTopic = TOPIC_LPCO_ATTCH_DOC;
        final String tableName = "TB_CB_LPCO_ATTCH_DOC";
        final String destinationTopic = BronzeToSilverPipeline.silverTopicFor(
            sourceTopic,
                ctx.silverTopicPrefix,
                ctx.silverStripPrefix,
                ctx.silverNameStyle
        );
        final String icebergTable = BronzeToSilverPipeline.icebergTableFor(ctx.icebergNamespace, tableName);

        builder.stream(sourceTopic, Consumed.with(ctx.keySerde, ctx.bronzeValueSerde))
            .mapValues(BronzeToSilverPipeline::extractAfter)
            .filter((k, after) -> after != null)
            .mapValues(after -> createTbCbLpcoAttchDocSilverEvent(after, ctx, icebergTable))
            .to(destinationTopic, Produced.with(ctx.keySerde, ctx.silverValueSerde));
    }

    private void configureTbCbLpcoCmdt(StreamsBuilder builder, BronzeToSilverContext ctx) {
        if (!ctx.bronzeTopics.contains(TOPIC_LPCO_CMDT)) {
            return;
        }

        final String sourceTopic = TOPIC_LPCO_CMDT;
        final String tableName = "TB_CB_LPCO_CMDT";
        final String destinationTopic = BronzeToSilverPipeline.silverTopicFor(
            sourceTopic,
                ctx.silverTopicPrefix,
                ctx.silverStripPrefix,
                ctx.silverNameStyle
        );
        final String icebergTable = BronzeToSilverPipeline.icebergTableFor(ctx.icebergNamespace, tableName);

        builder.stream(sourceTopic, Consumed.with(ctx.keySerde, ctx.bronzeValueSerde))
            .mapValues(BronzeToSilverPipeline::extractAfter)
            .filter((k, after) -> after != null)
            .mapValues(after -> createTbCbLpcoCmdtSilverEvent(after, ctx, icebergTable))
            .to(destinationTopic, Produced.with(ctx.keySerde, ctx.silverValueSerde));
    }

    private void configureTbCbLpcoCmnt(StreamsBuilder builder, BronzeToSilverContext ctx) {
        if (!ctx.bronzeTopics.contains(TOPIC_LPCO_CMNT)) {
            return;
        }

        final String sourceTopic = TOPIC_LPCO_CMNT;
        final String tableName = "TB_CB_LPCO_CMNT";
        final String destinationTopic = BronzeToSilverPipeline.silverTopicFor(
            sourceTopic,
                ctx.silverTopicPrefix,
                ctx.silverStripPrefix,
                ctx.silverNameStyle
        );
        final String icebergTable = BronzeToSilverPipeline.icebergTableFor(ctx.icebergNamespace, tableName);

        builder.stream(sourceTopic, Consumed.with(ctx.keySerde, ctx.bronzeValueSerde))
            .mapValues(BronzeToSilverPipeline::extractAfter)
            .filter((k, after) -> after != null)
            .mapValues(after -> createTbCbLpcoCmntSilverEvent(after, ctx, icebergTable))
            .to(destinationTopic, Produced.with(ctx.keySerde, ctx.silverValueSerde));
    }

    private void configureTbCbLpcoCnclAttchDoc(StreamsBuilder builder, BronzeToSilverContext ctx) {
        if (!ctx.bronzeTopics.contains(TOPIC_LPCO_CNCL_ATTCH_DOC)) {
            return;
        }

        final String sourceTopic = TOPIC_LPCO_CNCL_ATTCH_DOC;
        final String tableName = "TB_CB_LPCO_CNCL_ATTCH_DOC";
        final String destinationTopic = BronzeToSilverPipeline.silverTopicFor(
            sourceTopic,
                ctx.silverTopicPrefix,
                ctx.silverStripPrefix,
                ctx.silverNameStyle
        );
        final String icebergTable = BronzeToSilverPipeline.icebergTableFor(ctx.icebergNamespace, tableName);

        builder.stream(sourceTopic, Consumed.with(ctx.keySerde, ctx.bronzeValueSerde))
            .mapValues(BronzeToSilverPipeline::extractAfter)
            .filter((k, after) -> after != null)
            .mapValues(after -> createTbCbLpcoCnclAttchDocSilverEvent(after, ctx, icebergTable))
            .to(destinationTopic, Produced.with(ctx.keySerde, ctx.silverValueSerde));
    }

    private void configureTbCbLpcoCstms(StreamsBuilder builder, BronzeToSilverContext ctx) {
        if (!ctx.bronzeTopics.contains(TOPIC_LPCO_CSTMS)) {
            return;
        }

        final String sourceTopic = TOPIC_LPCO_CSTMS;
        final String tableName = "TB_CB_LPCO_CSTMS";
        final String destinationTopic = BronzeToSilverPipeline.silverTopicFor(
            sourceTopic,
                ctx.silverTopicPrefix,
                ctx.silverStripPrefix,
                ctx.silverNameStyle
        );
        final String icebergTable = BronzeToSilverPipeline.icebergTableFor(ctx.icebergNamespace, tableName);

        builder.stream(sourceTopic, Consumed.with(ctx.keySerde, ctx.bronzeValueSerde))
            .mapValues(BronzeToSilverPipeline::extractAfter)
            .filter((k, after) -> after != null)
            .mapValues(after -> createTbCbLpcoCstmsSilverEvent(after, ctx, icebergTable))
            .to(destinationTopic, Produced.with(ctx.keySerde, ctx.silverValueSerde));
    }

    private void configureTbCbLpcoMpng(StreamsBuilder builder, BronzeToSilverContext ctx) {
        if (!ctx.bronzeTopics.contains(TOPIC_LPCO_MPNG)) {
            return;
        }

        final String sourceTopic = TOPIC_LPCO_MPNG;
        final String tableName = "TB_CB_LPCO_MPNG";
        final String destinationTopic = BronzeToSilverPipeline.silverTopicFor(
                sourceTopic,
                ctx.silverTopicPrefix,
                ctx.silverStripPrefix,
                ctx.silverNameStyle
        );
        final String icebergTable = BronzeToSilverPipeline.icebergTableFor(ctx.icebergNamespace, tableName);

        builder.stream(sourceTopic, Consumed.with(ctx.keySerde, ctx.bronzeValueSerde))
                .mapValues(BronzeToSilverPipeline::extractAfter)
                .filter((k, after) -> after != null)
                .mapValues(after -> createTbCbLpcoMpngSilverEvent(after, ctx, icebergTable))
                .to(destinationTopic, Produced.with(ctx.keySerde, ctx.silverValueSerde));
    }

    private static GenericRecord createTbCbLpcoSilverEvent(GenericRecord after, BronzeToSilverContext ctx, String icebergTable) {
        Schema schema = BronzeToSilverPipeline.silverSchemaFor(after, ctx.silverRecordNamespace, "TB_CB_LPCO");
        return BronzeToSilverPipeline.toSilver(after, schema, icebergTable);
    }

    private static GenericRecord createTbCbLpcoAmdtAttchDocSilverEvent(GenericRecord after, BronzeToSilverContext ctx, String icebergTable) {
        Schema schema = BronzeToSilverPipeline.silverSchemaFor(after, ctx.silverRecordNamespace, "TB_CB_LPCO_AMDT_ATTCH_DOC");
        return BronzeToSilverPipeline.toSilver(after, schema, icebergTable);
    }

    private static GenericRecord createTbCbLpcoAttchDocSilverEvent(GenericRecord after, BronzeToSilverContext ctx, String icebergTable) {
        Schema schema = BronzeToSilverPipeline.silverSchemaFor(after, ctx.silverRecordNamespace, "TB_CB_LPCO_ATTCH_DOC");
        return BronzeToSilverPipeline.toSilver(after, schema, icebergTable);
    }

    private static GenericRecord createTbCbLpcoCmdtSilverEvent(GenericRecord after, BronzeToSilverContext ctx, String icebergTable) {
        Schema schema = BronzeToSilverPipeline.silverSchemaFor(after, ctx.silverRecordNamespace, "TB_CB_LPCO_CMDT");
        return BronzeToSilverPipeline.toSilver(after, schema, icebergTable);
    }

    private static GenericRecord createTbCbLpcoCmntSilverEvent(GenericRecord after, BronzeToSilverContext ctx, String icebergTable) {
        Schema schema = BronzeToSilverPipeline.silverSchemaFor(after, ctx.silverRecordNamespace, "TB_CB_LPCO_CMNT");
        return BronzeToSilverPipeline.toSilver(after, schema, icebergTable);
    }

    private static GenericRecord createTbCbLpcoCnclAttchDocSilverEvent(GenericRecord after, BronzeToSilverContext ctx, String icebergTable) {
        Schema schema = BronzeToSilverPipeline.silverSchemaFor(after, ctx.silverRecordNamespace, "TB_CB_LPCO_CNCL_ATTCH_DOC");
        return BronzeToSilverPipeline.toSilver(after, schema, icebergTable);
    }

    private static GenericRecord createTbCbLpcoCstmsSilverEvent(GenericRecord after, BronzeToSilverContext ctx, String icebergTable) {
        Schema schema = BronzeToSilverPipeline.silverSchemaFor(after, ctx.silverRecordNamespace, "TB_CB_LPCO_CSTMS");
        return BronzeToSilverPipeline.toSilver(after, schema, icebergTable);
    }

    private static GenericRecord createTbCbLpcoMpngSilverEvent(GenericRecord after, BronzeToSilverContext ctx, String icebergTable) {
        Schema schema = BronzeToSilverPipeline.silverSchemaFor(after, ctx.silverRecordNamespace, "TB_CB_LPCO_MPNG");
        return BronzeToSilverPipeline.toSilver(after, schema, icebergTable);
    }
}
