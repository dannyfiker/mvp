package et.gov.lakehouse.govaggregator.core.bronze;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Shared transformations for Debezium-envelope Avro -> silver Avro ("after" projection + routing field). */
public final class BronzeToSilverPipeline {

    private BronzeToSilverPipeline() {}

    private static final ConcurrentHashMap<String, Schema> SILVER_SCHEMA_CACHE = new ConcurrentHashMap<>();

    public static String silverTopicFor(String inputTopic, String silverPrefix, String stripPrefix, String nameStyle) {
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

    public static GenericRecord extractAfter(GenericRecord root) {
        if (root == null) return null;

        // Debezium envelope is typically the root record (before/after/op/ts_ms/...)
        // but some pipelines may wrap it as payload.
        GenericRecord envelope = root;
        if (root.getSchema() != null && root.getSchema().getField("payload") != null) {
            Object maybePayload = root.get("payload");
            if (maybePayload instanceof GenericRecord gr) {
                envelope = gr;
            }
        }

        if (envelope.getSchema() == null || envelope.getSchema().getField("after") == null) {
            return null;
        }

        Object after = envelope.get("after");
        return (after instanceof GenericRecord gr) ? gr : null;
    }

    public static String deriveSourceTableFromTopic(String inputTopic, String stripPrefix) {
        String t = inputTopic;
        if (stripPrefix != null && !stripPrefix.isBlank() && t.startsWith(stripPrefix)) {
            t = t.substring(stripPrefix.length());
        }

        // For Oracle ESW we route Debezium topics to: raw-<TABLE>
        if (t.startsWith("raw-")) {
            return t.substring("raw-".length());
        }
        return t;
    }

    public static String icebergTableFor(String namespace, String tableName) {
        String ns = (namespace == null || namespace.isBlank()) ? "silver" : namespace;
        return ns + "." + tableName.toLowerCase();
    }

    public static Schema silverSchemaFor(GenericRecord after, String outputNamespace, String outputName) {
        if (after == null) {
            throw new IllegalArgumentException("after record must not be null");
        }

        String cacheKey = outputNamespace + ":" + outputName + ":" + after.getSchema().getFullName();
        return SILVER_SCHEMA_CACHE.computeIfAbsent(cacheKey, notUsed -> {
            Schema afterSchema = after.getSchema();

            Schema schema = Schema.createRecord(outputName, null, outputNamespace, false);
            List<Schema.Field> outFields = new ArrayList<>();

            outFields.add(new Schema.Field("__iceberg_table", Schema.create(Schema.Type.STRING), null, (Object) null));

            for (Schema.Field f : afterSchema.getFields()) {
                String outFieldName = f.name().toLowerCase();
                Schema.Field outField = new Schema.Field(outFieldName, f.schema(), f.doc(), f.defaultVal());
                for (Map.Entry<String, Object> e : f.getObjectProps().entrySet()) {
                    outField.addProp(e.getKey(), e.getValue());
                }
                outFields.add(outField);
            }

            schema.setFields(outFields);
            schema.addProp("connect.name", outputNamespace + "." + outputName);
            schema.addProp("connect.version", 1);
            return schema;
        });
    }

    public static GenericRecord toSilver(GenericRecord after, Schema silverSchema, String icebergTable) {
        GenericData.Record out = new GenericData.Record(silverSchema);
        out.put("__iceberg_table", icebergTable);
        for (Schema.Field f : after.getSchema().getFields()) {
            out.put(f.name().toLowerCase(), after.get(f.pos()));
        }
        return out;
    }
}
