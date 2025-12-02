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

    private static String sysOrEnv(String sysKey, String envKey, String defVal) {
        String v = System.getProperty(sysKey);
        if (v != null && !v.isBlank()) return v;
        v = System.getenv(envKey);
        return (v != null && !v.isBlank()) ? v : defVal;
    }

    public static void main(String[] args) {
        // ---- Core Streams config (override via -D or env) ----
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG,
                sysOrEnv("application.id", "APPLICATION_ID", "gov-aggregator-app"));
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                sysOrEnv("bootstrap.servers", "BOOTSTRAP_SERVERS", "mvp-kafka:9092"));
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                sysOrEnv("processing.guarantee", "PROCESSING_GUARANTEE", "at_least_once"));
        p.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
                sysOrEnv("cache.max.bytes.buffering", "CACHE_MAX_BYTES_BUFFERING", "10485760"));
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
                sysOrEnv("commit.interval.ms", "COMMIT_INTERVAL_MS", "5000"));

        // ---- Apicurio wiring (override via -D or env) ----
        final String registryUrl = sysOrEnv(
                "apicurio.url", "APICURIO_URL", "http://mvp-apicurio:8080/apis/registry/v2");
        final String artifactGroupId = sysOrEnv(
                "apicurio.group.id", "APICURIO_GROUP_ID", "gov-aggregator");

        StreamsBuilder b = new StreamsBuilder();

        // ---- SerDes ----
        Serde<String> stringSerde = SerdeFactory.stringSerde();

        Serde<AggregatedRecord> outSerde       = SerdeFactory.avroSerde(AggregatedRecord.class, registryUrl, artifactGroupId);
        Serde<MorTaxPayment> morSerde          = SerdeFactory.avroSerde(MorTaxPayment.class, registryUrl, artifactGroupId);
        Serde<EccTradePermit> eccSerde         = SerdeFactory.avroSerde(EccTradePermit.class, registryUrl, artifactGroupId);
        Serde<MotriTransportPermit> motriSerde = SerdeFactory.avroSerde(MotriTransportPermit.class, registryUrl, artifactGroupId);
        Serde<NbeFxRate> nbeSerde              = SerdeFactory.avroSerde(NbeFxRate.class, registryUrl, artifactGroupId);
        Serde<MoeEducationStat> moeSerde       = SerdeFactory.avroSerde(MoeEducationStat.class, registryUrl, artifactGroupId);

        // ---- Sources ----
        KStream<String, MorTaxPayment> mor   = b.stream(Topics.MOR,   Consumed.with(stringSerde, morSerde));
        KStream<String, EccTradePermit> ecc  = b.stream(Topics.ECC,   Consumed.with(stringSerde, eccSerde));
        KStream<String, MotriTransportPermit> motri = b.stream(Topics.MOTRI, Consumed.with(stringSerde, motriSerde));
        KStream<String, NbeFxRate> nbe      = b.stream(Topics.NBE,    Consumed.with(stringSerde, nbeSerde));
        KStream<String, MoeEducationStat> moe = b.stream(Topics.MOE,  Consumed.with(stringSerde, moeSerde));

        // ---- Per-source transforms ----
        KStream<String, AggregatedRecord> morAgg    = MorTask.build(mor);
        KStream<String, AggregatedRecord> eccAgg    = EccTask.build(ecc);
        KStream<String, AggregatedRecord> motriAgg  = MotriTask.build(motri);
        KStream<String, AggregatedRecord> nbeAgg    = NbeTask.build(nbe);
        KStream<String, AggregatedRecord> moeAgg    = MoeTask.build(moe);

        // ---- Union + sink ----
        KStream<String, AggregatedRecord> unified =
                morAgg.merge(eccAgg).merge(motriAgg).merge(nbeAgg).merge(moeAgg);

        unified.to(Topics.OUT, Produced.with(stringSerde, outSerde));

        // ---- Bootstrap Streams ----
        Topology topology = b.build();
        KafkaStreams streams = new KafkaStreams(topology, p);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
