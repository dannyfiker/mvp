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
