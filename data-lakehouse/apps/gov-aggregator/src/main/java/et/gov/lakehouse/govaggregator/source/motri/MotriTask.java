package et.gov.lakehouse.govaggregator.source.motri;

import et.gov.lakehouse.govaggregator.avro.AggregatedRecord;
import et.gov.lakehouse.govaggregator.avro.MotriTransportPermit;
import org.apache.kafka.streams.kstream.KStream;

public final class MotriTask {
    public static KStream<String, AggregatedRecord> build(KStream<String, MotriTransportPermit> in) {
        return in.mapValues(v -> new AggregatedRecord("MoTRI","transport_permit", v.getValidUntil(), v.getPermitNo(),
                "{\"plate\":\""+v.getVehiclePlate()+"\",\"route\":\""+v.getRoute()+"\"}"));
    }
    private MotriTask() {}
}
