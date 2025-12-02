package et.gov.lakehouse.govaggregator.source.ecc;

import et.gov.lakehouse.govaggregator.avro.AggregatedRecord;
import et.gov.lakehouse.govaggregator.avro.EccTradePermit;
import org.apache.kafka.streams.kstream.KStream;

public final class EccTask {
    public static KStream<String, AggregatedRecord> build(KStream<String, EccTradePermit> in) {
        return in.mapValues(v -> new AggregatedRecord("ECC","trade_permit", v.getIssuedAt(), v.getPermitId(),
                "{\"company\":\""+v.getCompanyName()+"\",\"commodity\":\""+v.getCommodity()+"\",\"valueUsd\":"+v.getValueUsd()+"}"));
    }
    private EccTask() {}
}
