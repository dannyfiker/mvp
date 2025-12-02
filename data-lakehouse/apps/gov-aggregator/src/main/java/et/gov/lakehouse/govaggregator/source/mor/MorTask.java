package et.gov.lakehouse.govaggregator.source.mor;

import et.gov.lakehouse.govaggregator.avro.AggregatedRecord;
import et.gov.lakehouse.govaggregator.avro.MorTaxPayment;
import org.apache.kafka.streams.kstream.KStream;

public final class MorTask {
    public static KStream<String, AggregatedRecord> build(KStream<String, MorTaxPayment> in) {
        return in.mapValues(v -> new AggregatedRecord("MoR","tax_payment", v.getPaidAt(), v.getPaymentId(),
                "{\"tin\":\""+v.getTin()+"\",\"amount\":"+v.getAmount()+",\"currency\":\""+v.getCurrency()+"\"}"));
    }
    private MorTask() {}
}
