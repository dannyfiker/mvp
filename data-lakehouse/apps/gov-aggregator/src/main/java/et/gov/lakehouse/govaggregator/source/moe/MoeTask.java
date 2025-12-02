package et.gov.lakehouse.govaggregator.source.moe;

import et.gov.lakehouse.govaggregator.avro.AggregatedRecord;
import et.gov.lakehouse.govaggregator.avro.MoeEducationStat;
import org.apache.kafka.streams.kstream.KStream;

public final class MoeTask {
    public static KStream<String, AggregatedRecord> build(KStream<String, MoeEducationStat> in) {
        return in.mapValues(v -> new AggregatedRecord("MoE","education_stat", v.getReportedAt(), v.getSchoolId(),
                "{\"region\":\""+v.getRegion()+"\",\"students\":"+v.getStudents()+",\"teachers\":"+v.getTeachers()+"}"));
    }
    private MoeTask() {}
}
