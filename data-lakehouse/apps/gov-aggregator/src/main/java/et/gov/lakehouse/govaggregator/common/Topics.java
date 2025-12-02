package et.gov.lakehouse.govaggregator.common;

public final class Topics {
    public static final String MOR = System.getProperty("topic.mor", System.getenv().getOrDefault("TOPIC_MOR","mor.tax.payments"));
    public static final String ECC = System.getProperty("topic.ecc", System.getenv().getOrDefault("TOPIC_ECC","ecc.trade.permits"));
    public static final String MOTRI = System.getProperty("topic.motri", System.getenv().getOrDefault("TOPIC_MOTRI","motri.transport.permits"));
    public static final String NBE = System.getProperty("topic.nbe", System.getenv().getOrDefault("TOPIC_NBE","nbe.fx.rates"));
    public static final String MOE = System.getProperty("topic.moe", System.getenv().getOrDefault("TOPIC_MOE","moe.education.stats"));
    public static final String OUT = System.getProperty("topic.out", System.getenv().getOrDefault("TOPIC_OUT","gov.aggregates.enriched"));
    private Topics() {}
}
