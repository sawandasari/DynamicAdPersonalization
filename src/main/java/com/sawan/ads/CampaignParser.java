package com.sawan.ads;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sawan.ads.model.Campaign;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class CampaignParser extends ProcessFunction<String, Campaign> {
    public static final OutputTag<String> DLQ = new OutputTag<>(Constants.DLQ.CAMPAIGN);
    private final ObjectMapper m = new ObjectMapper();
    @Override public void processElement(String v, Context ctx, Collector<Campaign> out) {
        try {
            out.collect(m.readValue(v, Campaign.class));
        } catch (Exception e) {
            ctx.output(DLQ, v);
        }
    }
}
