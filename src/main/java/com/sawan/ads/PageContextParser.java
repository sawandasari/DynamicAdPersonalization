package com.sawan.ads;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sawan.ads.model.PageContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Converts raw JSON strings into PageContext.
 * Bad JSON goes to side output "page_dlq" instead of failing the job.
 */
public class PageContextParser extends ProcessFunction<String, PageContext> {

    public static final OutputTag<String> DLQ = new OutputTag<>(Constants.DLQ.PAGE);
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void processElement(String value, Context ctx, Collector<PageContext> out) {
        try {
            out.collect(mapper.readValue(value, PageContext.class));
        } catch (Exception e) {
            // Send the original bad payload to DLQ for auditing
            ctx.output(DLQ, value);
        }
    }
}
