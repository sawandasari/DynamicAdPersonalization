package com.sawan.ads;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BootCheck {
    public static void main(String[] args) throws Exception{
        var config = AppConfig.load();
        MongoProvider.ping(config.mongo.uri, config.mongo.database);

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        // get raw strings from Kafka
        var raw = KafkaSources.pageContext(env, config);
        // parse → typed stream + DLQ
        var parsed = raw.process(new PageContextParser()).name("parse-page");
        // (temporary) observe both good and bad
        parsed.print("page");  // good payloads
        parsed.getSideOutput(PageContextParser.DLQ).print("dlq"); // malformed JSON

        KafkaSources.pageContext(env, config).print();
        env.execute("Boot Check");
    }
}
