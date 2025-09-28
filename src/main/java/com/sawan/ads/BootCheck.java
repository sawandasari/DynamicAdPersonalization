package com.sawan.ads;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BootCheck {
    public static void main(String[] args) throws Exception{
        var config = AppConfig.load();
        MongoProvider.ping(config.mongo.uri, config.mongo.database);

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSources.pageContext(env, config).print();
        env.execute("Boot Check");
    }
}
