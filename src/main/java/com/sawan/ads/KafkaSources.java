package com.sawan.ads;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaSources {
    public static DataStreamSource<String> pageContext(StreamExecutionEnvironment env, AppConfig config){
        var src = KafkaSource.<String>builder()
                .setBootstrapServers(config.kafka.bootstrapServers)
                .setTopics(config.kafka.topics.pageContext)
                .setGroupId("ads-page-context")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        return env.fromSource(src, WatermarkStrategy.noWatermarks(), Constants.Kafka.SOURCE_PAGE_CONTEXT_OP_NAME);
    }
}
