package com.sawan.ads;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sawan.ads.model.PageContext;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

public class AdDecisionJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        var cfg = AppConfig.load();
        MongoProvider.ping(cfg.mongo.uri, cfg.mongo.database);

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(Constants.Defaults.CHECKPOINT_MS);

        // Sources
        DataStream<String> rawPages = KafkaSourceBuilder(env, cfg.kafka.bootstrapServers, cfg.kafka.topics.pageContext);
        var pages = rawPages.process(new PageContextParser()).name("parse-page");
        DataStream<String> rawCampaigns = KafkaSourceBuilder(env, cfg.kafka.bootstrapServers, cfg.kafka.topics.campaigns);
        var campaigns = rawCampaigns.process(new CampaignParser()).name("parse-campaign");

        // Broadcast + decision
        var bcast = campaigns.broadcast(DecisionFunction.CAMPAIGNS_DESC);
        var decisions = pages
                .keyBy(PageContext::userId)
                .connect(bcast)
                .process(new DecisionFunction())
                .name("decide");

        // Sinks
        decisions.addSink(MongoSinks.adDecisions(cfg.mongo.uri, cfg.mongo.database))
                .name("mongo-upsert");
        decisions.map(ad -> MAPPER.writeValueAsString(ad))
                .sinkTo(kafkaDecisionSink(cfg.kafka.bootstrapServers, cfg.kafka.topics.decisions))
                .name("kafka-decisions");

        env.execute("Ad Decision Job");
    }

    private static DataStream<String> KafkaSourceBuilder(StreamExecutionEnvironment env, String bootstrap, String topic) {
        KafkaSource<String> src = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("ads-" + topic + "-g")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        return env.fromSource(src, WatermarkStrategy.noWatermarks(), Constants.Kafka.SOURCE_PAGE_CONTEXT_OP_NAME);
    }

    private static KafkaSink<String> kafkaDecisionSink(String bootstrap, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema((String s) -> s.getBytes(StandardCharsets.UTF_8))
                                .build())
                .build();
    }
}
