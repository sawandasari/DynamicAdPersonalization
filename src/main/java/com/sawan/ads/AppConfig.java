package com.sawan.ads;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;

public class AppConfig {
    public String env;
    public Kafka kafka;
    public Mongo mongo;

    public static class Kafka {
        public String bootstrapServers;
        public Topics topics;
        public static class Topics {
            public String pageContext;
        }
    }
    public static class Mongo{
        public String database;
        public String uri;
    }
    public static AppConfig load() {

        try(var in = AppConfig.class.getClassLoader().getResourceAsStream("application.yaml")){
            var cfg = new com.fasterxml.jackson.databind.ObjectMapper(
                    new com.fasterxml.jackson.dataformat.yaml.YAMLFactory())
                    .readValue(in, AppConfig.class);

            // Prefer environment variable at runtime, fall back to YAML value
            var envUri = System.getenv("MONGO_URI");
            if (envUri != null && !envUri.isBlank()) {
                cfg.mongo.uri = envUri;
            }
            return cfg;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load application.yaml", e);
        }
    }
}
