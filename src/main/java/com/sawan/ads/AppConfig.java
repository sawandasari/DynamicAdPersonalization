package com.sawan.ads;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;

public class AppConfig {
    public String env = "local";
    public Kafka kafka = new Kafka();
    public Mongo mongo = new Mongo();

    public static class Kafka {
        public String bootstrapServers;
        public Topics topics = new Topics();

        public static class Topics {
            public String pageContext;
            public String campaigns;
            public String decisions;
        }
    }

    public static class Mongo {
        public String uri;
        public String database;
    }

    /** Load from application.yaml, then apply env/system-property overrides, set safe defaults, validate, log. */
    public static AppConfig load() {
        try (InputStream in = AppConfig.class.getClassLoader().getResourceAsStream("application.yaml")) {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            AppConfig cfg = (in != null) ? mapper.readValue(in, AppConfig.class) : new AppConfig();

            // ---- Overrides from environment or -D system properties ----
            String bs  = getenvOrProp(Constants.Env.KAFKA_BOOTSTRAP);
            String mu  = getenvOrProp(Constants.Env.MONGO_URI);
            String mdb = getenvOrProp(Constants.Env.MONGO_DB);
            if (notBlank(bs))  cfg.kafka.bootstrapServers = bs;
            if (notBlank(mu))  cfg.mongo.uri = mu;
            if (notBlank(mdb)) cfg.mongo.database = mdb;

            // ---- Sensible defaults for local dev ----
            if (isBlank(cfg.kafka.bootstrapServers)) cfg.kafka.bootstrapServers = Constants.Defaults.KAFKA_BOOTSTRAP;
            if (isBlank(cfg.kafka.topics.pageContext)) cfg.kafka.topics.pageContext = Constants.Defaults.TOPIC_PAGE_CONTEXT;
            if (isBlank(cfg.kafka.topics.campaigns))   cfg.kafka.topics.campaigns   = Constants.Defaults.TOPIC_CAMPAIGNS;
            if (isBlank(cfg.kafka.topics.decisions))   cfg.kafka.topics.decisions   = Constants.Defaults.TOPIC_DECISIONS;
            if (isBlank(cfg.mongo.uri))                cfg.mongo.uri                = Constants.Defaults.MONGO_URI;
            if (isBlank(cfg.mongo.database))           cfg.mongo.database           = Constants.Defaults.MONGO_DB;


            // ---- Validate & log (mask credentials if present) ----
            validate(cfg);
            logConfig(cfg);

            return cfg;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load application.yaml", e);
        }
    }

    private static String getenvOrProp(String k) {
        String v = System.getenv(k);
        if (isBlank(v)) v = System.getProperty(k);
        return v;
    }
    private static boolean isBlank(String s) { return s == null || s.trim().isEmpty(); }
    private static boolean notBlank(String s) { return !isBlank(s); }

    private static void validate(AppConfig c) {
        StringBuilder sb = new StringBuilder();
        if (isBlank(c.kafka.bootstrapServers)) sb.append("kafka.bootstrapServers is required\n");
        if (isBlank(c.kafka.topics.pageContext)) sb.append("kafka.topics.pageContext is required\n");
        if (isBlank(c.kafka.topics.campaigns))   sb.append("kafka.topics.campaigns is required\n");
        if (isBlank(c.kafka.topics.decisions))   sb.append("kafka.topics.decisions is required\n");
        if (isBlank(c.mongo.uri))                sb.append("mongo.uri is required\n");
        if (isBlank(c.mongo.database))           sb.append("mongo.database is required\n");
        if (sb.length() > 0) throw new IllegalStateException("Invalid configuration:\n" + sb);
    }

    private static void logConfig(AppConfig c) {
        String safeMongo = c.mongo.uri;
        try {
            // mask user:pass in mongodb URI if present
            safeMongo = c.mongo.uri.replaceAll(Constants.Regex.MASK_CREDENTIALS_IN_URI, "<user>:****@");
        } catch (Exception ignore) {}
        System.out.println("[cfg] env=" + c.env +
                " kafka=" + c.kafka.bootstrapServers +
                " topics=" + c.kafka.topics.pageContext + "," + c.kafka.topics.campaigns + "," + c.kafka.topics.decisions +
                " mongo=" + safeMongo + " db=" + c.mongo.database);
    }
}
