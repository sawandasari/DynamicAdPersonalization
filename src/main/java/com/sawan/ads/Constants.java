package com.sawan.ads;

/** Project-wide constants. Keep only values that are truly global and stable.
 *  Env-specific values still come from AppConfig (YAML/env vars). */
public final class Constants {
    private Constants() {}

    // Environment variable names (used to override YAML in AppConfig)
    public static final class Env {
        public static final String KAFKA_BOOTSTRAP = "KAFKA_BOOTSTRAP";
        public static final String MONGO_URI = "MONGO_URI";
        public static final String MONGO_DB = "MONGO_DB";
    }

    // Safe defaults (used by AppConfig if YAML/env are missing)
    public static final class Defaults {
        public static final String KAFKA_BOOTSTRAP      = "localhost:9092";
        public static final String TOPIC_PAGE_CONTEXT   = "page_context";
        public static final String TOPIC_CAMPAIGNS      = "ad_campaigns";
        public static final String TOPIC_DECISIONS      = "ad_decisions";
        public static final String MONGO_URI            = "mongodb://localhost:27017";
        public static final String MONGO_DB             = "realtime_ads";
        public static final long   CHECKPOINT_MS        = 10_000L;
    }

    // Kafka-related identifiers (group IDs, operator names)
    public static final class Kafka {
        public static final String GROUP_PAGE_CONTEXT          = "ads-pagectx-local";
        public static final String GROUP_CAMPAIGNS             = "ads-campaigns-local";
        public static final String SOURCE_PAGE_CONTEXT_OP_NAME = "page_context";
        public static final String SOURCE_CAMPAIGNS_OP_NAME    = "ad_campaigns";
        public static final String SINK_DECISIONS_OP_NAME      = "ad_decisions";
    }

    // MongoDB collection names
    public static final class Mongo {
        public static final String COL_HEARTBEAT = "boot_heartbeat";
        public static final String COL_DECISIONS = "ad_decisions";
    }

    // Side output tags (Dead Letter Queues)
    public static final class DLQ {
        public static final String PAGE     = "page_dlq";
        public static final String CAMPAIGN = "campaign_dlq";
    }

    // Flink state names / descriptors
    public static final class StateNames {
        public static final String CAMPAIGNS_BROADCAST = "campaigns";
        public static final String USER_CAPS = "userCaps";
    }

    // Job names (appear in Flink UI / logs)
    public static final class Jobs {
        public static final String BOOTCHECK     = "BootCheck";
        public static final String AD_DECISION   = "AdDecisionJob";
    }

    // Regex/constants used across utilities (e.g., masking credentials in URIs)
    public static final class Regex {
        /** Matches 'user:pass@' in a URI so we can mask it in logs. */
        public static final String MASK_CREDENTIALS_IN_URI = "(?<=//)[^:@/]+:[^@/]+@";
    }
}
