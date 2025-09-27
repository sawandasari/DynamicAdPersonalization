# DynamicAdPersonalization
Ingests user activity and page context events from Kafka, performs real-time candidate ad selection using contextual signals + user profile state (frequency capping, pacing, eligibility), and writes ad decisions + online metrics to MongoDB. Uses broadcast state for campaign configs and exactly-once semantics.
