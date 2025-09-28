package com.sawan.ads;

import com.mongodb.client.*;
import com.mongodb.client.model.ReplaceOptions;
import com.sawan.ads.model.AdDecision;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

public class MongoSinks {
    public static SinkFunction<AdDecision> adDecisions(String uri, String dbName) {
        return new SinkFunction<>() {
            transient MongoClient client; transient MongoCollection<Document> col;

            @Override public void invoke(AdDecision d, Context ctx) {
                if (client == null) {
                    client = MongoClients.create(uri);
                    col = client.getDatabase(dbName).getCollection(Constants.Mongo.COL_DECISIONS);

                }
                var doc = new Document("_id", d.requestId())
                        .append("decisionId", d.decisionId())
                        .append("userId", d.userId())
                        .append("campaignId", d.campaignId())
                        .append("score", d.score())
                        .append("reasons", d.reasons())
                        .append("ts", d.ts());
                col.replaceOne(new Document("_id", d.requestId()), doc, new ReplaceOptions().upsert(true));
            }
        };
    }
}
