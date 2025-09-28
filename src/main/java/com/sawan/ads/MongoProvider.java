package com.sawan.ads;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class MongoProvider {
    public static void ping(String uri, String databaseName){
        try(MongoClient c = MongoClients.create(uri)){
            c.getDatabase(databaseName).runCommand(new org.bson.Document("ping",1));
            System.out.println("[OK] Mongo ping: " + uri + " / " + databaseName);
        }
    }
}
