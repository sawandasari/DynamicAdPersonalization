package com.sawan.ads.model;
import java.util.List;

public record Campaign(
        String campaignId,
        List<String> languages,
        List<String> categories,
        List<String> keywords,
        double bid,
        int capPerUserPerDay,
        boolean active,
        long ts
) {}
