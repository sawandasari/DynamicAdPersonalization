package com.sawan.ads.model;
import java.util.List;

public record AdDecision(
        String decisionId,
        String requestId,
        String userId,
        String campaignId,
        double score,
        List<String> reasons,
        long ts
) {}
