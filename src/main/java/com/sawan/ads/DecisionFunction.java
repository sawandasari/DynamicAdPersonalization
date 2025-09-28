package com.sawan.ads;

import com.sawan.ads.model.AdDecision;
import com.sawan.ads.model.Campaign;
import com.sawan.ads.model.PageContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DecisionFunction extends KeyedBroadcastProcessFunction<String, PageContext, Campaign, AdDecision> {

    // Broadcast state: all active campaigns keyed by campaignId
    public static final MapStateDescriptor<String, Campaign> CAMPAIGNS_DESC =
            new MapStateDescriptor<>(Constants.StateNames.CAMPAIGNS_BROADCAST, Types.STRING, Types.POJO(Campaign.class));

    // Per-user frequency counts: key is "<yyyy-MM-dd>|<campaignId>"
    private transient MapState<String, Integer> userCaps;

    @Override
    public void open(Configuration parameters) {
        var ttl = StateTtlConfig.newBuilder(Time.days(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupFullSnapshot()
                .build();
        MapStateDescriptor<String, Integer> capsDesc =
                new MapStateDescriptor<>(Constants.StateNames.USER_CAPS, Types.STRING, Types.INT);
        capsDesc.enableTimeToLive(ttl);
        userCaps = getRuntimeContext().getMapState(capsDesc);
    }

    @Override
    public void processBroadcastElement(Campaign c, Context ctx, Collector<AdDecision> out) throws Exception {
        var b = ctx.getBroadcastState(CAMPAIGNS_DESC);
        if (c.active()) b.put(c.campaignId(), c); else b.remove(c.campaignId());
    }

    @Override
    public void processElement(PageContext p, ReadOnlyContext ctx, Collector<AdDecision> out) throws Exception {
        ReadOnlyBroadcastState<String, Campaign> camp = ctx.getBroadcastState(CAMPAIGNS_DESC);
        if (!camp.immutableEntries().iterator().hasNext()) return;

        final String today = DateTimeFormatter.ISO_LOCAL_DATE
                .withZone(ZoneOffset.UTC).format(Instant.ofEpochMilli(p.ts()));

        Campaign best = null;
        double bestScore = -1;
        List<String> reasons = new ArrayList<>();

        for (Map.Entry<String, Campaign> e : camp.immutableEntries()) {
            Campaign c = e.getValue();
            if (!c.active()) continue;
            if (!c.languages().isEmpty() && !c.languages().contains(p.lang())) continue;
            if (!c.categories().isEmpty() && !c.categories().contains(p.category())) continue;

            // frequency cap
            String capKey = today + "|" + c.campaignId();
            int used = userCaps.contains(capKey) ? userCaps.get(capKey) : 0;
            if (used >= Math.max(c.capPerUserPerDay(), 1)) continue;

            // simple score = (keyword overlap) + bid weight
            int overlap = 0;
            if (p.keywords() != null && c.keywords() != null) {
                var pageKs = new HashSet<>((Collection) p.keywords());
                for (String kw : c.keywords()) if (pageKs.contains(kw)) overlap++;
            }
            double score = overlap * 1.0 + c.bid() * 0.1; // tiny weight for bid
            if (score > bestScore) {
                bestScore = score;
                best = c;
            }
        }

        if (best == null) return;

        // increment cap
        String capKey = today + "|" + best.campaignId();
        int used = userCaps.contains(capKey) ? userCaps.get(capKey) : 0;
        userCaps.put(capKey, used + 1);

        reasons.add("lang=" + p.lang());
        reasons.add("cat=" + p.category());
        reasons.add("keywordsOverlap&bid");

        var decision = new AdDecision(
                "dec-" + p.requestId(), p.requestId(), p.userId(), best.campaignId(), bestScore, reasons, p.ts()
        );
        out.collect(decision);
    }
}
