package com.sawan.ads.model;

import java.util.List;

/**
 * Typed schema for an incoming page context event.
 * Keep fields public + no-args ctor so Jackson can deserialize easily.
 */
public class PageContext {
    public String requestId;
    public String userId;
    public String url;
    public String lang;
    public List<String> keywords;
    public String category;
    public boolean safe;
    public long ts; // event-time in millis

    public PageContext() {} // Jackson needs a no-args constructor

    public PageContext(String requestId, String userId, String url, String lang,
                       List<String> keywords, String category, boolean safe, long ts) {
        this.requestId = requestId;
        this.userId = userId;
        this.url = url;
        this.lang = lang;
        this.keywords = keywords;
        this.category = category;
        this.safe = safe;
        this.ts = ts;
    }

    public long ts() {
        return ts;
    }

    public String userId() {
        return userId;
    }

    public String requestId() {
        return requestId;
    }

    public String lang() {
        return lang;
    }

    public String category() {
        return category;
    }

    public Object keywords() {
        return keywords == null ? null : keywords.toArray(new String[0]);
    }
}
