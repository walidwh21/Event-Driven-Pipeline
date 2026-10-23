package io.example.pageviews.messaging;

/** Kafka topic names and the state-store name shared across the app. */
public final class Topics {

    public static final String MANUAL = "views-manual";
    public static final String AUTO = "views-auto";
    public static final String COUNTS = "views-counts";
    public static final String COUNT_STORE = "page-counts-store";

    private Topics() {
    }
}
