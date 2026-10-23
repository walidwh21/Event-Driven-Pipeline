package io.example.pageviews.web;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import io.example.pageviews.config.PageViewProperties;
import io.example.pageviews.messaging.Topics;
import jakarta.annotation.PreDestroy;

/** Reads the Kafka Streams state store and exposes it as a snapshot and as an SSE feed. */
@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsController.class);

    public record Snapshot(long at, Map<String, Long> counts) {
    }

    private final InteractiveQueryService queries;
    private final PageViewProperties props;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    public AnalyticsController(InteractiveQueryService queries, PageViewProperties props) {
        this.queries = queries;
        this.props = props;
    }

    @GetMapping("/snapshot")
    public Snapshot snapshot() {
        long now = System.currentTimeMillis();
        long windowMs = props.windowSeconds() * 1000L;
        Map<String, Long> counts = new LinkedHashMap<>();
        props.pages().forEach(page -> counts.put(page, 0L));

        try {
            ReadOnlyWindowStore<String, Long> store =
                    queries.getQueryableStore(Topics.COUNT_STORE, QueryableStoreTypes.windowStore());
            for (String page : props.pages()) {
                long latest = 0;
                try (WindowStoreIterator<Long> it =
                             store.fetch(page, Instant.ofEpochMilli(now - windowMs), Instant.ofEpochMilli(now))) {
                    while (it.hasNext()) {
                        latest = it.next().value;
                    }
                }
                counts.put(page, latest);
            }
        } catch (RuntimeException e) {
            // Store not ready yet (e.g. during start-up or rebalance): report zeros.
            log.debug("State store not available yet: {}", e.getMessage());
        }
        return new Snapshot(now, counts);
    }

    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        SseEmitter emitter = new SseEmitter(0L);
        ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(() -> {
            try {
                emitter.send(snapshot());
            } catch (Exception e) {
                emitter.completeWithError(e);
            }
        }, 0, 1, TimeUnit.SECONDS);

        emitter.onCompletion(() -> task.cancel(true));
        emitter.onTimeout(() -> task.cancel(true));
        emitter.onError(t -> task.cancel(true));
        return emitter;
    }

    @PreDestroy
    void shutdown() {
        scheduler.shutdownNow();
    }
}
