package io.example.pageviews.messaging;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.kafka.support.serializer.JsonSerde;

import io.example.pageviews.model.PageView;

/**
 * Pure stream-processing logic, kept separate from Spring wiring so it can be
 * tested with {@code TopologyTestDriver}.
 */
public final class PageViewTopology {

    private PageViewTopology() {
    }

    /**
     * Drops short visits, then counts views per page in tumbling windows.
     * Counts are also materialised in {@link Topics#COUNT_STORE} for interactive queries.
     */
    public static KStream<String, Long> countPerWindow(
            KStream<String, PageView> views, int minDwellMs, Duration window) {

        JsonSerde<PageView> valueSerde = new JsonSerde<>(PageView.class)
                .trustedPackages(PageView.class.getPackageName())
                .noTypeInfo();

        return views
                .filter((key, view) -> view != null && view.dwellMs() >= minDwellMs)
                .groupBy((key, view) -> view.page(), Grouped.with(Serdes.String(), valueSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(window))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(Topics.COUNT_STORE))
                .toStream()
                .map((windowedKey, count) -> KeyValue.pair(windowedKey.key(), count));
    }
}
