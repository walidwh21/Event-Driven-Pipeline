package io.example.pageviews.messaging;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.example.pageviews.config.PageViewProperties;
import io.example.pageviews.model.PageView;

/**
 * Functional beans picked up by Spring Cloud Stream.
 * Binding names follow {@code <bean>-in-0} / {@code <bean>-out-0}; see application.yml.
 */
@Configuration
public class PageViewFunctions {

    private static final Logger log = LoggerFactory.getLogger(PageViewFunctions.class);

    /** Sink: logs every manually published view. */
    @Bean
    public Consumer<PageView> logViews() {
        return view -> log.info("received view: page={} visitor={} dwell={}ms",
                view.page(), view.visitor(), view.dwellMs());
    }

    /** Source: emits a random view on every poll of the default poller. */
    @Bean
    public Supplier<PageView> autoViews(PageViewProperties props) {
        return () -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            return PageView.now(
                    pick(props.pages(), rnd),
                    pick(props.visitors(), rnd),
                    rnd.nextLong(20, 500));
        };
    }

    /** Processor: windowed per-page counts, from views-auto to views-counts. */
    @Bean
    public Function<KStream<String, PageView>, KStream<String, Long>> windowedCounts(PageViewProperties props) {
        return views -> PageViewTopology.countPerWindow(
                views, props.minDwellMs(), Duration.ofSeconds(props.windowSeconds()));
    }

    private static String pick(List<String> values, ThreadLocalRandom rnd) {
        return values.get(rnd.nextInt(values.size()));
    }
}
