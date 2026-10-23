package io.example.pageviews.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import io.example.pageviews.model.PageView;

class PageViewTopologyTest {

    @Test
    void countsPerPageAndIgnoresShortVisits() {
        JsonSerde<PageView> serde = new JsonSerde<>(PageView.class).noTypeInfo();

        StreamsBuilder builder = new StreamsBuilder();
        PageViewTopology
                .countPerWindow(
                        builder.stream("in", Consumed.with(Serdes.String(), serde)),
                        100,
                        Duration.ofSeconds(5))
                .to("out", Produced.with(Serdes.String(), Serdes.Long()));

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config)) {
            TestInputTopic<String, PageView> in =
                    driver.createInputTopic("in", new StringSerializer(), serde.serializer());
            TestOutputTopic<String, Long> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new LongDeserializer());

            Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
            in.pipeInput("k", new PageView("home", "v1", 0, 150), t0);
            in.pipeInput("k", new PageView("home", "v2", 0, 200), t0.plusMillis(500));
            in.pipeInput("k", new PageView("home", "v3", 0, 10), t0.plusMillis(900));   // too short
            in.pipeInput("k", new PageView("home", "v4", 0, 300), t0.plusMillis(1200));
            in.pipeInput("k", new PageView("docs", "v1", 0, 120), t0.plusMillis(1500));

            Map<String, Long> latest = out.readKeyValuesToMap();
            assertThat(latest).containsEntry("home", 3L).containsEntry("docs", 1L);
        }
    }
}
