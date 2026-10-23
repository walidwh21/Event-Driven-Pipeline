package io.example.pageviews.web;

import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.example.pageviews.config.PageViewProperties;
import io.example.pageviews.messaging.Topics;
import io.example.pageviews.model.PageView;

/** Publishes ad-hoc page views to Kafka through {@link StreamBridge}. */
@RestController
@RequestMapping("/api/views")
public class ViewPublisherController {

    private static final Set<String> ALLOWED_TOPICS = Set.of(Topics.MANUAL, Topics.AUTO);

    private final StreamBridge bridge;
    private final PageViewProperties props;

    public ViewPublisherController(StreamBridge bridge, PageViewProperties props) {
        this.bridge = bridge;
        this.props = props;
    }

    @PostMapping
    public ResponseEntity<PageView> publish(
            @RequestParam String page,
            @RequestParam(required = false) String visitor,
            @RequestParam(defaultValue = Topics.MANUAL) String topic) {

        if (!ALLOWED_TOPICS.contains(topic)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }

        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        String who = visitor != null ? visitor : props.visitors().get(rnd.nextInt(props.visitors().size()));
        PageView view = PageView.now(page, who, rnd.nextLong(20, 500));

        boolean sent = bridge.send(topic, view);
        return sent
                ? ResponseEntity.status(HttpStatus.ACCEPTED).body(view)
                : ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
    }
}
