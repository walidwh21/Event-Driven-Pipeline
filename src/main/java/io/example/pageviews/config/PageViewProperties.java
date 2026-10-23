package io.example.pageviews.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Tunable settings, bound from the {@code pageviews.*} namespace.
 */
@ConfigurationProperties(prefix = "pageviews")
public record PageViewProperties(
        @DefaultValue({"home", "pricing", "docs", "blog"}) List<String> pages,
        @DefaultValue({"visitor-a", "visitor-b", "visitor-c"}) List<String> visitors,
        @DefaultValue("100") int minDwellMs,
        @DefaultValue("5") int windowSeconds) {
}
