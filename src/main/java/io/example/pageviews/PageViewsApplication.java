package io.example.pageviews;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class PageViewsApplication {

    public static void main(String[] args) {
        SpringApplication.run(PageViewsApplication.class, args);
    }
}
