package com.github.tennyros.productservice.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@Getter
@RequiredArgsConstructor
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaPropertiesConfig {

    private final Map<String, String> topics;
    private final int partitions;
    private final int replicas;

}
