package com.github.tennyros.productservice.config;

import com.github.tennyros.eventmodels.event.ProductCreatedEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(KafkaPropertiesConfig.class)
public class KafkaConfig {

    private final KafkaPropertiesConfig kafkaProperties;

    @Bean
    ProducerFactory<String, ProductCreatedEvent> producerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> conf = new HashMap<>(kafkaProperties.buildProducerProperties());
        return new DefaultKafkaProducerFactory<>(conf);
    }

    @Bean
    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate(ProducerFactory<String, ProductCreatedEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public NewTopic createTopic() {
        return TopicBuilder.name(kafkaProperties.getTopics().get("product-created"))
                .partitions(kafkaProperties.getPartitions())
                .replicas(kafkaProperties.getReplicas())
                .configs(Map.of("min.insync.replicas", "2"))
                .build();
    }
}
