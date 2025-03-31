package com.github.tennyros.productservice.integration.kafka.producer;

import com.github.tennyros.eventmodels.event.ProductCreatedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class IdempotentProducerIT {

    @Autowired
    private KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

    @MockitoBean
    private KafkaAdmin kafkaAdmin;

    @Test
    void testProducerConfig_whenIdempotenceEnabled_assertsIdempotentProperties() {
        ProducerFactory<String, ProductCreatedEvent> producerFactory = kafkaTemplate.getProducerFactory();
        Map<String, Object> config = producerFactory.getConfigurationProperties();

        assertAll(
                () -> assertEquals("true", config.get(ENABLE_IDEMPOTENCE_CONFIG)),
                () -> assertTrue("all".equalsIgnoreCase(config.get(ACKS_CONFIG).toString())),
                () -> {
                    if (config.containsKey(RETRIES_CONFIG)) {
                        assertTrue(Integer.parseInt(config.get(RETRIES_CONFIG).toString()) > 0);
                    }
                }
        );
    }
}
