package com.github.tennyros.emailnotificationservice.handler;

import com.github.tennyros.eventmodels.event.ProductCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

    @KafkaHandler
    public void listen(ProductCreatedEvent event) {
        log.info("Received product created event: {}", event.getTitle());
    }
}
