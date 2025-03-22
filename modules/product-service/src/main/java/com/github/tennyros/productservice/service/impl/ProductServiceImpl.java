package com.github.tennyros.productservice.service.impl;

import com.github.tennyros.eventmodels.event.ProductCreatedEvent;
import com.github.tennyros.productservice.service.ProductService;
import com.github.tennyros.productservice.service.dto.CreateProductDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProductServiceImpl implements ProductService {

    private final KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

    @Override
    public String createProduct(CreateProductDto createProductDto) throws ExecutionException, InterruptedException {
        // TODO: 03/19/2025 save to DB
        String productId = UUID.randomUUID().toString();

        ProductCreatedEvent productCreatedEvent = ProductCreatedEvent.builder()
                .productId(productId)
                .title(createProductDto.getTitle())
                .price(createProductDto.getPrice())
                .quantity(createProductDto.getQuantity())
                .build();

        ProducerRecord<String, ProductCreatedEvent> productCreatedRecord = new ProducerRecord<>(
                "product-created-events-topic",
                productId,
                productCreatedEvent
        );

        productCreatedRecord.headers().add("messageId", "message-id".getBytes());

        SendResult<String, ProductCreatedEvent> result = kafkaTemplate
                .send(productCreatedRecord).get();

        log.info("Topic: {}", result.getRecordMetadata().topic());
        log.info("Partition: {}", result.getRecordMetadata().partition());
        log.info("Offset: {}", result.getRecordMetadata().offset());

        log.info("Return: {}", productId);

        return productId;
    }
}
