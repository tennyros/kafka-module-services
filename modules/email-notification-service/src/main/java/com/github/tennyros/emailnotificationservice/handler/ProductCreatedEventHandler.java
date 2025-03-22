package com.github.tennyros.emailnotificationservice.handler;

import com.github.tennyros.emailnotificationservice.exception.NonRetryableException;
import com.github.tennyros.emailnotificationservice.exception.RetryableException;
import com.github.tennyros.emailnotificationservice.persistence.entity.ProcessedEventEntity;
import com.github.tennyros.emailnotificationservice.persistence.repository.ProcessedEventRepository;
import com.github.tennyros.eventmodels.event.ProductCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

    private final RestTemplate restTemplate;
    private final ProcessedEventRepository processedEventRepository;

    @KafkaHandler
    @Transactional
    public void handle(@Payload ProductCreatedEvent productCreatedEvent,
                       @Header("messageId") String messageId,
                       @Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {

        log.info("Received product created event: {}, productId: {}", productCreatedEvent.getTitle(), productCreatedEvent.getProductId());

        ProcessedEventEntity eventEntity = processedEventRepository.findByMessageId(messageId);

        if (eventEntity != null) {
            log.info("Duplicate message id: {}", messageId);
            return;
        }

        try {
            String url = "http://localhost:8090/response/200";
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, null, String.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("Received response: {}", response.getBody());
            }
        } catch (ResourceAccessException e) {
            log.error("ResourceAccessException: {}", e.getMessage());
            throw new RetryableException(e);
        } catch (HttpServerErrorException e) {
            log.error("HttpServerErrorException: {}", e.getMessage());
            throw new NonRetryableException(e);
        } catch (Exception e) {
            log.error("Exception: {}", e.getMessage());
            throw new NonRetryableException(e);
        }

        try {
            processedEventRepository.save(new ProcessedEventEntity(messageId, productCreatedEvent.getProductId()));
        } catch (DataIntegrityViolationException e) {
            log.error(e.getMessage());
            throw new NonRetryableException(e);
        }
    }
}
