package com.github.tennyros.emailnotificationservice.handler;

import com.github.tennyros.emailnotificationservice.exception.NonRetryableException;
import com.github.tennyros.emailnotificationservice.exception.RetryableException;
import com.github.tennyros.eventmodels.event.ProductCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

    private final RestTemplate restTemplate;

    @KafkaHandler
    public void listen(ProductCreatedEvent event) {
        log.info("Received product created event: {}", event.getTitle());

        String url = "http://localhost:8090/response/200";
        try {
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
    }
}
