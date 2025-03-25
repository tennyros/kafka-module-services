package com.github.tennyros.productservice.integration.kafka.producer;

import com.github.tennyros.eventmodels.event.ProductCreatedEvent;
import com.github.tennyros.productservice.service.ProductService;
import com.github.tennyros.productservice.service.dto.CreateProductDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.kafka.support.serializer.ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS;
import static org.springframework.kafka.support.serializer.JsonDeserializer.TRUSTED_PACKAGES;

@DirtiesContext
@ActiveProfiles("test")
@TestInstance(Lifecycle.PER_CLASS)
@EmbeddedKafka(partitions = 3, count = 3, controlledShutdown = true)
@SpringBootTest(properties = "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}")
class ProductServiceIT {

    @Autowired
    private ProductService productService;

    @Autowired
    private Environment env;

    @Autowired
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private KafkaMessageListenerContainer<String, ProductCreatedEvent> container;
    private BlockingQueue<ConsumerRecord<String, ProductCreatedEvent>> records;

    @BeforeAll
    void setUp() {
        setupKafkaConsumer();
        startConsumerContainer();
    }

    private void setupKafkaConsumer() {
        var consumerFactory = new DefaultKafkaConsumerFactory<>(getConsumerProperties());
        var containerProperties = new ContainerProperties(getTopicName());

        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        records = new LinkedBlockingQueue<>();
        container.setupMessageListener((MessageListener<String, ProductCreatedEvent>) records::add);
    }

    private String getTopicName() {
        return env.getProperty("product.kafka.events.topic-name");
    }

    private void startConsumerContainer() {
        container.start();
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
    }

    @Test
    void testCreateProduct_whenGivenValidProductDetails_successfullySendKafkaMessage() throws ExecutionException, InterruptedException {

        var createProductDto = CreateProductDto.builder()
                .title("Samsung Galaxy")
                .price(new BigDecimal(1000))
                .quantity(1)
                .build();

        productService.createProduct(createProductDto);

        ConsumerRecord<String, ProductCreatedEvent> message = records.poll(3000L, MILLISECONDS);

        assertNotNull(message, "Kafka message not received");
        assertNotNull(message.key(), "Kafka message key not received");
        ProductCreatedEvent productCreatedEvent = message.value();
        assertEquals(createProductDto.getQuantity(), productCreatedEvent.getQuantity());
        assertEquals(createProductDto.getTitle(), productCreatedEvent.getTitle());
        assertEquals(createProductDto.getPrice(), productCreatedEvent.getPrice());

    }

    private Map<String, Object> getConsumerProperties() {
        return Map.of(BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString(),
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class,
                VALUE_DESERIALIZER_CLASS, JsonDeserializer.class,
                GROUP_ID_CONFIG, env.getProperty("spring.kafka.consumer.properties.group.id"),
                TRUSTED_PACKAGES, env.getProperty("spring.kafka.consumer.properties.spring.json.trusted.packages"),
                AUTO_OFFSET_RESET_CONFIG, env.getProperty("spring.kafka.consumer.properties.auto.offset.reset")
        );
    }

    @AfterAll
    void shutdown() {
        container.stop();
    }

}
