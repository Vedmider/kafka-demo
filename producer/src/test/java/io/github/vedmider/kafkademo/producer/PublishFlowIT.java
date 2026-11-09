package io.github.vedmider.kafkademo.producer;

import io.github.vedmider.kafkademo.common.OrderEvent;
import io.github.vedmider.kafkademo.common.Topics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Publishing via the REST API lands a correctly-keyed OrderEvent on the topic. */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PublishFlowIT {

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Test
    void publishEndpointProducesAKeyedEventToTheTopic() {
        var body = Map.of("key", "customer-9", "type", "ORDER_PLACED", "payload", "a gadget");
        var response = rest.postForEntity("/api/publish", body, Map.class);
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        try (Consumer<String, OrderEvent> consumer = testConsumer()) {
            consumer.subscribe(List.of(Topics.EVENTS));
            ConsumerRecord<String, OrderEvent> record = poll(consumer);
            assertThat(record.key()).isEqualTo("customer-9");
            assertThat(record.value().type()).isEqualTo("ORDER_PLACED");
            assertThat(record.value().payload()).isEqualTo("a gadget");
            assertThat(record.value().eventId()).isNotBlank();
        }
    }

    private Consumer<String, OrderEvent> testConsumer() {
        Map<String, Object> props = new java.util.HashMap<>();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("group.id", "producer-it");
        props.put("auto.offset.reset", "earliest");
        JsonDeserializer<OrderEvent> value = new JsonDeserializer<>(OrderEvent.class);
        value.addTrustedPackages("io.github.vedmider.kafkademo.common");
        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(
                props, new org.apache.kafka.common.serialization.StringDeserializer(), value);
    }

    private static ConsumerRecord<String, OrderEvent> poll(Consumer<String, OrderEvent> consumer) {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, OrderEvent> records = consumer.poll(Duration.ofMillis(500));
            if (!records.isEmpty()) {
                return records.iterator().next();
            }
        }
        throw new AssertionError("no record on topic " + Topics.EVENTS + " within timeout");
    }
}
