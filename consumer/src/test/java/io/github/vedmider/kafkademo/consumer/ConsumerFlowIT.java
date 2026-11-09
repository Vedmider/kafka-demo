package io.github.vedmider.kafkademo.consumer;

import io.github.vedmider.kafkademo.common.OrderEvent;
import io.github.vedmider.kafkademo.common.Topics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end against a real Kafka broker: a produced event reaches the in-memory store, and a
 * {@code "boom"} payload is retried then routed to the dead-letter topic.
 */
@Testcontainers
@SpringBootTest
class ConsumerFlowIT {

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Autowired
    private KafkaTemplate<String, OrderEvent> template;

    @Autowired
    private ConsumedStore store;

    @Test
    void producedEventIsConsumedIntoTheStore() {
        OrderEvent event = OrderEvent.create("customer-1", "ORDER_PLACED", "2 widgets");
        template.send(Topics.EVENTS, event.key(), event);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(store.byId(event.eventId())).isPresent());
    }

    @Test
    void boomPayloadIsRoutedToTheDeadLetterTopic() {
        OrderEvent event = OrderEvent.create("customer-2", "ORDER_PLACED", "boom");
        template.send(Topics.EVENTS, event.key(), event);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(store.deadLetters()).extracting(OrderEvent::eventId).contains(event.eventId()));
        assertThat(store.all()).extracting(OrderEvent::eventId).doesNotContain(event.eventId());
    }
}
