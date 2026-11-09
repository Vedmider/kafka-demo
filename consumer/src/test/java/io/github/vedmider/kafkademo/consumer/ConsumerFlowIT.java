package io.github.vedmider.kafkademo.consumer;

import io.github.vedmider.kafkademo.common.OrderEvent;
import io.github.vedmider.kafkademo.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Real-broker integration against the app's own consumer. Each test uses unique keys/eventIds so
 * methods can't collide on the shared topic/consumer within the class.
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

    private static OrderEvent event(String key, String type, String payload) {
        return new OrderEvent(UUID.randomUUID().toString(), key, type, payload, Instant.now());
    }

    @Test
    void producedEventIsConsumedIntoTheStore() {
        OrderEvent e = event("happy-" + UUID.randomUUID(), "ORDER_PLACED", "2 widgets");
        template.send(Topics.EVENTS, e.key(), e);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(store.byId(e.eventId())).isPresent());
    }

    // A1
    @Test
    void sameKeyPreservesOrder() {
        String key = "order-" + UUID.randomUUID();
        List<OrderEvent> sent = IntStream.range(0, 5)
                .mapToObj(i -> event(key, "T", "msg-" + i)).toList();
        sent.forEach(e -> template.send(Topics.EVENTS, e.key(), e));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(store.byKey(key)).hasSize(5));
        // store is newest-first; reverse to get send order
        List<OrderEvent> got = store.byKey(key);
        List<String> inSendOrder = got.stream().map(OrderEvent::payload)
                .collect(java.util.stream.Collectors.collectingAndThen(
                        java.util.stream.Collectors.toList(), l -> { java.util.Collections.reverse(l); return l; }));
        assertThat(inSendOrder).containsExactly("msg-0", "msg-1", "msg-2", "msg-3", "msg-4");
    }

    // A2
    @Test
    void crossKeyFanOutLosesNothing() {
        List<OrderEvent> sent = IntStream.range(0, 10)
                .mapToObj(i -> event("fan-" + UUID.randomUUID(), "T", "p")).toList();
        sent.forEach(e -> template.send(Topics.EVENTS, e.key(), e));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(store.all().stream().map(OrderEvent::eventId))
                        .containsAll(sent.stream().map(OrderEvent::eventId).toList()));
    }

    // A3
    @Test
    void duplicateEventIdIsStoredTwiceAtLeastOnce() {
        OrderEvent e = event("dup-" + UUID.randomUUID(), "T", "p");
        template.send(Topics.EVENTS, e.key(), e);
        template.send(Topics.EVENTS, e.key(), e);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(store.all().stream().filter(x -> x.eventId().equals(e.eventId())).count())
                        .isEqualTo(2));
    }

    // A4
    @Test
    void throughputAllMessagesLand() {
        String tag = "bulk-" + UUID.randomUUID();
        List<OrderEvent> sent = IntStream.range(0, 100)
                .mapToObj(i -> event(tag + "-" + i, "T", "p")).toList();
        sent.forEach(e -> template.send(Topics.EVENTS, e.key(), e));

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() ->
                assertThat(store.all().stream().map(OrderEvent::eventId))
                        .containsAll(sent.stream().map(OrderEvent::eventId).toList()));
    }

    // D1
    @Test
    void permanentFailureIsRetriedThenDeadLettered() {
        OrderEvent e = event("boom-" + UUID.randomUUID(), "T", "boom");
        template.send(Topics.EVENTS, e.key(), e);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(store.deadLetters()).extracting(OrderEvent::eventId).contains(e.eventId()));
        assertThat(store.all()).extracting(OrderEvent::eventId).doesNotContain(e.eventId());
    }

    // D2
    @Test
    void deadLetterRetainsKeyAndPayload() {
        OrderEvent e = event("dlt-" + UUID.randomUUID(), "T", "boom");
        template.send(Topics.EVENTS, e.key(), e);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(store.deadLetters()).anySatisfy(d -> {
                    assertThat(d.eventId()).isEqualTo(e.eventId());
                    assertThat(d.key()).isEqualTo(e.key());
                    assertThat(d.payload()).isEqualTo("boom");
                }));
    }

    // D3
    @Test
    void transientFailureRecoversToConsumed() {
        OrderEvent e = event("flaky-" + UUID.randomUUID(), "T", "flaky");
        template.send(Topics.EVENTS, e.key(), e);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(store.byId(e.eventId())).isPresent());
        assertThat(store.deadLetters()).extracting(OrderEvent::eventId).doesNotContain(e.eventId());
    }

    // D4 + D5
    @Test
    void poisonPillIsNotConsumedAndDoesNotBlockLaterGoodRecords() {
        try (KafkaProducer<String, byte[]> raw = rawByteProducer()) {
            raw.send(new ProducerRecord<>(Topics.EVENTS, "poison", "{not valid json".getBytes()));
            raw.flush();
        }
        OrderEvent good = event("after-poison-" + UUID.randomUUID(), "T", "fine");
        template.send(Topics.EVENTS, good.key(), good);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(store.byId(good.eventId())).isPresent());
        assertThat(store.all()).noneMatch(e -> "{not valid json".equals(e.payload()));
    }

    // E1
    @Test
    void unknownExtraJsonFieldStillDeserializes() {
        String id = UUID.randomUUID().toString();
        String key = "extra-" + id;
        String json = "{\"eventId\":\"" + id + "\",\"key\":\"" + key + "\",\"type\":\"T\","
                + "\"payload\":\"p\",\"createdAt\":\"2026-01-01T00:00:00Z\",\"somethingNew\":123}";
        try (KafkaProducer<String, String> raw = rawStringProducer()) {
            raw.send(new ProducerRecord<>(Topics.EVENTS, key, json));
            raw.flush();
        }
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(store.byId(id)).isPresent());
    }

    // E2
    @Test
    void nullTypeRoundTrips() {
        OrderEvent e = event("nulltype-" + UUID.randomUUID(), null, "p");
        template.send(Topics.EVENTS, e.key(), e);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(store.byId(e.eventId())).isPresent()
                        .get().extracting(OrderEvent::type).isNull());
    }

    private KafkaProducer<String, byte[]> rawByteProducer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
                new StringSerializer(), new ByteArraySerializer());
    }

    private KafkaProducer<String, String> rawStringProducer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
                new StringSerializer(), new StringSerializer());
    }
}
