package io.github.vedmider.kafkademo.consumer;

import io.github.vedmider.kafkademo.common.Topics;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Consumer-group and offset semantics driven with raw clients (no app listener), so the behavior is
 * deterministic and independent of the app's single consumer.
 */
@Testcontainers
class ConsumerGroupIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @BeforeAll
    static void createTopic() throws Exception {
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(Topics.EVENTS, Topics.PARTITIONS, (short) 1)))
                    .all().get();
        }
    }

    private static void produce(int count, String keyPrefix) {
        try (KafkaProducer<String, String> p = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
                new StringSerializer(), new StringSerializer())) {
            IntStream.range(0, count).forEach(i ->
                    p.send(new ProducerRecord<>(Topics.EVENTS, keyPrefix + "-" + i, "v" + i)));
            p.flush();
        }
    }

    private static KafkaConsumer<String, String> consumer(String group) {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, group,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"),
                new StringDeserializer(), new StringDeserializer());
    }

    // B1: two consumers, one group => each record delivered to exactly one of them (no overlap).
    @Test
    void oneGroupSplitsPartitionsNoDuplicateAcrossMembers() throws Exception {
        int n = 30;
        String prefix = "g1-" + UUID.randomUUID();
        produce(n, prefix);
        String group = "split-" + UUID.randomUUID();
        Set<String> seenA = ConcurrentHashMap.newKeySet();
        Set<String> seenB = ConcurrentHashMap.newKeySet();

        // Poll both members concurrently so the group rebalances across both before we assert.
        try (var a = consumer(group); var b = consumer(group)) {
            a.subscribe(List.of(Topics.EVENTS));
            b.subscribe(List.of(Topics.EVENTS));
            var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
            long deadline = System.currentTimeMillis() + 30_000;
            Runnable pollA = () -> { while (System.currentTimeMillis() < deadline) drainMatching(a, seenA, prefix); };
            Runnable pollB = () -> { while (System.currentTimeMillis() < deadline) drainMatching(b, seenB, prefix); };
            pool.submit(pollA);
            pool.submit(pollB);
            await().atMost(Duration.ofSeconds(35)).untilAsserted(() ->
                    assertThat(seenA.size() + seenB.size()).isEqualTo(n));
            pool.shutdownNow();
            pool.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
        }
        assertThat(seenA.stream().anyMatch(seenB::contains)).isFalse();   // no record consumed by both
        Set<String> union = ConcurrentHashMap.newKeySet();
        union.addAll(seenA);
        union.addAll(seenB);
        assertThat(union).hasSize(n);
    }

    // B2: two different groups => each group independently sees every record.
    @Test
    void differentGroupsEachReceiveEverything() {
        int n = 15;
        produce(n, "g2-" + UUID.randomUUID());
        Set<String> g1 = ConcurrentHashMap.newKeySet();
        Set<String> g2 = ConcurrentHashMap.newKeySet();

        try (var c1 = consumer("grpA-" + UUID.randomUUID()); var c2 = consumer("grpB-" + UUID.randomUUID())) {
            c1.subscribe(List.of(Topics.EVENTS));
            c2.subscribe(List.of(Topics.EVENTS));
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                drain(c1, g1);
                drain(c2, g2);
                assertThat(g1.size()).isGreaterThanOrEqualTo(n);
                assertThat(g2.size()).isGreaterThanOrEqualTo(n);
            });
        }
    }

    // C1: produce before subscribe, earliest reset still reads from offset 0.
    @Test
    void earliestResetReadsRecordsProducedBeforeSubscribe() {
        int n = 8;
        String prefix = "c1-" + UUID.randomUUID();
        produce(n, prefix);
        Set<String> seen = ConcurrentHashMap.newKeySet();

        try (var c = consumer("late-" + UUID.randomUUID())) {
            c.subscribe(List.of(Topics.EVENTS));
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                drain(c, seen);
                assertThat(seen.stream().filter(k -> k.startsWith(prefix)).count()).isEqualTo(n);
            });
        }
    }

    private static void drain(KafkaConsumer<String, String> c, Set<String> into) {
        ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> r : records) {
            into.add(r.key());
        }
    }

    private static void drainMatching(KafkaConsumer<String, String> c, Set<String> into, String prefix) {
        ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(300));
        for (ConsumerRecord<String, String> r : records) {
            if (r.key() != null && r.key().startsWith(prefix)) {
                into.add(r.key());
            }
        }
    }
}
