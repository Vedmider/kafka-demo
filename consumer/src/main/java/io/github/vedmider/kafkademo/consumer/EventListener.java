package io.github.vedmider.kafkademo.consumer;

import io.github.vedmider.kafkademo.common.OrderEvent;
import io.github.vedmider.kafkademo.common.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumes events into the in-memory store. Two demo failure paths: payload {@code "boom"} always
 * throws (permanent -> DLT); payload {@code "flaky"} throws only on first delivery of an eventId
 * (transient -> recovers to consumed on retry).
 */
@Component
public class EventListener {

    private static final Logger log = LoggerFactory.getLogger(EventListener.class);

    private final ConsumedStore store;
    private final ConcurrentHashMap<String, Integer> flakyAttempts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    public EventListener(ConsumedStore store) {
        this.store = store;
    }

    /** Total onEvent invocations seen for an eventId (incl. retries) — for test assertions. */
    public int attemptsFor(String eventId) {
        AtomicInteger a = attempts.get(eventId);
        return a == null ? 0 : a.get();
    }

    @KafkaListener(topics = Topics.EVENTS, groupId = "${app.consumer.group-id}")
    public void onEvent(OrderEvent event) {
        attempts.computeIfAbsent(event.eventId(), k -> new AtomicInteger()).incrementAndGet();
        if ("boom".equals(event.payload())) {
            throw new IllegalStateException("payload 'boom' — permanent failure, forces the DLT");
        }
        if ("flaky".equals(event.payload())
                && flakyAttempts.merge(event.eventId(), 1, Integer::sum) == 1) {
            throw new IllegalStateException("payload 'flaky' — transient failure, recovers on retry");
        }
        store.addConsumed(event);
        log.info("consumed eventId={} key={} type={}", event.eventId(), event.key(), event.type());
    }

    @KafkaListener(topics = Topics.EVENTS_DLT, groupId = "${app.consumer.group-id}-dlt")
    public void onDeadLetter(OrderEvent event) {
        store.addDeadLettered(event);
        log.warn("dead-lettered eventId={} key={}", event.eventId(), event.key());
    }
}
