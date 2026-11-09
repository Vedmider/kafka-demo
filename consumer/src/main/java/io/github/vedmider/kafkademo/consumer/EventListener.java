package io.github.vedmider.kafkademo.consumer;

import io.github.vedmider.kafkademo.common.OrderEvent;
import io.github.vedmider.kafkademo.common.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumes events into the in-memory store. Two demo failure paths: payload {@code "boom"} always
 * throws (permanent -> DLT); payload {@code "flaky"} throws only on first delivery of an eventId
 * (transient -> recovers to consumed on retry).
 */
@Component
public class EventListener {

    private static final Logger log = LoggerFactory.getLogger(EventListener.class);

    private final ConsumedStore store;
    private final ConcurrentHashMap<String, Boolean> flakyFailedOnce = new ConcurrentHashMap<>();

    public EventListener(ConsumedStore store) {
        this.store = store;
    }

    @KafkaListener(topics = Topics.EVENTS, groupId = "${app.consumer.group-id}")
    public void onEvent(OrderEvent event) {
        if ("boom".equals(event.payload())) {
            throw new IllegalStateException("payload 'boom' — permanent failure, forces the DLT");
        }
        if ("flaky".equals(event.payload())
                && flakyFailedOnce.putIfAbsent(event.eventId(), Boolean.TRUE) == null) {
            throw new IllegalStateException("payload 'flaky' — transient failure, recovers on retry");
        }
        flakyFailedOnce.remove(event.eventId());
        store.addConsumed(event);
        log.info("consumed eventId={} key={} type={}", event.eventId(), event.key(), event.type());
    }

    // Valid-JSON business failures (e.g. "boom") deserialize and land here; raw poison-pill bytes
    // fail deserialization and remain on the DLT topic only — inspect those via the topic itself.
    @KafkaListener(topics = Topics.EVENTS_DLT, groupId = "${app.consumer.group-id}-dlt")
    public void onDeadLetter(OrderEvent event) {
        store.addDeadLettered(event);
        log.warn("dead-lettered eventId={} key={}", event.eventId(), event.key());
    }
}
