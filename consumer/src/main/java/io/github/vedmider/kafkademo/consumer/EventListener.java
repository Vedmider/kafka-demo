package io.github.vedmider.kafkademo.consumer;

import io.github.vedmider.kafkademo.common.OrderEvent;
import io.github.vedmider.kafkademo.common.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes events into the in-memory store. A payload of {@code "boom"} throws, so after retries the
 * error handler routes it to the DLT — the dead-letter path you can trigger on demand and inspect.
 */
@Component
public class EventListener {

    private static final Logger log = LoggerFactory.getLogger(EventListener.class);

    private final ConsumedStore store;

    public EventListener(ConsumedStore store) {
        this.store = store;
    }

    @KafkaListener(topics = Topics.EVENTS, groupId = "${app.consumer.group-id}")
    public void onEvent(OrderEvent event) {
        if ("boom".equals(event.payload())) {
            throw new IllegalStateException("payload 'boom' — forcing a failure to demo the DLT");
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
