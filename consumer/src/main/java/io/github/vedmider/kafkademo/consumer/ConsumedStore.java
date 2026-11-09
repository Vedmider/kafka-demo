package io.github.vedmider.kafkademo.consumer;

import io.github.vedmider.kafkademo.common.OrderEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

/**
 * In-memory, thread-safe record of what the consumer has seen — the thing the REST API inspects.
 * Two buckets: successfully consumed events and dead-lettered ones. Newest-first on read.
 *
 * <p>Deliberately non-durable: restart the consumer and it is empty. That is correct for a
 * learning tool (the point is to watch messages flow), not a storage design.
 */
@Component
public class ConsumedStore {

    private final Deque<OrderEvent> consumed = new ArrayDeque<>();
    private final Deque<OrderEvent> deadLettered = new ArrayDeque<>();

    public synchronized void addConsumed(OrderEvent event) {
        consumed.addFirst(event);
    }

    public synchronized void addDeadLettered(OrderEvent event) {
        deadLettered.addFirst(event);
    }

    public synchronized List<OrderEvent> all() {
        return new ArrayList<>(consumed);
    }

    public synchronized List<OrderEvent> byKey(String key) {
        return consumed.stream().filter(e -> e.key().equals(key)).toList();
    }

    public synchronized Optional<OrderEvent> byId(String eventId) {
        return consumed.stream().filter(e -> e.eventId().equals(eventId)).findFirst();
    }

    public synchronized List<OrderEvent> deadLetters() {
        return new ArrayList<>(deadLettered);
    }

    public synchronized void clear() {
        consumed.clear();
        deadLettered.clear();
    }
}
