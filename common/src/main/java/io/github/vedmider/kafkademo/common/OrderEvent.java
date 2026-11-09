package io.github.vedmider.kafkademo.common;

import java.time.Instant;
import java.util.UUID;

/**
 * The message both apps exchange. {@code key} is the Kafka record key — records with the same key
 * land on the same partition, so you can observe per-key ordering. {@code eventId}/{@code createdAt}
 * are filled by the producer.
 *
 * @param eventId   unique id (producer-assigned) — useful for dedup/lookup
 * @param key       partition key (e.g. a customer id); same key =&gt; same partition =&gt; ordered
 * @param type      event type (e.g. ORDER_PLACED) — room to demonstrate routing/filtering later
 * @param payload   free-text body; the literal {@code "boom"} makes the consumer throw (DLT demo)
 * @param createdAt producer timestamp
 */
public record OrderEvent(String eventId, String key, String type, String payload, Instant createdAt) {

    /** Build an event for a new publish request, assigning id + timestamp. */
    public static OrderEvent create(String key, String type, String payload) {
        return new OrderEvent(UUID.randomUUID().toString(), key, type, payload, Instant.now());
    }
}
