package io.github.vedmider.kafkademo.consumer;

import io.github.vedmider.kafkademo.common.OrderEvent;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumedStoreTest {

    private final ConsumedStore store = new ConsumedStore();

    @Test
    void listsNewestFirst() {
        store.addConsumed(OrderEvent.create("k", "T", "first"));
        store.addConsumed(OrderEvent.create("k", "T", "second"));
        assertThat(store.all()).extracting(OrderEvent::payload).containsExactly("second", "first");
    }

    @Test
    void filtersByKey() {
        store.addConsumed(OrderEvent.create("a", "T", "1"));
        store.addConsumed(OrderEvent.create("b", "T", "2"));
        assertThat(store.byKey("a")).extracting(OrderEvent::payload).containsExactly("1");
    }

    @Test
    void findsById() {
        OrderEvent e = OrderEvent.create("k", "T", "x");
        store.addConsumed(e);
        assertThat(store.byId(e.eventId())).isPresent();
        assertThat(store.byId("nope")).isEmpty();
    }

    @Test
    void separatesDeadLetters() {
        store.addDeadLettered(OrderEvent.create("k", "T", "boom"));
        assertThat(store.all()).isEmpty();
        assertThat(store.deadLetters()).hasSize(1);
    }

    @Test
    void clearResetsBoth() {
        store.addConsumed(OrderEvent.create("k", "T", "x"));
        store.addDeadLettered(OrderEvent.create("k", "T", "boom"));
        store.clear();
        assertThat(store.all()).isEmpty();
        assertThat(store.deadLetters()).isEmpty();
    }
}
