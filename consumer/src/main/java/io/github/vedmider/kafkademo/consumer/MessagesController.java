package io.github.vedmider.kafkademo.consumer;

import io.github.vedmider.kafkademo.common.OrderEvent;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

/** Inspect what the consumer has seen: list all / by key / one by id, plus the dead letters. */
@RestController
@RequestMapping("/api")
public class MessagesController {

    private final ConsumedStore store;

    public MessagesController(ConsumedStore store) {
        this.store = store;
    }

    /** All consumed events (newest first), or only those with the given {@code key}. */
    @GetMapping("/messages")
    public List<OrderEvent> messages(@RequestParam(required = false) String key) {
        return key == null ? store.all() : store.byKey(key);
    }

    @GetMapping("/messages/{eventId}")
    public OrderEvent message(@PathVariable String eventId) {
        return store.byId(eventId).orElseThrow(
                () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "no such eventId: " + eventId));
    }

    /** Events that failed processing and were routed to the dead-letter topic. */
    @GetMapping("/dlt")
    public List<OrderEvent> deadLetters() {
        return store.deadLetters();
    }

    /** Reset the in-memory store between experiments. */
    @DeleteMapping("/messages")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void clear() {
        store.clear();
    }
}
