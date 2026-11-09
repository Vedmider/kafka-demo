package io.github.vedmider.kafkademo.producer;

import io.github.vedmider.kafkademo.common.OrderEvent;
import io.github.vedmider.kafkademo.common.Topics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

import java.util.Map;

/** Publishes events to Kafka. The record {@code key} drives partition assignment. */
@RestController
@RequestMapping("/api/publish")
public class PublishController {

    private final KafkaTemplate<String, OrderEvent> kafka;

    public PublishController(KafkaTemplate<String, OrderEvent> kafka) {
        this.kafka = kafka;
    }

    @PostMapping
    public Map<String, Object> publish(@RequestBody PublishRequest req) {
        if (!StringUtils.hasText(req.key()) || !StringUtils.hasText(req.payload())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "key and payload are required");
        }
        OrderEvent event = OrderEvent.create(req.key(), req.type(), req.payload());
        kafka.send(Topics.EVENTS, event.key(), event);
        return Map.of("published", true, "eventId", event.eventId(), "key", event.key());
    }

    public record PublishRequest(String key, String type, String payload) {
    }
}
