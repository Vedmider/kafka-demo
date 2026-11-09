package io.github.vedmider.kafkademo.consumer;

import io.github.vedmider.kafkademo.common.OrderEvent;
import io.github.vedmider.kafkademo.common.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Declares the topics (so the 3-partition topic exists regardless of which app starts first — a
 * cold start otherwise lets the consumer subscribe to a not-yet-created topic and get a partial
 * assignment) and the DLT error handler.
 */
@Configuration
public class KafkaConfig {

    @Bean
    NewTopic eventsTopic() {
        return TopicBuilder.name(Topics.EVENTS).partitions(Topics.PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic eventsDltTopic() {
        return TopicBuilder.name(Topics.EVENTS_DLT).partitions(Topics.PARTITIONS).replicas(1).build();
    }

    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, OrderEvent> kafka) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, ex) -> new TopicPartition(Topics.EVENTS_DLT, -1));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(500L, 2L));
    }
}
