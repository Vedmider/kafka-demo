package io.github.vedmider.kafkademo.consumer;

import io.github.vedmider.kafkademo.common.OrderEvent;
import io.github.vedmider.kafkademo.common.Topics;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Error handling: a failed record is retried a few times, then published to {@link Topics#EVENTS_DLT}
 * (keeping the original key/partition) instead of blocking the partition forever.
 */
@Configuration
public class KafkaConfig {

    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, OrderEvent> kafka) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, ex) -> new TopicPartition(Topics.EVENTS_DLT, -1));
        // 2 retries, 500ms apart, then route to the DLT.
        return new DefaultErrorHandler(recoverer, new FixedBackOff(500L, 2L));
    }
}
