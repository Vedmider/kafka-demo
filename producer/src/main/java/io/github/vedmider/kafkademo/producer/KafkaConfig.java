package io.github.vedmider.kafkademo.producer;

import io.github.vedmider.kafkademo.common.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Creates the demo topic with 3 partitions on startup so key-based partitioning is observable. */
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
}
