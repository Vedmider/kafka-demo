package io.github.vedmider.kafkademo.common;

/** Single source of truth for topic names and partition count, shared by both apps. */
public final class Topics {

    /** Main event topic. 3 partitions so key-based partitioning/ordering is observable. */
    public static final String EVENTS = "kafka-demo.events";

    /** Dead-letter topic: records that fail processing after retries land here. */
    public static final String EVENTS_DLT = "kafka-demo.events.DLT";

    public static final int PARTITIONS = 3;

    private Topics() {
    }
}
