package com.rabbitmq.spark.connector;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MessageSizeTracker} averaging and reset behavior
 * without requiring a broker.
 */
class MessageSizeTrackerTest {

    @Test
    void recordAccumulatesBytesAndRecords() {
        MessageSizeTracker.record(100, 2);
        MessageSizeTracker.record(50, 1);

        int average = MessageSizeTracker.drainAverage(1024);

        assertThat(average).isEqualTo(50);
    }

    @Test
    void drainAverageReturnsCurrentEstimateWhenNoRecords() {
        int average = MessageSizeTracker.drainAverage(2048);

        assertThat(average).isEqualTo(2048);
    }

    @Test
    void drainAverageClampsMinimumToOne() {
        MessageSizeTracker.record(0, 5);

        int average = MessageSizeTracker.drainAverage(100);

        assertThat(average).isEqualTo(1);
    }

    @Test
    void drainAverageResetsCountersBetweenCalls() {
        MessageSizeTracker.record(10, 1);

        int first = MessageSizeTracker.drainAverage(10);
        int second = MessageSizeTracker.drainAverage(10);

        assertThat(first).isEqualTo(10);
        assertThat(second).isEqualTo(10);
    }
}
