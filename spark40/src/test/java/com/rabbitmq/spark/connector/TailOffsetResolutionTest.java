package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.StreamStats;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TailOffsetResolutionTest {

    @Test
    void prefersCommittedOffsetWhenAvailable() {
        StreamStats stats = new TestStats(10, 42, false);

        assertThat(RabbitMQMicroBatchStream.resolveTailOffset(stats)).isEqualTo(43);
        assertThat(RabbitMQScan.resolveTailOffset(stats)).isEqualTo(43);
    }

    @Test
    void fallsBackToCommittedChunkIdWhenCommittedOffsetUnavailable() {
        StreamStats stats = new TestStats(10, 42, true);

        assertThat(RabbitMQMicroBatchStream.resolveTailOffset(stats)).isEqualTo(11);
        assertThat(RabbitMQScan.resolveTailOffset(stats)).isEqualTo(11);
    }

    @Test
    void fallsBackWhenCommittedOffsetThrowsInvocationTargetException() throws Exception {
        StreamStats stats = new FailingStats(10, new RuntimeException("boom"));

        assertThat(RabbitMQMicroBatchStream.resolveTailOffset(stats)).isEqualTo(11);
    }

    @Test
    void fallsBackWhenCommittedOffsetIllegalAccess() throws Exception {
        StreamStats stats = new PrivateCommittedOffsetStats(10, 42);

        assertThat(RabbitMQMicroBatchStream.resolveTailOffset(stats)).isEqualTo(11);
    }

    private static final class TestStats implements StreamStats {
        private final long committedChunkId;
        private final long committedOffset;
        private final boolean throwNoOffsetOnCommittedOffset;

        private TestStats(long committedChunkId, long committedOffset,
                          boolean throwNoOffsetOnCommittedOffset) {
            this.committedChunkId = committedChunkId;
            this.committedOffset = committedOffset;
            this.throwNoOffsetOnCommittedOffset = throwNoOffsetOnCommittedOffset;
        }

        @Override
        public long firstOffset() {
            return 0;
        }

        @Override
        public long committedChunkId() {
            return committedChunkId;
        }

        // Available on newer stream client/broker combinations.
        public long committedOffset() {
            if (throwNoOffsetOnCommittedOffset) {
                throw new NoOffsetException("committedOffset unavailable");
            }
            return committedOffset;
        }
    }

    private static final class FailingStats implements StreamStats {
        private final long committedChunkId;
        private final RuntimeException failure;

        private FailingStats(long committedChunkId, RuntimeException failure) {
            this.committedChunkId = committedChunkId;
            this.failure = failure;
        }

        @Override
        public long firstOffset() {
            return 0;
        }

        @Override
        public long committedChunkId() {
            return committedChunkId;
        }

        public long committedOffset() {
            throw failure;
        }
    }

    private static final class PrivateCommittedOffsetStats implements StreamStats {
        private final long committedChunkId;
        private final long committedOffset;

        private PrivateCommittedOffsetStats(long committedChunkId, long committedOffset) {
            this.committedChunkId = committedChunkId;
            this.committedOffset = committedOffset;
        }

        @Override
        public long firstOffset() {
            return 0;
        }

        @Override
        public long committedChunkId() {
            return committedChunkId;
        }

        @SuppressWarnings("unused")
        private long committedOffset() {
            return committedOffset;
        }
    }
}
