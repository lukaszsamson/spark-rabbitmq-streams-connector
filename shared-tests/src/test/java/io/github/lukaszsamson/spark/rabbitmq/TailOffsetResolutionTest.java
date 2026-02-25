package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.StreamStats;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TailOffsetResolutionTest {

    @Test
    void prefersCommittedOffsetWhenAvailable() {
        StreamStats stats = new TestStats(10, 42, false, false);

        assertThat(RabbitMQMicroBatchStream.resolveTailOffset(stats)).isEqualTo(43);
        assertThat(RabbitMQScan.resolveTailOffset(stats)).isEqualTo(43);
    }

    @Test
    void returnsZeroWhenCommittedOffsetUnavailable() {
        StreamStats stats = new TestStats(10, 42, true, false);

        assertThat(RabbitMQMicroBatchStream.resolveTailOffset(stats)).isZero();
        assertThat(RabbitMQScan.resolveTailOffset(stats)).isZero();
    }

    @Test
    void returnsZeroWhenCommittedOffsetFailsUnexpectedly() {
        StreamStats stats = new TestStats(10, 42, false, true);

        assertThat(RabbitMQMicroBatchStream.resolveTailOffset(stats)).isZero();
        assertThat(RabbitMQScan.resolveTailOffset(stats)).isZero();
    }

    @Test
    void returnsZeroWhenBothTailSourcesHaveNoOffsets() {
        StreamStats stats = new EmptyStats();

        assertThat(RabbitMQMicroBatchStream.resolveTailOffset(stats)).isZero();
        assertThat(RabbitMQScan.resolveTailOffset(stats)).isZero();
    }

    private static final class TestStats implements StreamStats {
        private final long committedChunkId;
        private final long committedOffset;
        private final boolean throwNoOffsetOnCommittedOffset;
        private final boolean throwRuntimeOnCommittedOffset;

        private TestStats(long committedChunkId, long committedOffset,
                          boolean throwNoOffsetOnCommittedOffset,
                          boolean throwRuntimeOnCommittedOffset) {
            this.committedChunkId = committedChunkId;
            this.committedOffset = committedOffset;
            this.throwNoOffsetOnCommittedOffset = throwNoOffsetOnCommittedOffset;
            this.throwRuntimeOnCommittedOffset = throwRuntimeOnCommittedOffset;
        }

        @Override
        public long firstOffset() {
            return 0;
        }

        @Override
        public long committedChunkId() {
            return committedChunkId;
        }

        @Override
        public long committedOffset() {
            if (throwNoOffsetOnCommittedOffset) {
                throw new NoOffsetException("committedOffset unavailable");
            }
            if (throwRuntimeOnCommittedOffset) {
                throw new RuntimeException("boom");
            }
            return committedOffset;
        }
    }

    private static final class EmptyStats implements StreamStats {
        @Override
        public long firstOffset() {
            throw new NoOffsetException("empty");
        }

        @Override
        public long committedChunkId() {
            throw new NoOffsetException("empty");
        }

        @Override
        public long committedOffset() {
            throw new NoOffsetException("empty");
        }
    }
}
