package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.StreamStats;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RabbitMQMicroBatchStreamRealTimeLagMetricsTest {

    @Test
    void mergeOffsetsRefreshesTailOffsetForLagMetricsInRealTimeMode() {
        RabbitMQMicroBatchStream stream = new RabbitMQMicroBatchStream(
                minimalOptions(), new StructType(), "/tmp/rt-lag-metrics-test");
        stream.prepareForRealTimeMode();

        Environment env = mock(Environment.class);
        StreamStats stats = mock(StreamStats.class);
        when(env.queryStreamStats("test-stream")).thenReturn(stats);
        when(env.consumerBuilder()).thenThrow(new IllegalStateException("probe disabled"));
        when(stats.committedOffset()).thenReturn(100L);

        stream.environment = env;
        stream.streams = List.of("test-stream");

        try {
            Offset merged = stream.mergeOffsets(new PartitionOffset[]{
                    new RabbitMQPartitionOffset("test-stream", 42L)
            });

            assertThat(((RabbitMQStreamOffset) merged).getStreamOffsets())
                    .containsEntry("test-stream", 42L);

            Offset latest = stream.reportLatestOffset();
            assertThat(((RabbitMQStreamOffset) latest).getStreamOffsets())
                    .containsEntry("test-stream", 101L);
        } finally {
            stream.stop();
        }
    }

    @Test
    void stopDoesNotPersistRealTimeCachedLatestWithoutCommit() throws IOException {
        Path checkpoint = Files.createTempDirectory("rt-stop-offset-test-");
        RabbitMQMicroBatchStream stream = new RabbitMQMicroBatchStream(
                minimalOptions(), new StructType(), checkpoint.toString());
        stream.prepareForRealTimeMode();

        Environment env = mock(Environment.class);
        stream.environment = env;
        stream.cachedLatestOffset = new RabbitMQStreamOffset(Map.of("test-stream", 42L));

        stream.stop();

        verify(env, never()).storeOffset(anyString(), anyString(), anyLong());
    }

    private static ConnectorOptions minimalOptions() {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        return new ConnectorOptions(opts);
    }
}
