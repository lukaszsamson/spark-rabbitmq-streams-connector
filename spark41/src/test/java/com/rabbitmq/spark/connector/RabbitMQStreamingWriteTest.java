package com.rabbitmq.spark.connector;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitMQStreamingWriteTest {

    @Test
    void useCommitCoordinatorIsDisabled() {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        ConnectorOptions options = new ConnectorOptions(opts);
        StructType schema = new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty())
        });

        RabbitMQStreamingWrite write = new RabbitMQStreamingWrite(
                new RabbitMQDataWriterFactory(options, schema));

        assertThat(write.useCommitCoordinator()).isFalse();
    }
}
