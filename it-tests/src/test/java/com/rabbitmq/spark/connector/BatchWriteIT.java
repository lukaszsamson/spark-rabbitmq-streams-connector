package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Message;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for batch writes via the {@code rabbitmq_streams} DataSource.
 */
class BatchWriteIT extends AbstractRabbitMQIT {

    private String stream;

    @BeforeEach
    void setUp() {
        stream = uniqueStreamName();
        createStream(stream);
    }

    @AfterEach
    void tearDown() {
        deleteStream(stream);
    }

    @Test
    void batchWriteAndReadBack() {
        // Create a DataFrame with value column
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            data.add(RowFactory.create(("write-test-" + i).getBytes()));
        }

        Dataset<Row> df = spark.createDataFrame(data, schema);

        // Write via connector
        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .save();

        // Read back from broker and verify
        List<Message> messages = consumeMessages(stream, 20);
        assertThat(messages).hasSize(20);

        List<String> bodies = messages.stream()
                .map(msg -> new String(msg.getBodyAsBinary()))
                .sorted()
                .toList();

        assertThat(bodies).contains("write-test-0", "write-test-1", "write-test-19");
    }

    @Test
    void batchWriteWithApplicationProperties() {
        // Create DataFrame with value and application_properties
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("application_properties",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                        true);

        List<Row> data = new ArrayList<>();
        data.add(RowFactory.create(
                "msg-with-props".getBytes(),
                java.util.Map.of("color", "blue", "priority", "high")));

        Dataset<Row> df = spark.createDataFrame(data, schema);

        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .save();

        // Read back and verify application properties
        List<Message> messages = consumeMessages(stream, 1);
        assertThat(messages).hasSize(1);

        Message msg = messages.get(0);
        assertThat(new String(msg.getBodyAsBinary())).isEqualTo("msg-with-props");
        assertThat(msg.getApplicationProperties())
                .containsEntry("color", "blue")
                .containsEntry("priority", "high");
    }

    @Test
    void batchWriteThenBatchRead() {
        // Write via connector
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            data.add(RowFactory.create(("roundtrip-" + i).getBytes()));
        }

        spark.createDataFrame(data, schema)
                .write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .save();

        // Verify messages arrived via broker client
        var messages = consumeMessages(stream, 30);
        assertThat(messages).hasSize(30);

        List<String> values = messages.stream()
                .map(msg -> new String(msg.getBodyAsBinary()))
                .sorted()
                .toList();

        assertThat(values).contains("roundtrip-0", "roundtrip-15", "roundtrip-29");
    }
}
