package io.github.lukaszsamson.spark.rabbitmq.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

/**
 * Structured Streaming pipeline: read from one stream, write to another.
 *
 * <p>Demonstrates micro-batch streaming with backpressure control. Run with:
 * <pre>
 * spark-submit --class com.rabbitmq.spark.examples.StreamingReadWriteExample \
 *   --jars sparkling-rabbit-spark41-&lt;version&gt;.jar \
 *   sparkling-rabbit-examples-&lt;version&gt;.jar
 * </pre>
 */
public class StreamingReadWriteExample {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("RabbitMQ Streaming Read-Write")
                .getOrCreate();

        // Read stream with backpressure
        Dataset<Row> source = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", "localhost:5552")
                .option("stream", "input-stream")
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", "1000")
                .option("metadataFields", "")  // only fixed columns
                .load();

        // Transform: select value and offset
        Dataset<Row> transformed = source.selectExpr(
                "value", "offset", "stream"
        );

        // Write to output stream
        StreamingQuery query = transformed.writeStream()
                .format("rabbitmq_streams")
                .option("endpoints", "localhost:5552")
                .option("stream", "output-stream")
                .option("checkpointLocation", "/tmp/spark-checkpoint/streaming-example")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        System.out.println("Streaming query started: " + query.id());
        System.out.println("Checkpoint: /tmp/spark-checkpoint/streaming-example");

        // Wait for termination (Ctrl+C)
        query.awaitTermination();

        spark.stop();
    }
}
