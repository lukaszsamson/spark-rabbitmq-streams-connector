package io.github.lukaszsamson.spark.rabbitmq.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Batch read from a RabbitMQ stream.
 *
 * <p>Reads all messages from a stream and displays them. Run with:
 * <pre>
 * spark-submit --class com.rabbitmq.spark.examples.BatchReadExample \
 *   --jars sparkling-rabbit-spark41-&lt;version&gt;.jar \
 *   sparkling-rabbit-examples-&lt;version&gt;.jar
 * </pre>
 */
public class BatchReadExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("RabbitMQ Batch Read")
                .getOrCreate();

        // Read all messages from the stream
        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", "localhost:5552")
                .option("stream", "my-stream")
                .option("startingOffsets", "earliest")
                .load();

        // Show schema and data
        df.printSchema();
        df.show(20, false);

        // Access specific columns
        Dataset<Row> values = df.selectExpr(
                "cast(value as string) as message",
                "stream",
                "offset",
                "chunk_timestamp"
        );
        values.show(20, false);

        // Read a bounded offset range
        Dataset<Row> bounded = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", "localhost:5552")
                .option("stream", "my-stream")
                .option("startingOffsets", "offset")
                .option("startingOffset", "0")
                .option("endingOffsets", "offset")
                .option("endingOffset", "100")
                .load();

        System.out.println("Bounded read count: " + bounded.count());

        spark.stop();
    }
}
