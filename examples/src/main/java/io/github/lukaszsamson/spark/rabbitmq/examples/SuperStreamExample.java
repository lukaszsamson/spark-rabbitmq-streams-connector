package io.github.lukaszsamson.spark.rabbitmq.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Superstream batch read and write with routing.
 *
 * <p>Demonstrates reading from and writing to a superstream. The superstream
 * must be created beforehand (e.g., via {@code rabbitmq-streams add_super_stream}).
 *
 * <p>Run with:
 * <pre>
 * spark-submit --class com.rabbitmq.spark.examples.SuperStreamExample \
 *   --jars sparkling-rabbit-spark41-&lt;version&gt;.jar \
 *   sparkling-rabbit-examples-&lt;version&gt;.jar
 * </pre>
 */
public class SuperStreamExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("RabbitMQ Superstream Example")
                .getOrCreate();

        // --- Write to superstream with hash routing ---

        StructType writeSchema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("routing_key", DataTypes.StringType, true);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String routingKey = "key-" + (i % 10);
            data.add(RowFactory.create(
                    ("superstream-msg-" + i).getBytes(),
                    routingKey
            ));
        }

        Dataset<Row> writeDF = spark.createDataFrame(data, writeSchema);

        writeDF.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", "localhost:5552")
                .option("superstream", "my-superstream")
                .option("routingStrategy", "hash")
                .save();

        System.out.println("Wrote 100 messages to my-superstream with hash routing");

        // --- Read from superstream ---

        Dataset<Row> readDF = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", "localhost:5552")
                .option("superstream", "my-superstream")
                .option("startingOffsets", "earliest")
                .load();

        readDF.printSchema();

        // Show messages per partition stream
        readDF.groupBy("stream").count().show();

        // Show sample data
        readDF.selectExpr(
                "cast(value as string) as message",
                "stream",
                "offset"
        ).show(20, false);

        System.out.println("Total messages read: " + readDF.count());

        spark.stop();
    }
}
