package com.rabbitmq.spark.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Batch write to a RabbitMQ stream.
 *
 * <p>Creates sample data and writes it to a stream. Run with:
 * <pre>
 * spark-submit --class com.rabbitmq.spark.examples.BatchWriteExample \
 *   --jars sparkling-rabbit-spark41-&lt;version&gt;.jar \
 *   sparkling-rabbit-examples-&lt;version&gt;.jar
 * </pre>
 */
public class BatchWriteExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("RabbitMQ Batch Write")
                .getOrCreate();

        // Create sample data with value column
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(RowFactory.create(("message-" + i).getBytes()));
        }

        Dataset<Row> df = spark.createDataFrame(data, schema);

        // Write to stream
        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", "localhost:5552")
                .option("stream", "my-stream")
                .save();

        System.out.println("Wrote " + data.size() + " messages to my-stream");

        // Write with application properties
        StructType schemaWithProps = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("application_properties",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true);

        List<Row> dataWithProps = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            dataWithProps.add(RowFactory.create(
                    ("enriched-" + i).getBytes(),
                    java.util.Map.of("source", "batch-example", "index", String.valueOf(i))
            ));
        }

        Dataset<Row> dfWithProps = spark.createDataFrame(dataWithProps, schemaWithProps);

        dfWithProps.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", "localhost:5552")
                .option("stream", "my-stream")
                .save();

        System.out.println("Wrote " + dataWithProps.size() + " messages with properties");

        spark.stop();
    }
}
