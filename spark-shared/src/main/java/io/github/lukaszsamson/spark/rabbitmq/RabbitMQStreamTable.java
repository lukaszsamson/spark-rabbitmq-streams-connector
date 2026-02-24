package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Spark {@link Table} for RabbitMQ Streams.
 *
 * <p>Supports batch read, batch write, micro-batch read, and streaming write.
 * The schema includes fixed source columns plus optional metadata columns
 * controlled by the {@code metadataFields} option.
 */
public class RabbitMQStreamTable implements Table, SupportsRead, SupportsWrite {

    private static final Set<TableCapability> CAPABILITIES = Set.of(
            TableCapability.BATCH_READ,
            TableCapability.BATCH_WRITE,
            TableCapability.MICRO_BATCH_READ,
            TableCapability.STREAMING_WRITE,
            TableCapability.ACCEPT_ANY_SCHEMA
    );

    private final ConnectorOptions options;
    private final StructType schema;
    private final Map<String, String> tableOptions;

    public RabbitMQStreamTable(ConnectorOptions options) {
        this(options, Map.of());
    }

    public RabbitMQStreamTable(ConnectorOptions options, Map<String, String> tableOptions) {
        this.options = options;
        this.schema = buildSourceSchema(options.getMetadataFields());
        this.tableOptions = tableOptions == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(tableOptions));
    }

    @Override
    public String name() {
        return options.isStreamMode() ? options.getStream() : options.getSuperStream();
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return CAPABILITIES;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap sparkOptions) {
        // Spark V2 semantics: per-operation options override table options.
        ConnectorOptions scanOptions = sparkOptions.isEmpty()
                ? options
                : mergeOptionsWithOperationOverrides(tableOptions, sparkOptions.asCaseSensitiveMap());
        StructType scanSchema = sparkOptions.isEmpty() ? schema
                : buildSourceSchema(scanOptions.getMetadataFields());
        return new RabbitMQScanBuilder(scanOptions, scanSchema);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        // Spark V2 semantics: per-operation options override table options.
        CaseInsensitiveStringMap writeOpts = info.options();
        ConnectorOptions writeOptions = writeOpts.isEmpty()
                ? options
                : mergeOptionsWithOperationOverrides(tableOptions, writeOpts.asCaseSensitiveMap());
        return new RabbitMQWriteBuilder(writeOptions, info.schema(), info.queryId());
    }

    /** Returns the parsed connector options. */
    public ConnectorOptions getOptions() {
        return options;
    }

    private static ConnectorOptions mergeOptionsWithOperationOverrides(
            Map<String, String> tableOptions, Map<String, String> operationOptions) {
        Map<String, String> merged = new LinkedHashMap<>();
        putNormalizedKeys(merged, tableOptions);
        putNormalizedKeys(merged, operationOptions);
        return new ConnectorOptions(merged);
    }

    private static void putNormalizedKeys(Map<String, String> target, Map<String, String> source) {
        for (Map.Entry<String, String> entry : source.entrySet()) {
            String key = entry.getKey();
            if (key == null) {
                continue;
            }
            target.put(key.toLowerCase(Locale.ROOT), entry.getValue());
        }
    }

    // ---- Schema construction ----

    /** AMQP 1.0 properties struct type. */
    static final StructType PROPERTIES_STRUCT = new StructType(new StructField[]{
            new StructField("message_id", DataTypes.StringType, true, Metadata.empty()),
            new StructField("user_id", DataTypes.BinaryType, true, Metadata.empty()),
            new StructField("to", DataTypes.StringType, true, Metadata.empty()),
            new StructField("subject", DataTypes.StringType, true, Metadata.empty()),
            new StructField("reply_to", DataTypes.StringType, true, Metadata.empty()),
            new StructField("correlation_id", DataTypes.StringType, true, Metadata.empty()),
            new StructField("content_type", DataTypes.StringType, true, Metadata.empty()),
            new StructField("content_encoding", DataTypes.StringType, true, Metadata.empty()),
            new StructField("absolute_expiry_time", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("creation_time", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("group_id", DataTypes.StringType, true, Metadata.empty()),
            new StructField("group_sequence", DataTypes.LongType, true, Metadata.empty()),
            new StructField("reply_to_group_id", DataTypes.StringType, true, Metadata.empty()),
    });

    static StructType buildSourceSchema(Set<MetadataField> metadataFields) {
        List<StructField> fields = new ArrayList<>();

        // Fixed columns (always present)
        fields.add(new StructField("value", DataTypes.BinaryType, false, Metadata.empty()));
        fields.add(new StructField("stream", DataTypes.StringType, false, Metadata.empty()));
        fields.add(new StructField("offset", DataTypes.LongType, false, Metadata.empty()));
        fields.add(new StructField("chunk_timestamp", DataTypes.TimestampType, false, Metadata.empty()));

        // Optional metadata columns
        if (metadataFields.contains(MetadataField.PROPERTIES)) {
            fields.add(new StructField("properties", PROPERTIES_STRUCT, true, Metadata.empty()));
        }
        if (metadataFields.contains(MetadataField.APPLICATION_PROPERTIES)) {
            fields.add(new StructField("application_properties",
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                    true, Metadata.empty()));
        }
        if (metadataFields.contains(MetadataField.MESSAGE_ANNOTATIONS)) {
            fields.add(new StructField("message_annotations",
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                    true, Metadata.empty()));
        }
        if (metadataFields.contains(MetadataField.CREATION_TIME)) {
            fields.add(new StructField("creation_time", DataTypes.TimestampType, true, Metadata.empty()));
        }
        if (metadataFields.contains(MetadataField.ROUTING_KEY)) {
            fields.add(new StructField("routing_key", DataTypes.StringType, true, Metadata.empty()));
        }

        return new StructType(fields.toArray(new StructField[0]));
    }
}
