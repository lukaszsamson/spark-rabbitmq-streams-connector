package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;

import static com.rabbitmq.spark.connector.MessageToRowConverter.microsToMillis;

/**
 * Converts a Spark {@link InternalRow} to a RabbitMQ {@link Message}
 * using the provided {@link MessageBuilder}.
 *
 * <p>Handles extraction of value, optional AMQP properties,
 * application_properties, message_annotations, and timestamp
 * conversion (micros → millis).
 */
public final class RowToMessageConverter implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int valueIndex;
    private final int streamIndex;
    private final int routingKeyIndex;
    private final int propertiesIndex;
    private final int appPropertiesIndex;
    private final int msgAnnotationsIndex;
    private final int creationTimeIndex;

    private final int propertiesFieldCount;

    // Indices of known AMQP property fields within the user-supplied properties struct.
    // -1 means the field is absent from the struct.
    private static final String[] PROPERTY_FIELD_NAMES = {
            "message_id", "user_id", "to", "subject", "reply_to",
            "correlation_id", "content_type", "content_encoding",
            "absolute_expiry_time", "creation_time", "group_id",
            "group_sequence", "reply_to_group_id"
    };
    private static final int PROP_MESSAGE_ID = 0;
    private static final int PROP_USER_ID = 1;
    private static final int PROP_TO = 2;
    private static final int PROP_SUBJECT = 3;
    private static final int PROP_REPLY_TO = 4;
    private static final int PROP_CORRELATION_ID = 5;
    private static final int PROP_CONTENT_TYPE = 6;
    private static final int PROP_CONTENT_ENCODING = 7;
    private static final int PROP_ABSOLUTE_EXPIRY_TIME = 8;
    private static final int PROP_CREATION_TIME = 9;
    private static final int PROP_GROUP_ID = 10;
    private static final int PROP_GROUP_SEQUENCE = 11;
    private static final int PROP_REPLY_TO_GROUP_ID = 12;

    private final int[] propsFieldIndices;

    /**
     * Create a converter for the given input schema.
     *
     * @param schema the validated sink input schema
     */
    public RowToMessageConverter(StructType schema) {
        this.valueIndex = fieldIndex(schema, "value");
        this.streamIndex = fieldIndex(schema, "stream");
        this.routingKeyIndex = fieldIndex(schema, "routing_key");
        this.propertiesIndex = fieldIndex(schema, "properties");
        this.appPropertiesIndex = fieldIndex(schema, "application_properties");
        this.msgAnnotationsIndex = fieldIndex(schema, "message_annotations");
        this.creationTimeIndex = fieldIndex(schema, "creation_time");

        if (propertiesIndex >= 0) {
            StructType propsType = (StructType) schema.fields()[propertiesIndex].dataType();
            this.propertiesFieldCount = propsType.fields().length;
            this.propsFieldIndices = new int[PROPERTY_FIELD_NAMES.length];
            for (int i = 0; i < PROPERTY_FIELD_NAMES.length; i++) {
                propsFieldIndices[i] = fieldIndex(propsType, PROPERTY_FIELD_NAMES[i]);
            }
        } else {
            this.propertiesFieldCount = 0;
            this.propsFieldIndices = null;
        }
    }

    /**
     * Convert a row to a RabbitMQ message.
     *
     * @param row the Spark internal row
     * @param builder a fresh MessageBuilder (typically from Producer.messageBuilder())
     * @return the built message
     */
    public Message convert(InternalRow row, MessageBuilder builder) {
        // Required: value
        byte[] body = row.getBinary(valueIndex);
        builder.addData(body);

        // Collect creation_time from both sources, top-level overrides struct
        Long creationTimeMillis = null;

        // Optional: properties struct
        if (propertiesIndex >= 0 && !row.isNullAt(propertiesIndex)) {
            InternalRow propsRow = row.getStruct(propertiesIndex, propertiesFieldCount);
            creationTimeMillis = applyProperties(propsRow, builder);
        }

        // Optional: top-level creation_time overrides properties.creation_time
        if (creationTimeIndex >= 0 && !row.isNullAt(creationTimeIndex)) {
            creationTimeMillis = microsToMillis(row.getLong(creationTimeIndex));
        }

        // Apply creation_time if set (either from struct or top-level)
        if (creationTimeMillis != null) {
            builder.properties().creationTime(creationTimeMillis).messageBuilder();
        }

        // Optional: application_properties
        if (appPropertiesIndex >= 0 && !row.isNullAt(appPropertiesIndex)) {
            applyApplicationProperties(row.getMap(appPropertiesIndex), builder);
        }

        // Optional: routing_key → stored in application properties for routing
        if (routingKeyIndex >= 0 && !row.isNullAt(routingKeyIndex)) {
            UTF8String routingKeyUtf8 = row.getUTF8String(routingKeyIndex);
            if (routingKeyUtf8 != null) {
                builder.applicationProperties()
                        .entry("routing_key", routingKeyUtf8.toString())
                        .messageBuilder();
            }
        }

        // Optional: message_annotations
        if (msgAnnotationsIndex >= 0 && !row.isNullAt(msgAnnotationsIndex)) {
            applyMessageAnnotations(row.getMap(msgAnnotationsIndex), builder);
        }

        return builder.build();
    }

    /**
     * Extract the routing key from the row, or {@code null} if absent.
     */
    public String getRoutingKey(InternalRow row) {
        if (routingKeyIndex < 0 || row.isNullAt(routingKeyIndex)) {
            return null;
        }
        UTF8String utf8 = row.getUTF8String(routingKeyIndex);
        return utf8 != null ? utf8.toString() : null;
    }

    /**
     * Extract the target stream name from the row, or {@code null} if absent.
     */
    public String getStream(InternalRow row) {
        if (streamIndex < 0 || row.isNullAt(streamIndex)) {
            return null;
        }
        UTF8String utf8 = row.getUTF8String(streamIndex);
        return utf8 != null ? utf8.toString() : null;
    }

    /**
     * Extract the message body value from the row.
     */
    public byte[] getValue(InternalRow row) {
        return row.getBinary(valueIndex);
    }

    // ---- Properties struct → PropertiesBuilder ----

    /**
     * Apply AMQP properties from a struct row to the message builder.
     * Uses name-based field lookup to handle subset and reordered properties structs.
     *
     * @return creation_time in millis if set, otherwise null
     */
    private Long applyProperties(InternalRow propsRow, MessageBuilder builder) {
        MessageBuilder.PropertiesBuilder pb = builder.properties();
        Long creationTimeMillis = null;
        int idx;

        idx = propsFieldIndices[PROP_MESSAGE_ID];
        if (idx >= 0 && !propsRow.isNullAt(idx)) {
            pb.messageId(propsRow.getUTF8String(idx).toString());
        }
        idx = propsFieldIndices[PROP_USER_ID];
        if (idx >= 0 && !propsRow.isNullAt(idx)) {
            pb.userId(propsRow.getBinary(idx));
        }
        idx = propsFieldIndices[PROP_TO];
        if (idx >= 0 && !propsRow.isNullAt(idx)) {
            pb.to(propsRow.getUTF8String(idx).toString());
        }
        idx = propsFieldIndices[PROP_SUBJECT];
        if (idx >= 0 && !propsRow.isNullAt(idx)) {
            pb.subject(propsRow.getUTF8String(idx).toString());
        }
        idx = propsFieldIndices[PROP_REPLY_TO];
        if (idx >= 0 && !propsRow.isNullAt(idx)) {
            pb.replyTo(propsRow.getUTF8String(idx).toString());
        }
        idx = propsFieldIndices[PROP_CORRELATION_ID];
        if (idx >= 0 && !propsRow.isNullAt(idx)) {
            pb.correlationId(propsRow.getUTF8String(idx).toString());
        }
        idx = propsFieldIndices[PROP_CONTENT_TYPE];
        if (idx >= 0 && !propsRow.isNullAt(idx)) {
            pb.contentType(propsRow.getUTF8String(idx).toString());
        }
        idx = propsFieldIndices[PROP_CONTENT_ENCODING];
        if (idx >= 0 && !propsRow.isNullAt(idx)) {
            pb.contentEncoding(propsRow.getUTF8String(idx).toString());
        }
        idx = propsFieldIndices[PROP_ABSOLUTE_EXPIRY_TIME];
        if (idx >= 0 && !propsRow.isNullAt(idx)) {
            pb.absoluteExpiryTime(microsToMillis(propsRow.getLong(idx)));
        }
        idx = propsFieldIndices[PROP_CREATION_TIME];
        if (idx >= 0 && !propsRow.isNullAt(idx)) {
            creationTimeMillis = microsToMillis(propsRow.getLong(idx));
            pb.creationTime(creationTimeMillis);
        }
        idx = propsFieldIndices[PROP_GROUP_ID];
        if (idx >= 0 && !propsRow.isNullAt(idx)) {
            pb.groupId(propsRow.getUTF8String(idx).toString());
        }
        idx = propsFieldIndices[PROP_GROUP_SEQUENCE];
        if (idx >= 0 && !propsRow.isNullAt(idx)) {
            pb.groupSequence(propsRow.getLong(idx));
        }
        idx = propsFieldIndices[PROP_REPLY_TO_GROUP_ID];
        if (idx >= 0 && !propsRow.isNullAt(idx)) {
            pb.replyToGroupId(propsRow.getUTF8String(idx).toString());
        }

        pb.messageBuilder();
        return creationTimeMillis;
    }

    // ---- Map conversions ----

    private void applyApplicationProperties(MapData mapData, MessageBuilder builder) {
        if (mapData.numElements() == 0) return;
        MessageBuilder.ApplicationPropertiesBuilder apb = builder.applicationProperties();
        var keyArray = mapData.keyArray();
        var valueArray = mapData.valueArray();
        for (int i = 0; i < mapData.numElements(); i++) {
            String key = keyArray.getUTF8String(i).toString();
            String value = valueArray.isNullAt(i) ? null
                    : valueArray.getUTF8String(i).toString();
            if (value != null) {
                apb.entry(key, value);
            }
        }
        apb.messageBuilder();
    }

    private void applyMessageAnnotations(MapData mapData, MessageBuilder builder) {
        if (mapData.numElements() == 0) return;
        MessageBuilder.MessageAnnotationsBuilder mab = builder.messageAnnotations();
        var keyArray = mapData.keyArray();
        var valueArray = mapData.valueArray();
        for (int i = 0; i < mapData.numElements(); i++) {
            String key = keyArray.getUTF8String(i).toString();
            String value = valueArray.isNullAt(i) ? null
                    : valueArray.getUTF8String(i).toString();
            if (value != null) {
                mab.entry(key, value);
            }
        }
        mab.messageBuilder();
    }

    // ---- Helpers ----

    /**
     * Returns the field index in the schema, or -1 if absent.
     */
    private static int fieldIndex(StructType schema, String name) {
        String[] names = schema.fieldNames();
        for (int i = 0; i < names.length; i++) {
            if (names[i].equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }
}
