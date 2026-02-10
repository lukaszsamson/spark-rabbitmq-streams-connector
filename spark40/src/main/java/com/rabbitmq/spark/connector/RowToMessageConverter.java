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
        } else {
            this.propertiesFieldCount = 0;
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

    // ---- Properties struct → PropertiesBuilder ----

    /**
     * Apply AMQP properties from a struct row to the message builder.
     *
     * @return creation_time in millis if set, otherwise null
     */
    private Long applyProperties(InternalRow propsRow, MessageBuilder builder) {
        MessageBuilder.PropertiesBuilder pb = builder.properties();
        Long creationTimeMillis = null;

        // message_id (index 0, String)
        if (!propsRow.isNullAt(0)) {
            pb.messageId(propsRow.getUTF8String(0).toString());
        }
        // user_id (index 1, binary)
        if (!propsRow.isNullAt(1)) {
            pb.userId(propsRow.getBinary(1));
        }
        // to (index 2, String)
        if (!propsRow.isNullAt(2)) {
            pb.to(propsRow.getUTF8String(2).toString());
        }
        // subject (index 3, String)
        if (!propsRow.isNullAt(3)) {
            pb.subject(propsRow.getUTF8String(3).toString());
        }
        // reply_to (index 4, String)
        if (!propsRow.isNullAt(4)) {
            pb.replyTo(propsRow.getUTF8String(4).toString());
        }
        // correlation_id (index 5, String)
        if (!propsRow.isNullAt(5)) {
            pb.correlationId(propsRow.getUTF8String(5).toString());
        }
        // content_type (index 6, String)
        if (!propsRow.isNullAt(6)) {
            pb.contentType(propsRow.getUTF8String(6).toString());
        }
        // content_encoding (index 7, String)
        if (!propsRow.isNullAt(7)) {
            pb.contentEncoding(propsRow.getUTF8String(7).toString());
        }
        // absolute_expiry_time (index 8, Timestamp → millis)
        if (!propsRow.isNullAt(8)) {
            pb.absoluteExpiryTime(microsToMillis(propsRow.getLong(8)));
        }
        // creation_time (index 9, Timestamp → millis)
        if (!propsRow.isNullAt(9)) {
            creationTimeMillis = microsToMillis(propsRow.getLong(9));
            pb.creationTime(creationTimeMillis);
        }
        // group_id (index 10, String)
        if (!propsRow.isNullAt(10)) {
            pb.groupId(propsRow.getUTF8String(10).toString());
        }
        // group_sequence (index 11, Long)
        if (!propsRow.isNullAt(11)) {
            pb.groupSequence(propsRow.getLong(11));
        }
        // reply_to_group_id (index 12, String)
        if (!propsRow.isNullAt(12)) {
            pb.replyToGroupId(propsRow.getUTF8String(12).toString());
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
