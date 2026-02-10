package com.rabbitmq.spark.connector;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

/**
 * Optional metadata columns in the source schema.
 *
 * <p>Fixed columns ({@code value}, {@code stream}, {@code offset}, {@code chunk_timestamp})
 * are always present. These optional metadata columns can be toggled via the
 * {@code metadataFields} option.
 */
public enum MetadataField {
    PROPERTIES("properties"),
    APPLICATION_PROPERTIES("application_properties"),
    MESSAGE_ANNOTATIONS("message_annotations"),
    CREATION_TIME("creation_time"),
    ROUTING_KEY("routing_key");

    /** All metadata fields (the default set). */
    public static final Set<MetadataField> ALL = EnumSet.allOf(MetadataField.class);

    private final String fieldName;

    MetadataField(String fieldName) {
        this.fieldName = fieldName;
    }

    public String fieldName() {
        return fieldName;
    }

    public static MetadataField fromString(String value) {
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (MetadataField f : values()) {
            if (f.fieldName.equals(normalized)) {
                return f;
            }
        }
        throw new IllegalArgumentException(
                "Unknown metadata field: '" + value +
                        "'. Valid fields: properties, application_properties, " +
                        "message_annotations, creation_time, routing_key");
    }
}
