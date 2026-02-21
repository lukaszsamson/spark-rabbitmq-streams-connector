package io.github.lukaszsamson.spark.rabbitmq;

import java.util.Locale;

/**
 * Path parsing and extraction for {@link ConnectorMessageView}.
 */
public final class ConnectorMessagePath {

    private ConnectorMessagePath() {}

    public static void validate(String path, String optionName) {
        ParsedPath parsed = parse(path, optionName);
        if (parsed.root == Root.PROPERTIES) {
            normalizePropertyField(parsed.field, optionName);
        }
    }

    public static String extract(ConnectorMessageView message, String path) {
        ParsedPath parsed = parse(path, "path");
        return switch (parsed.root) {
            case APPLICATION_PROPERTIES -> message.getApplicationProperties().get(parsed.field);
            case MESSAGE_ANNOTATIONS -> message.getMessageAnnotations().get(parsed.field);
            case PROPERTIES -> message.getProperties().get(normalizePropertyField(parsed.field, "path"));
        };
    }

    private static ParsedPath parse(String path, String optionName) {
        if (path == null) {
            throw new IllegalArgumentException("'" + optionName + "' must not be null");
        }
        String trimmed = path.trim();
        int dot = trimmed.indexOf('.');
        if (dot <= 0 || dot == trimmed.length() - 1) {
            throw new IllegalArgumentException(
                    "'" + optionName + "' must be in '<root>.<field>' format, got: '" + path + "'");
        }

        String rawRoot = trimmed.substring(0, dot);
        String field = trimmed.substring(dot + 1).trim();
        if (field.isEmpty()) {
            throw new IllegalArgumentException(
                    "'" + optionName + "' has an empty field segment: '" + path + "'");
        }
        return new ParsedPath(parseRoot(rawRoot, optionName), field);
    }

    private static Root parseRoot(String rawRoot, String optionName) {
        String normalized = rawRoot.trim().toLowerCase(Locale.ROOT)
                .replace("-", "_");
        return switch (normalized) {
            case "application_properties", "applicationproperties" -> Root.APPLICATION_PROPERTIES;
            case "message_annotations", "messageannotations" -> Root.MESSAGE_ANNOTATIONS;
            case "properties" -> Root.PROPERTIES;
            default -> throw new IllegalArgumentException(
                    "'" + optionName + "' root must be one of " +
                            "application_properties, message_annotations, properties; got: '" +
                            rawRoot + "'");
        };
    }

    private static String normalizePropertyField(String rawField, String optionName) {
        String normalized = rawField.trim().toLowerCase(Locale.ROOT)
                .replace("-", "_");
        String compact = normalized.replace("_", "");
        return switch (compact) {
            case "messageid" -> "message_id";
            case "userid" -> "user_id";
            case "to" -> "to";
            case "subject" -> "subject";
            case "replyto" -> "reply_to";
            case "correlationid" -> "correlation_id";
            case "contenttype" -> "content_type";
            case "contentencoding" -> "content_encoding";
            case "absoluteexpirytime" -> "absolute_expiry_time";
            case "creationtime" -> "creation_time";
            case "groupid" -> "group_id";
            case "groupsequence" -> "group_sequence";
            case "replytogroupid" -> "reply_to_group_id";
            default -> throw new IllegalArgumentException(
                    "'" + optionName + "' uses unknown AMQP properties field '" + rawField + "'");
        };
    }

    private enum Root {
        APPLICATION_PROPERTIES,
        MESSAGE_ANNOTATIONS,
        PROPERTIES
    }

    private record ParsedPath(Root root, String field) {}
}
