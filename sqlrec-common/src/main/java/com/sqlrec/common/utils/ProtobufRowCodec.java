package com.sqlrec.common.utils;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.sqlrec.common.schema.FieldSchema;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Encodes and decodes SQLRec rows with the schema of a generated Protobuf message class. */
public final class ProtobufRowCodec {
    private final Message defaultInstance;

    public ProtobufRowCodec(String messageClassName) {
        if (messageClassName == null || messageClassName.trim().isEmpty()) {
            throw new IllegalArgumentException("protobuf.message-class-name must not be empty");
        }
        this.defaultInstance = loadDefaultInstance(messageClassName);
    }

    public byte[] encode(Object[] row, List<FieldSchema> fieldSchemas) {
        if (row == null) {
            throw new IllegalArgumentException("row must not be null");
        }
        if (fieldSchemas == null || row.length != fieldSchemas.size()) {
            throw new IllegalArgumentException("row size must match the table schema");
        }

        Message.Builder builder = defaultInstance.newBuilderForType();
        Descriptors.Descriptor descriptor = defaultInstance.getDescriptorForType();
        for (int i = 0; i < fieldSchemas.size(); i++) {
            Object value = row[i];
            if (value == null) {
                continue;
            }

            String fieldName = fieldSchemas.get(i).getName();
            Descriptors.FieldDescriptor field = descriptor.findFieldByName(fieldName);
            if (field == null) {
                throw new IllegalArgumentException(
                        "Table field '" + fieldName + "' does not exist in Protobuf message "
                                + descriptor.getFullName());
            }
            setField(builder, field, value);
        }
        return builder.build().toByteArray();
    }

    public Object[] decode(byte[] bytes, List<FieldSchema> fieldSchemas) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes must not be null");
        }
        if (fieldSchemas == null) {
            throw new IllegalArgumentException("table schema must not be null");
        }

        final Message message;
        try {
            message = (Message) defaultInstance.getParserForType().parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(
                    "Unable to parse " + defaultInstance.getDescriptorForType().getFullName(), e);
        }

        Object[] row = new Object[fieldSchemas.size()];
        Descriptors.Descriptor descriptor = message.getDescriptorForType();
        for (int i = 0; i < fieldSchemas.size(); i++) {
            FieldSchema fieldSchema = fieldSchemas.get(i);
            Descriptors.FieldDescriptor field = descriptor.findFieldByName(fieldSchema.getName());
            if (field == null) {
                throw new IllegalArgumentException(
                        "Table field '" + fieldSchema.getName()
                                + "' does not exist in Protobuf message " + descriptor.getFullName());
            }
            row[i] = readField(message, field, fieldSchema.getType());
        }
        return row;
    }

    private static Message loadDefaultInstance(String messageClassName) {
        try {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            if (contextClassLoader == null) {
                contextClassLoader = ProtobufRowCodec.class.getClassLoader();
            }
            Class<?> messageClass = Class.forName(messageClassName, true, contextClassLoader);
            Method method = messageClass.getMethod("getDefaultInstance");
            Object instance = method.invoke(null);
            if (!(instance instanceof Message)) {
                throw new IllegalArgumentException(
                        messageClassName + " is not a generated Protobuf Message class");
            }
            return (Message) instance;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (ReflectiveOperationException | LinkageError e) {
            throw new IllegalArgumentException(
                    "Unable to load Protobuf message class " + messageClassName, e);
        }
    }

    private static void setField(
            Message.Builder builder,
            Descriptors.FieldDescriptor field,
            Object value
    ) {
        if (field.isMapField()) {
            if (!(value instanceof Map)) {
                throw incompatible(field, value);
            }
            Descriptors.Descriptor entryType = field.getMessageType();
            Descriptors.FieldDescriptor keyField = entryType.findFieldByName("key");
            Descriptors.FieldDescriptor valueField = entryType.findFieldByName("value");
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                DynamicMessage.Builder entryBuilder = DynamicMessage.newBuilder(entryType);
                entryBuilder.setField(keyField, convertScalar(keyField, entry.getKey()));
                entryBuilder.setField(valueField, convertScalar(valueField, entry.getValue()));
                builder.addRepeatedField(field, entryBuilder.build());
            }
            return;
        }

        if (field.isRepeated()) {
            if (value instanceof Iterable) {
                for (Object element : (Iterable<?>) value) {
                    builder.addRepeatedField(field, convertScalar(field, element));
                }
                return;
            }
            if (value.getClass().isArray()) {
                for (int i = 0; i < Array.getLength(value); i++) {
                    builder.addRepeatedField(field, convertScalar(field, Array.get(value, i)));
                }
                return;
            }
            throw incompatible(field, value);
        }

        builder.setField(field, convertScalar(field, value));
    }

    private static Object convertScalar(Descriptors.FieldDescriptor field, Object value) {
        if (value == null) {
            throw new IllegalArgumentException(
                    "Protobuf repeated and map values cannot contain null for field "
                            + field.getFullName());
        }
        try {
            switch (field.getJavaType()) {
                case INT:
                    return ((Number) value).intValue();
                case LONG:
                    return ((Number) value).longValue();
                case FLOAT:
                    return ((Number) value).floatValue();
                case DOUBLE:
                    return ((Number) value).doubleValue();
                case BOOLEAN:
                    return (Boolean) value;
                case STRING:
                    return value.toString();
                case BYTE_STRING:
                    if (value instanceof ByteString) {
                        return value;
                    }
                    if (value instanceof byte[]) {
                        return ByteString.copyFrom((byte[]) value);
                    }
                    if (value instanceof ByteBuffer) {
                        return ByteString.copyFrom(((ByteBuffer) value).duplicate());
                    }
                    throw incompatible(field, value);
                case ENUM:
                    Descriptors.EnumValueDescriptor enumValue;
                    if (value instanceof Number) {
                        enumValue = field.getEnumType().findValueByNumber(((Number) value).intValue());
                    } else {
                        enumValue = field.getEnumType().findValueByName(value.toString());
                    }
                    if (enumValue == null) {
                        throw new IllegalArgumentException(
                                "Unknown enum value '" + value + "' for field " + field.getFullName());
                    }
                    return enumValue;
                case MESSAGE:
                    if (value instanceof Message
                            && ((Message) value).getDescriptorForType().equals(field.getMessageType())) {
                        return value;
                    }
                    if (value instanceof Map) {
                        DynamicMessage.Builder nested = DynamicMessage.newBuilder(field.getMessageType());
                        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                            Descriptors.FieldDescriptor nestedField = field.getMessageType()
                                    .findFieldByName(String.valueOf(entry.getKey()));
                            if (nestedField == null) {
                                throw new IllegalArgumentException(
                                        "Unknown nested field '" + entry.getKey() + "' for "
                                                + field.getMessageType().getFullName());
                            }
                            setField(nested, nestedField, entry.getValue());
                        }
                        return nested.build();
                    }
                    throw incompatible(field, value);
                default:
                    throw incompatible(field, value);
            }
        } catch (ClassCastException e) {
            throw incompatible(field, value);
        }
    }

    private static Object readField(
            Message message,
            Descriptors.FieldDescriptor field,
            String sqlType
    ) {
        if (field.isMapField()) {
            Map<Object, Object> result = new LinkedHashMap<>();
            Descriptors.FieldDescriptor keyField = field.getMessageType().findFieldByName("key");
            Descriptors.FieldDescriptor valueField = field.getMessageType().findFieldByName("value");
            for (Object value : (List<?>) message.getField(field)) {
                Message entry = (Message) value;
                result.put(
                        fromProtobufValue(keyField, entry.getField(keyField), null),
                        fromProtobufValue(valueField, entry.getField(valueField), null));
            }
            return result;
        }

        if (field.isRepeated()) {
            List<Object> result = new ArrayList<>();
            for (Object value : (List<?>) message.getField(field)) {
                result.add(fromProtobufValue(field, value, arrayElementType(sqlType)));
            }
            return result;
        }

        if (field.hasPresence() && !message.hasField(field)) {
            return null;
        }
        return fromProtobufValue(field, message.getField(field), sqlType);
    }

    private static Object fromProtobufValue(
            Descriptors.FieldDescriptor field,
            Object value,
            String sqlType
    ) {
        switch (field.getJavaType()) {
            case BYTE_STRING:
                return ((ByteString) value).toByteArray();
            case ENUM:
                Descriptors.EnumValueDescriptor enumValue =
                        (Descriptors.EnumValueDescriptor) value;
                return isStringType(sqlType) ? enumValue.getName() : enumValue.getNumber();
            case MESSAGE:
                Message nestedMessage = (Message) value;
                Map<String, Object> nested = new LinkedHashMap<>();
                for (Descriptors.FieldDescriptor nestedField
                        : nestedMessage.getDescriptorForType().getFields()) {
                    nested.put(
                            nestedField.getName(),
                            readField(nestedMessage, nestedField, null));
                }
                return nested;
            default:
                return value;
        }
    }

    private static String arrayElementType(String sqlType) {
        if (sqlType == null) {
            return null;
        }
        String normalized = sqlType.trim();
        if (normalized.regionMatches(true, 0, "ARRAY<", 0, "ARRAY<".length())
                && normalized.endsWith(">")) {
            return normalized.substring("ARRAY<".length(), normalized.length() - 1).trim();
        }
        return null;
    }

    private static boolean isStringType(String sqlType) {
        if (sqlType == null) {
            return false;
        }
        String normalized = sqlType.trim().toUpperCase();
        return normalized.startsWith("CHAR")
                || normalized.startsWith("VARCHAR")
                || normalized.equals("STRING")
                || normalized.equals("TEXT");
    }

    private static IllegalArgumentException incompatible(
            Descriptors.FieldDescriptor field,
            Object value
    ) {
        return new IllegalArgumentException(
                "Value of type " + value.getClass().getName()
                        + " is incompatible with Protobuf field " + field.getFullName());
    }
}
