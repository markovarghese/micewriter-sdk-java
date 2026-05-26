package com.micewriter.sdk.schema;

import java.time.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility for mapping Java types to Iceberg primitive type strings and extracting
 * field values from POJOs for the IPC payload.
 */
final class PojoInspector {

    private PojoInspector() {}

    /**
     * Map a Java field type to the Iceberg primitive type string used in
     * {@code RegisterSchema} and matched by the engine's {@code parquet_writer}.
     */
    static String javaTypeToIcebergType(Class<?> type) {
        if (type == String.class)                                        return "string";
        if (type == long.class    || type == Long.class)                 return "long";
        if (type == int.class     || type == Integer.class)              return "int";
        if (type == double.class  || type == Double.class)               return "double";
        if (type == float.class   || type == Float.class)                return "float";
        if (type == boolean.class || type == Boolean.class)              return "boolean";
        if (type == Instant.class || type == OffsetDateTime.class
                || type == ZonedDateTime.class)                          return "timestamptz";
        if (type == LocalDateTime.class)                                 return "timestamp";
        if (type == LocalDate.class)                                     return "date";
        if (type == byte[].class)                                        return "binary";
        return "string"; // safe fallback for unknown types
    }

    /**
     * Convert a Java field value to a JSON-serialisable form that the engine can
     * store and later compile into a correctly-typed Arrow array.
     *
     * <ul>
     *   <li>{@link Instant}/{@link OffsetDateTime}/{@link ZonedDateTime} → microseconds since epoch (long)</li>
     *   <li>{@link LocalDate} → days since epoch (int)</li>
     *   <li>Everything else → pass through (Jackson handles primitives, Strings, nulls)</li>
     * </ul>
     */
    static Object serializeValue(Object value) {
        if (value == null) return null;
        if (value instanceof Instant i)          return i.toEpochMilli() * 1_000L;
        if (value instanceof OffsetDateTime odt) return odt.toInstant().toEpochMilli() * 1_000L;
        if (value instanceof ZonedDateTime zdt)  return zdt.toInstant().toEpochMilli() * 1_000L;
        if (value instanceof LocalDateTime ldt)  return ldt.toInstant(ZoneOffset.UTC).toEpochMilli() * 1_000L;
        if (value instanceof LocalDate ld)       return (int) ld.toEpochDay();
        return value;
    }

    /**
     * Extract all declared fields from a class (does not walk superclasses beyond Object).
     * Returns a list of {@code [fieldName, serializedValue]} pairs suitable for
     * the {@code IngestRecord.fields} JSON array.
     */
    static List<List<Object>> extractFields(Object entity) {
        List<List<Object>> result = new ArrayList<>();
        for (java.lang.reflect.Field f : entity.getClass().getDeclaredFields()) {
            f.setAccessible(true);
            try {
                Object raw = f.get(entity);
                result.add(List.of(f.getName(), serializeValue(raw) == null ? "" : serializeValue(raw)));
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Cannot read field " + f.getName(), e);
            }
        }
        return result;
    }
}
