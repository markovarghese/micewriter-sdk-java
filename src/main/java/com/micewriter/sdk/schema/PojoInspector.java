package com.micewriter.sdk.schema;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public final class PojoInspector {

    private PojoInspector() {}

    private static final ConcurrentHashMap<Class<?>, Schema> SCHEMA_CACHE = new ConcurrentHashMap<>();

    /**
     * Map a Java field type to the Iceberg primitive type string used in
     * {@code REGISTER_SCHEMA} JSON messages.
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
        return "string";
    }

    /**
     * Build a cached Arrow {@link Schema} for the given POJO class. The schema is
     * derived from declared fields and their Java types — it never changes at runtime,
     * so it is computed once and cached.
     */
    public static Schema buildArrowSchema(Class<?> clazz) {
        return SCHEMA_CACHE.computeIfAbsent(clazz, PojoInspector::computeSchema);
    }

    private static Schema computeSchema(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        for (java.lang.reflect.Field f : clazz.getDeclaredFields()) {
            ArrowType arrowType = javaTypeToArrowType(f.getType());
            fields.add(new Field(f.getName(), new FieldType(true, arrowType, null), null));
        }
        return new Schema(fields);
    }

    private static ArrowType javaTypeToArrowType(Class<?> type) {
        if (type == String.class)                                        return new ArrowType.Utf8();
        if (type == long.class    || type == Long.class)                 return new ArrowType.Int(64, true);
        if (type == int.class     || type == Integer.class)              return new ArrowType.Int(32, true);
        if (type == double.class  || type == Double.class)               return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        if (type == float.class   || type == Float.class)                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        if (type == boolean.class || type == Boolean.class)              return new ArrowType.Bool();
        if (type == Instant.class || type == OffsetDateTime.class
                || type == ZonedDateTime.class)                          return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
        if (type == LocalDateTime.class)                                 return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
        if (type == LocalDate.class)                                     return new ArrowType.Date(DateUnit.DAY);
        if (type == byte[].class)                                        return new ArrowType.Binary();
        return new ArrowType.Utf8();
    }

    /**
     * Serialize {@code entity} as an Apache Arrow IPC stream (schema message +
     * single RecordBatch + EOS marker) using the pre-built {@code schema}.
     *
     * @param entity    the POJO to serialize (one row)
     * @param schema    Arrow schema produced by {@link #buildArrowSchema}
     * @param allocator Arrow buffer allocator (caller-owned, must outlive this call)
     * @return raw Arrow IPC stream bytes
     */
    public static byte[] toArrowIpcStream(Object entity, Schema schema, BufferAllocator allocator) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            root.allocateNew();

            java.lang.reflect.Field[] declaredFields = entity.getClass().getDeclaredFields();
            for (int i = 0; i < declaredFields.length; i++) {
                java.lang.reflect.Field f = declaredFields[i];
                f.setAccessible(true);
                Object value;
                try {
                    value = f.get(entity);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Cannot read field " + f.getName(), e);
                }
                setVectorValue(root.getVector(i), value, 0);
            }

            root.setRowCount(1);

            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }

            return out.toByteArray();

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Arrow IPC serialization failed", e);
        }
    }

    private static void setVectorValue(FieldVector vector, Object value, int index) {
        if (value == null) {
            vector.setNull(index);
            return;
        }
        if (vector instanceof VarCharVector v) {
            v.setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
        } else if (vector instanceof LargeVarCharVector v) {
            v.setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
        } else if (vector instanceof BigIntVector v) {
            v.setSafe(index, ((Number) value).longValue());
        } else if (vector instanceof IntVector v) {
            v.setSafe(index, ((Number) value).intValue());
        } else if (vector instanceof Float8Vector v) {
            v.setSafe(index, ((Number) value).doubleValue());
        } else if (vector instanceof Float4Vector v) {
            v.setSafe(index, ((Number) value).floatValue());
        } else if (vector instanceof BitVector v) {
            v.setSafe(index, Boolean.TRUE.equals(value) ? 1 : 0);
        } else if (vector instanceof TimeStampMicroTZVector v) {
            v.setSafe(index, toMicros(value));
        } else if (vector instanceof TimeStampMicroVector v) {
            long micros = ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli() * 1_000L;
            v.setSafe(index, micros);
        } else if (vector instanceof DateDayVector v) {
            v.setSafe(index, (int) ((LocalDate) value).toEpochDay());
        } else if (vector instanceof VarBinaryVector v) {
            v.setSafe(index, (byte[]) value);
        } else {
            // Fallback: toString into the vector (covers unknown Utf8-mapped types)
            if (vector instanceof VarCharVector vc) {
                vc.setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private static long toMicros(Object value) {
        if (value instanceof Instant i)          return i.toEpochMilli() * 1_000L;
        if (value instanceof OffsetDateTime odt) return odt.toInstant().toEpochMilli() * 1_000L;
        if (value instanceof ZonedDateTime zdt)  return zdt.toInstant().toEpochMilli() * 1_000L;
        return 0L;
    }
}
