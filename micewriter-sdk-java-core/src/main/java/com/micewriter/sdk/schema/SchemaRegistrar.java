package com.micewriter.sdk.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.micewriter.sdk.annotation.IcebergEntity;
import com.micewriter.sdk.ipc.AckResponse;
import com.micewriter.sdk.ipc.UdsConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Registers Iceberg table schemas with the micewriter-engine over UDS.
 *
 * <p>This is the framework-agnostic core. Call {@link #register(Class[])} directly
 * to register a fixed set of entity classes — no classpath scanning required.
 * Framework-specific integrations (Spring Boot, Dropwizard) wrap this class and
 * supply the entity list via their own lifecycle hooks.
 */
public class SchemaRegistrar {

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistrar.class);

    /** IPC type discriminant — must match {@code MSG_REGISTER_SCHEMA} in the Rust engine. */
    private static final byte MSG_REGISTER_SCHEMA = 0x01;

    private final UdsConnection connection;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** Guards against double-registration when called more than once. */
    private final AtomicBoolean registered = new AtomicBoolean(false);

    public SchemaRegistrar(UdsConnection connection) {
        this.connection = connection;
    }

    /**
     * Register a fixed set of {@link IcebergEntity}-annotated classes.
     * Idempotent — subsequent calls after the first are no-ops.
     *
     * @param entityClasses classes annotated with {@link IcebergEntity}
     */
    public void register(Class<?>... entityClasses) {
        if (!registered.compareAndSet(false, true)) {
            return;
        }
        log.info("Registering {} @IcebergEntity class(es)", entityClasses.length);
        for (Class<?> clazz : entityClasses) {
            try {
                registerSchema(clazz);
            } catch (Exception e) {
                log.error("Schema registration failed for {}: {}", clazz.getName(), e.getMessage(), e);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private void registerSchema(Class<?> clazz) throws Exception {
        IcebergEntity ann = clazz.getAnnotation(IcebergEntity.class);
        if (ann == null) {
            log.warn("Skipping {} — not annotated with @IcebergEntity", clazz.getName());
            return;
        }

        String tableName = ann.table().isEmpty()
                ? clazz.getSimpleName().toLowerCase(Locale.ROOT)
                : ann.table();

        List<Map<String, Object>> fields = new ArrayList<>();
        for (java.lang.reflect.Field f : clazz.getDeclaredFields()) {
            Map<String, Object> fieldDef = new LinkedHashMap<>();
            fieldDef.put("name", f.getName());
            fieldDef.put("type", javaTypeToIcebergType(f.getGenericType()));
            fieldDef.put("required", f.getType().isPrimitive()
                    || f.isAnnotationPresent(com.micewriter.sdk.annotation.IcebergId.class));
            fields.add(fieldDef);
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("table", tableName);
        payload.put("namespace", Arrays.asList(ann.namespace()));
        payload.put("fields", fields);

        byte[] json = objectMapper.writeValueAsBytes(payload);
        byte[] ipcPayload = prependTypeByte(MSG_REGISTER_SCHEMA, json);

        AckResponse ack = connection.send(ipcPayload);
        if (!ack.isOk()) {
            throw new RuntimeException("Engine rejected schema for '" + tableName + "': " + ack.getMsg());
        }
        log.info("Registered Iceberg schema for table '{}' ({} fields)", tableName, fields.size());
    }

    /** Prepend the 1-byte message type discriminant. The 4-byte length prefix is added by UdsConnection. */
    public static byte[] prependTypeByte(byte type, byte[] body) {
        byte[] payload = new byte[1 + body.length];
        payload[0] = type;
        System.arraycopy(body, 0, payload, 1, body.length);
        return payload;
    }

    /**
     * Map a Java type to its Iceberg type string.
     *
     * <p>Note for {@code timestamptz}: {@link java.time.Instant} and
     * {@link java.time.OffsetDateTime} produce ISO-8601 with numeric offsets
     * and are safe. {@link java.time.ZonedDateTime} may serialise with a
     * named zone like {@code 2026-05-30T07:30Z[UTC]} which the engine
     * cannot parse — see {@link com.micewriter.sdk.template.IcebergStreamTemplate}'s
     * class-level docs.
     */
    private static String javaTypeToIcebergType(Type type) {
        if (type instanceof ParameterizedType) {
            ParameterizedType pType = (ParameterizedType) type;
            if (pType.getRawType() == java.util.List.class) {
                Type innerType = pType.getActualTypeArguments()[0];
                return "list(" + javaTypeToIcebergType(innerType) + ")";
            }
        }
        
        Class<?> clazz = null;
        if (type instanceof Class<?>) {
            clazz = (Class<?>) type;
        } else {
            return "string";
        }

        if (clazz == String.class) return "string";
        if (clazz == int.class || clazz == Integer.class) return "int";
        if (clazz == long.class || clazz == Long.class) return "long";
        if (clazz == double.class || clazz == Double.class) return "double";
        if (clazz == float.class || clazz == Float.class) return "float";
        if (clazz == boolean.class || clazz == Boolean.class) return "boolean";
        if (clazz == byte[].class) return "binary";
        if (java.time.Instant.class.isAssignableFrom(clazz) ||
            java.time.OffsetDateTime.class.isAssignableFrom(clazz) ||
            java.time.ZonedDateTime.class.isAssignableFrom(clazz)) return "timestamptz";
        if (java.time.LocalDateTime.class.isAssignableFrom(clazz)) return "timestamp";
        if (java.time.LocalDate.class.isAssignableFrom(clazz) ||
            java.sql.Date.class.isAssignableFrom(clazz)) return "date";
        return "string";
    }
}
