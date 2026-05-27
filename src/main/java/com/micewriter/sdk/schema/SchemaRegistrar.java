package com.micewriter.sdk.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.micewriter.sdk.annotation.IcebergEntity;
import com.micewriter.sdk.ipc.AckResponse;
import com.micewriter.sdk.ipc.UdsConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Scans the classpath for {@link IcebergEntity}-annotated classes on
 * {@link ContextRefreshedEvent} and sends one {@code REGISTER_SCHEMA} IPC message
 * per discovered entity to ensure the engine has created the corresponding Iceberg
 * table before the first record is written.
 */
public class SchemaRegistrar implements ApplicationListener<ContextRefreshedEvent> {

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistrar.class);

    /** IPC type discriminant — must match {@code MSG_REGISTER_SCHEMA} in the Rust engine. */
    private static final byte MSG_REGISTER_SCHEMA = 0x01;

    private final UdsConnection connection;
    private final String basePackage;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** Guard against double-registration on multi-context Spring Boot apps. */
    private final AtomicBoolean registered = new AtomicBoolean(false);

    public SchemaRegistrar(UdsConnection connection, String basePackage) {
        this.connection = connection;
        this.basePackage = basePackage;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (!registered.compareAndSet(false, true)) {
            return;
        }

        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(IcebergEntity.class));

        Set<BeanDefinition> candidates = scanner.findCandidateComponents(basePackage);
        log.info("Found {} @IcebergEntity class(es) to register", candidates.size());

        for (BeanDefinition bd : candidates) {
            try {
                Class<?> clazz = Class.forName(bd.getBeanClassName());
                registerSchema(clazz);
            } catch (Exception e) {
                log.error("Schema registration failed for {}: {}", bd.getBeanClassName(), e.getMessage(), e);
            }
        }
    }

    // -------------------------------------------------------------------------

    private void registerSchema(Class<?> clazz) throws Exception {
        IcebergEntity ann = clazz.getAnnotation(IcebergEntity.class);
        String tableName = ann.table().isEmpty()
                ? clazz.getSimpleName().toLowerCase(Locale.ROOT)
                : ann.table();

        List<Map<String, Object>> fields = new ArrayList<>();
        for (java.lang.reflect.Field f : clazz.getDeclaredFields()) {
            Map<String, Object> fieldDef = new LinkedHashMap<>();
            fieldDef.put("name", f.getName());
            fieldDef.put("type", PojoInspector.javaTypeToIcebergType(f.getType()));
            fieldDef.put("required", !f.getType().isPrimitive()
                    ? f.isAnnotationPresent(com.micewriter.sdk.annotation.IcebergId.class)
                    : true);
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
}
