package com.micewriter.sdk.template;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.micewriter.sdk.annotation.IcebergEntity;
import com.micewriter.sdk.ipc.AckResponse;
import com.micewriter.sdk.ipc.UdsConnection;
import com.micewriter.sdk.schema.PojoInspector;
import com.micewriter.sdk.schema.SchemaRegistrar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Primary SDK entry point for application code.
 *
 * <pre>{@code
 * @Autowired
 * private IcebergStreamTemplate icebergTemplate;
 *
 * public void handleEvent(TelemetryEvent event) {
 *     icebergTemplate.send(event);   // non-blocking from the app's perspective
 * }
 * }</pre>
 *
 * Each call serialises the POJO as an {@code IngestRecord} JSON frame, writes it
 * over the Unix Domain Socket, and blocks until the engine ACKs the append to
 * RocksDB (microsecond latency — the engine does NOT wait for S3 here).
 */
public class IcebergStreamTemplate {

    private static final Logger log = LoggerFactory.getLogger(IcebergStreamTemplate.class);

    /** Must match {@code MSG_INGEST_RECORD} in the Rust engine's protocol.rs. */
    private static final byte MSG_INGEST_RECORD = 0x02;

    private final UdsConnection connection;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public IcebergStreamTemplate(UdsConnection connection) {
        this.connection = connection;
    }

    /**
     * Stream a single {@link IcebergEntity}-annotated POJO to the engine.
     *
     * @throws IllegalArgumentException if {@code entity} is not annotated with {@link IcebergEntity}
     * @throws RuntimeException         if the engine rejects the record or the ACK times out
     */
    public <T> void send(T entity) {
        Class<?> clazz = entity.getClass();
        IcebergEntity ann = clazz.getAnnotation(IcebergEntity.class);
        if (ann == null) {
            throw new IllegalArgumentException(
                    clazz.getName() + " must be annotated with @IcebergEntity");
        }

        String tableName = ann.table().isEmpty()
                ? clazz.getSimpleName().toLowerCase(Locale.ROOT)
                : ann.table();

        // Build the IngestRecord JSON: { "table": "...", "fields": [["name", value], ...] }
        // The fields list is Vec<(String, Value)> in Rust → serialised as array-of-2-arrays.
        List<List<Object>> fields = PojoInspector.extractFields(entity);

        Map<String, Object> record = new LinkedHashMap<>();
        record.put("table", tableName);
        record.put("fields", fields);

        try {
            byte[] json = objectMapper.writeValueAsBytes(record);
            byte[] payload = SchemaRegistrar.prependTypeByte(MSG_INGEST_RECORD, json);

            AckResponse ack = connection.send(payload);
            if (!ack.isOk()) {
                throw new RuntimeException("Engine rejected record for table '" + tableName + "': " + ack.getMsg());
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to send entity to micewriter-engine", e);
        }
    }
}
