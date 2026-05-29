package com.micewriter.sdk.template;

import com.micewriter.sdk.annotation.IcebergEntity;
import com.micewriter.sdk.ipc.AckResponse;
import com.micewriter.sdk.ipc.UdsConnection;
import com.micewriter.sdk.schema.PojoInspector;
import com.micewriter.sdk.schema.SchemaRegistrar;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Primary SDK entry point for application code.
 *
 * <pre>{@code
 * icebergTemplate.send(event);
 * }</pre>
 *
 * Each call serializes the POJO as an Apache Arrow IPC RecordBatch, frames it
 * with the custom binary header the engine expects, writes it over the Unix Domain
 * Socket, and blocks until the engine ACKs the RocksDB append (microsecond latency).
 *
 * <p>The SDK is append-only. Row-level updates and deletes are not supported.
 */
public class IcebergStreamTemplate implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(IcebergStreamTemplate.class);

    /** Must match {@code MSG_INGEST_RECORD} in the Rust engine's protocol.rs. */
    private static final byte MSG_INGEST_RECORD = 0x02;

    /** Must match {@code MSG_FLUSH_NOW} in the Rust engine's protocol.rs. */
    private static final byte MSG_FLUSH_NOW = 0x03;

    /** Matches {@code MAX_PAYLOAD_SIZE} enforced by the engine's UDS server. */
    private static final int MAX_PAYLOAD_BYTES = 128 * 1024 * 1024;

    private final UdsConnection connection;
    private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    public IcebergStreamTemplate(UdsConnection connection) {
        this.connection = connection;
    }

    /**
     * Stream a single {@link IcebergEntity}-annotated POJO to the engine.
     *
     * <p>Wire format after the 4-byte length prefix and 1-byte discriminant (0x02):
     * <pre>
     *   [table_name_len : u16 big-endian]
     *   [table_name     : UTF-8 bytes]
     *   [schema_id      : i32 big-endian, value=0]
     *   [Arrow IPC stream bytes]
     * </pre>
     *
     * @throws IllegalArgumentException if {@code entity} is not annotated with {@link IcebergEntity},
     *                                  or if the serialized payload exceeds 128 MB
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

        byte[] tableNameBytes = tableName.getBytes(StandardCharsets.UTF_8);

        Schema arrowSchema = PojoInspector.buildArrowSchema(clazz);
        byte[] arrowIpcBytes = PojoInspector.toArrowIpcStream(entity, arrowSchema, allocator);

        // [table_name_len u16][table_name][schema_id i32 = 0][Arrow IPC stream]
        int headerLen = 2 + tableNameBytes.length + 4;
        byte[] body = new byte[headerLen + arrowIpcBytes.length];
        body[0] = (byte) (tableNameBytes.length >> 8);
        body[1] = (byte) (tableNameBytes.length & 0xFF);
        System.arraycopy(tableNameBytes, 0, body, 2, tableNameBytes.length);
        // bytes [2+len .. 2+len+4] stay zero → schema_id = 0
        System.arraycopy(arrowIpcBytes, 0, body, headerLen, arrowIpcBytes.length);

        byte[] payload = SchemaRegistrar.prependTypeByte(MSG_INGEST_RECORD, body);

        if (payload.length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(
                    "INGEST_RECORD payload (" + payload.length + " bytes) exceeds 128 MB limit");
        }

        AckResponse ack = connection.send(payload);
        if (!ack.isOk()) {
            throw new RuntimeException(
                    "Engine rejected record for table '" + tableName + "': " + ack.getMsg());
        }
    }

    /**
     * Force the engine to immediately compile and commit all buffered records.
     * This is intended for end-to-end testing synchronization.
     *
     * @throws RuntimeException if the engine rejects the request (e.g., manual flush is disabled)
     */
    public void flushNow() {
        byte[] payload = new byte[] { MSG_FLUSH_NOW };
        AckResponse ack = connection.send(payload);
        if (!ack.isOk()) {
            throw new RuntimeException("Engine rejected manual flush request: " + ack.getMsg());
        }
    }

    @Override
    public void close() {
        allocator.close();
    }
}
