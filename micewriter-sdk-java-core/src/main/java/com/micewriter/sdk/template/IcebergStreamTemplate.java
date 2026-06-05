package com.micewriter.sdk.template;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.micewriter.sdk.annotation.IcebergEntity;
import com.micewriter.sdk.ipc.AckResponse;
import com.micewriter.sdk.ipc.UdsConnection;
import com.micewriter.sdk.schema.SchemaRegistrar;
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
 * Each call serializes the POJO to CBOR format, frames it
 * with the custom binary header the engine expects, writes it over the Unix Domain
 * Socket asynchronously.
 *
 * <p>The SDK is append-only. Row-level updates and deletes are not supported.
 *
 * <h2>Timestamp serialization</h2>
 * Fields mapped to Iceberg {@code timestamptz} columns are CBOR-encoded as ISO-8601
 * strings (Jackson is configured with {@code WRITE_DATES_AS_TIMESTAMPS=false}). The
 * engine's arrow-json parser accepts numeric UTC offsets ({@code Z} or
 * {@code +HH:MM}) but rejects named timezones like {@code "UTC"} or bracketed zone
 * suffixes like {@code 2026-05-30T07:30Z[UTC]}.
 *
 * <p>Safe field types:
 * <ul>
 *   <li>{@link java.time.Instant} — always serialises as {@code ...Z}</li>
 *   <li>{@link java.time.OffsetDateTime} — serialises with a numeric offset</li>
 * </ul>
 *
 * <p>Avoid {@link java.time.ZonedDateTime} for {@code timestamptz} columns: depending
 * on the Jackson version it may emit {@code 2026-05-30T07:30Z[UTC]} (or a similar
 * bracketed zone), which the engine cannot parse and will cause the entire flush
 * batch to be retained for retry. Convert to {@code Instant} or
 * {@code OffsetDateTime} at the entity boundary.
 */
public class IcebergStreamTemplate implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(IcebergStreamTemplate.class);

    /** Must match {@code MSG_INGEST_RECORD} in the Rust engine's protocol.rs. */
    private static final byte MSG_INGEST_RECORD = 0x02;

    /** Must match {@code MSG_FLUSH_NOW} in the Rust engine's protocol.rs. */
    private static final byte MSG_FLUSH_NOW = 0x03;

    /** Matches {@code MAX_PAYLOAD_SIZE} enforced by the engine's UDS server. */
    private static final int MAX_PAYLOAD_BYTES = 16 * 1024 * 1024;

    private final UdsConnection connection;
    private final ObjectMapper cborMapper;

    public IcebergStreamTemplate(UdsConnection connection) {
        this.connection = connection;
        this.cborMapper = new ObjectMapper(new CBORFactory())
                .findAndRegisterModules()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    /**
     * Stream a single {@link IcebergEntity}-annotated POJO to the engine.
     *
     * <p>Wire format after the 4-byte length prefix and 1-byte discriminant (0x02):
     * <pre>
     *   [table_name_len : u16 big-endian]
     *   [table_name     : UTF-8 bytes]
     *   [CBOR stream bytes]
     * </pre>
     *
     * @throws IllegalArgumentException if {@code entity} is not annotated with {@link IcebergEntity},
     *                                  or if the serialized payload exceeds 16 MB
     * @throws RuntimeException         if the engine rejects the record or the ACK times out
     */
    public <T> void send(T entity) {
        byte[] tableNameBytes = IcebergEntityCache.getTableNameBytes(entity.getClass());

        byte[] cborBytes;
        try {
            cborBytes = cborMapper.writeValueAsBytes(entity);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize entity to CBOR", e);
        }

        // [table_name_len u16][table_name][CBOR bytes]
        int headerLen = 2 + tableNameBytes.length;
        byte[] body = new byte[headerLen + cborBytes.length];
        body[0] = (byte) (tableNameBytes.length >> 8);
        body[1] = (byte) (tableNameBytes.length & 0xFF);
        System.arraycopy(tableNameBytes, 0, body, 2, tableNameBytes.length);
        System.arraycopy(cborBytes, 0, body, headerLen, cborBytes.length);

        byte[] payload = SchemaRegistrar.prependTypeByte(MSG_INGEST_RECORD, body);

        if (payload.length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(
                    "INGEST_RECORD payload (" + payload.length + " bytes) exceeds 16 MB limit");
        }

        AckResponse ack = connection.send(payload);
        if (!ack.isOk()) {
            throw new RuntimeException("Engine rejected record: " + ack.getMsg());
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
        // No native allocator to close anymore
    }
}
