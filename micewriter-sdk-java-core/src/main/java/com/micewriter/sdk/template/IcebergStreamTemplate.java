package com.micewriter.sdk.template;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.micewriter.sdk.annotation.IcebergEntity;
import com.micewriter.sdk.ipc.AckResponse;
import com.micewriter.sdk.ipc.UdsConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * Primary SDK entry point for application code.
 *
 * <pre>{@code
 * // Synchronous (one record at a time):
 * icebergTemplate.send(event);
 *
 * // Pipelined / non-blocking (multiple records in flight):
 * CompletableFuture<Void> f = icebergTemplate.sendAsync(event);
 * }</pre>
 *
 * Each call serializes the POJO to JSON format, frames it
 * with the custom binary header the engine expects, writes it over the Unix Domain
 * Socket.
 *
 * <p>The SDK is append-only. Row-level updates and deletes are not supported.
 *
 * <h2>Async pipelining ({@link #sendAsync})</h2>
 * {@link #sendAsync} returns immediately after acquiring an in-flight permit and writing
 * the frame; the caller can fire many sends before any ACKs arrive. The connection's
 * in-flight byte budget (default 8 MiB) bounds host heap: when the budget is exhausted
 * the calling thread blocks briefly until an ACK clears a slot. Exceptions surface via
 * the returned future rather than being thrown on the calling thread.
 *
 * <h2>Timestamp serialization</h2>
 * Fields mapped to Iceberg {@code timestamptz} columns are JSON-encoded as ISO-8601
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
    private final ObjectMapper jsonMapper;

    public IcebergStreamTemplate(UdsConnection connection) {
        this.connection = connection;
        this.jsonMapper = new ObjectMapper()
                .findAndRegisterModules()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    /**
     * Stream a single {@link IcebergEntity}-annotated POJO to the engine (synchronous).
     *
     * <p>Wire format after the 4-byte length prefix and 1-byte discriminant (0x02):
     * <pre>
     *   [table_name_len : u16 big-endian]
     *   [table_name     : UTF-8 bytes]
     *   [JSON stream bytes]
     * </pre>
     *
     * @throws IllegalArgumentException if the serialized payload exceeds 16 MB
     * @throws RuntimeException         if the engine rejects the record or the ACK times out
     */
    public <T> void send(T entity) {
        byte[] payload = buildIngestPayload(entity);
        AckResponse ack = connection.send(payload);
        if (!ack.isOk()) throw new RuntimeException("Engine rejected record: " + ack.getMsg());
    }

    /**
     * Pipelined, non-blocking send. Returns a future completed when the engine ACKs.
     *
     * <p>Blocks the caller only when the connection's in-flight byte budget is full
     * (natural backpressure). Multiple outstanding futures can be in flight concurrently,
     * letting throughput exceed the single-caller ACK-round-trip ceiling.
     *
     * <p>Any error (timeout, channel drop, engine rejection) completes the returned future
     * exceptionally — it is not thrown on the calling thread.
     *
     * @throws IllegalArgumentException if the serialized payload exceeds 16 MB
     */
    public <T> CompletableFuture<Void> sendAsync(T entity) {
        byte[] payload = buildIngestPayload(entity);
        return connection.sendAsync(payload).thenAccept(ack -> {
            if (!ack.isOk()) throw new RuntimeException("Engine rejected record: " + ack.getMsg());
        });
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
        // No native allocator to close
    }

    /**
     * Builds [type=0x02][tableNameLen u16][tableName][JSON] in one pass,
     * streaming JSON directly into the frame buffer (no redundant full-payload copies).
     */
    private <T> byte[] buildIngestPayload(T entity) {
        byte[] tableNameBytes = IcebergEntityCache.getTableNameBytes(entity.getClass());
        ByteArrayOutputStream baos =
            new ByteArrayOutputStream(3 + tableNameBytes.length + 1024);
        try {
            baos.write(MSG_INGEST_RECORD);
            baos.write((tableNameBytes.length >> 8) & 0xFF);
            baos.write(tableNameBytes.length & 0xFF);
            baos.write(tableNameBytes, 0, tableNameBytes.length);
            jsonMapper.writeValue(baos, entity);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize entity to JSON", e);
        }
        byte[] payload = baos.toByteArray();
        if (payload.length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(
                "INGEST_RECORD payload (" + payload.length + " bytes) exceeds 16 MB limit");
        }
        return payload;
    }
}
