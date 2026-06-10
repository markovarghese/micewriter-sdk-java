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
import java.util.concurrent.TimeUnit;

/**
 * Primary SDK entry point for application code.
 *
 * <pre>{@code
 * // Pipelined with automatic retry (recommended for data path):
 * icebergTemplate.sendAsyncWithRetry(event);
 *
 * // Pipelined without retry (caller handles errors):
 * icebergTemplate.sendAsync(event);
 * }</pre>
 *
 * Each call serializes the POJO to JSON format, frames it
 * with the custom binary header the engine expects, writes it over the Unix Domain
 * Socket.
 *
 * <p>The SDK is append-only. Row-level updates and deletes are not supported.
 *
 * <h2>Async pipelining ({@link #sendAsyncWithRetry} / {@link #sendAsync})</h2>
 * Both methods return immediately after acquiring an in-flight permit and writing the frame;
 * the caller can fire many sends before any ACKs arrive. The connection's in-flight byte
 * budget (default 8 MiB) bounds host heap: when the budget is exhausted the calling thread
 * blocks briefly until an ACK clears a slot. Exceptions surface via the returned future.
 *
 * <p>{@link #sendAsyncWithRetry} adds bounded retry with exponential backoff on top of
 * {@link #sendAsync} — prefer it for data-path sends. {@link #sendAsync} is the primitive
 * for callers that need custom retry or error-handling policy.
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
     * @deprecated Use {@link #sendAsyncWithRetry(Object)} instead, which pipelines sends
     *             and provides configurable retry with exponential backoff. This method will
     *             be removed in the next major version.
     */
    @Deprecated
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
     * Pipelined send with bounded retry and exponential backoff.
     *
     * <p>On any failure (channel drop, timeout, engine rejection), waits
     * {@code initialDelayMs} then retries, doubling the delay each attempt up to
     * {@code maxDelayMs}. After {@code maxAttempts} total attempts the returned
     * future completes exceptionally.
     *
     * <p>Retry is safe: failed sends release their in-flight permits and the connection
     * reconnects (re-registering schemas via the existing reconnect listener) before
     * the next attempt.
     *
     * @param maxAttempts    total attempts including the first (≥ 1)
     * @param initialDelayMs delay before the first retry in milliseconds
     * @param maxDelayMs     cap on retry delay in milliseconds
     * @throws IllegalArgumentException if the serialized payload exceeds 16 MB
     */
    public <T> CompletableFuture<Void> sendAsyncWithRetry(
            T entity, int maxAttempts, long initialDelayMs, long maxDelayMs) {
        return sendAsyncAttempt(entity, 1, maxAttempts, initialDelayMs, maxDelayMs);
    }

    /**
     * Pipelined send with retry using sensible defaults: 3 attempts,
     * 100 ms initial delay doubling up to 2 s.
     *
     * @throws IllegalArgumentException if the serialized payload exceeds 16 MB
     */
    public <T> CompletableFuture<Void> sendAsyncWithRetry(T entity) {
        return sendAsyncWithRetry(entity, 3, 100, 2_000);
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

    private <T> CompletableFuture<Void> sendAsyncAttempt(
            T entity, int attempt, int maxAttempts, long delayMs, long maxDelayMs) {
        return sendAsync(entity).exceptionallyCompose(ex -> {
            if (attempt >= maxAttempts) {
                CompletableFuture<Void> failed = new CompletableFuture<>();
                failed.completeExceptionally(
                    new RuntimeException("sendAsync failed after " + maxAttempts + " attempt(s)", ex));
                return failed;
            }
            log.warn("sendAsync attempt {}/{} failed: {}; retrying in {} ms",
                     attempt, maxAttempts, ex.getMessage(), delayMs);
            long nextDelay = Math.min(delayMs * 2, maxDelayMs);
            return CompletableFuture
                .runAsync(() -> {}, CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS))
                .thenCompose(__ -> sendAsyncAttempt(entity, attempt + 1, maxAttempts, nextDelay, maxDelayMs));
        });
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
