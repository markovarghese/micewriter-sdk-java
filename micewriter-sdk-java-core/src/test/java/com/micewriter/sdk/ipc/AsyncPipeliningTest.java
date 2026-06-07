package com.micewriter.sdk.ipc;

import com.micewriter.sdk.annotation.IcebergEntity;
import com.micewriter.sdk.template.IcebergStreamTemplate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for the bounded-async pipelining path.
 *
 * All tests require Linux (Netty Epoll + EpollDomainSocketChannel).
 * They are automatically skipped on other platforms.
 */
@EnabledOnOs(OS.LINUX)
class AsyncPipeliningTest {

    // -------------------------------------------------------------------------
    // Test entity
    // -------------------------------------------------------------------------

    @IcebergEntity(table = "test_events", namespace = {"test"})
    static final class TestEvent {
        private final String id;
        private final String data;
        TestEvent() { this("", ""); }
        TestEvent(String id, String data) { this.id = id; this.data = data; }
        public String getId() { return id; }
        public String getData() { return data; }
    }

    // -------------------------------------------------------------------------
    // Test 1: Ordering under pipelining
    // -------------------------------------------------------------------------

    @Test
    void pipelinedSendsAllCompleteSuccessfully() throws Exception {
        String path = FakeUdsEngine.tmpSocketPath();
        try (FakeUdsEngine engine = new FakeUdsEngine(path);
             UdsConnection conn = new UdsConnection(path, 5_000, 5_000);
             IcebergStreamTemplate template = new IcebergStreamTemplate(conn)) {

            int n = 8;
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                futures.add(template.sendAsync(new TestEvent("id-" + i, "data-" + i)));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                             .get(10, TimeUnit.SECONDS);

            assertThat(engine.receivedCount()).isEqualTo(n);
            assertThat(futures).allSatisfy(f -> assertThat(f).isCompleted());
        }
    }

    // -------------------------------------------------------------------------
    // Test 2: Throughput improvement (pipelining beats serial bound)
    // -------------------------------------------------------------------------

    @Test
    void pipelinedSendsCompleteFasterThanSerial() throws Exception {
        int n = 6;
        long ackDelayMs = 40;
        long serialBoundMs = (long) (n * ackDelayMs * 0.8); // 80% of serial — clear win threshold

        String path = FakeUdsEngine.tmpSocketPath();
        try (FakeUdsEngine engine = new FakeUdsEngine(path, ackDelayMs);
             UdsConnection conn = new UdsConnection(path, 5_000, 5_000);
             IcebergStreamTemplate template = new IcebergStreamTemplate(conn)) {

            long start = System.currentTimeMillis();
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                futures.add(template.sendAsync(new TestEvent("id-" + i, "payload")));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                             .get(10, TimeUnit.SECONDS);
            long elapsed = System.currentTimeMillis() - start;

            assertThat(elapsed)
                .as("pipelined wall-clock (%dms) should be well below serial bound (%dms)",
                    elapsed, serialBoundMs)
                .isLessThan(serialBoundMs);
        }
    }

    // -------------------------------------------------------------------------
    // Test 3: In-flight byte budget enforced — at most 1 send at a time
    // -------------------------------------------------------------------------

    @Test
    void budgetBoundsInFlightSends() throws Exception {
        // maxInFlightBytes=1 means min(payload.len, 1)=1 permit per send,
        // Semaphore(1) → exactly one send in flight at a time.
        long budget = 1;
        String path = FakeUdsEngine.tmpSocketPath();
        try (FakeUdsEngine engine = new FakeUdsEngine(path, 0, true, 0);
             UdsConnection conn = new UdsConnection(path, 5_000, 5_000, budget)) {

            engine.holdAcks(); // start with all ACKs held

            IcebergStreamTemplate template = new IcebergStreamTemplate(conn);
            TestEvent ev = new TestEvent("x", "y");

            // Send two requests concurrently from background threads.
            CompletableFuture<Void> f1 =
                CompletableFuture.runAsync(() -> template.sendAsync(ev).join());
            CompletableFuture<Void> f2 =
                CompletableFuture.runAsync(() -> template.sendAsync(ev).join());

            // Wait for exactly 1 frame to arrive at the server (the first send acquired the sole permit).
            boolean got1 = engine.awaitFrames(1, 3, TimeUnit.SECONDS);
            assertThat(got1).as("first frame should arrive at server").isTrue();

            // Give f2 time to attempt permit acquisition — it should be blocked.
            Thread.sleep(100);
            assertThat(engine.receivedCount())
                .as("second frame must not arrive while budget is exhausted")
                .isEqualTo(1);

            // Release the first ACK → f1 completes → permit freed → f2 unblocks.
            engine.releaseAcks(1);
            boolean got2 = engine.awaitFrames(2, 3, TimeUnit.SECONDS);
            assertThat(got2).as("second frame should arrive after first ACK").isTrue();

            engine.releaseAcks(1);
            CompletableFuture.allOf(f1, f2).get(5, TimeUnit.SECONDS);
        }
    }

    // -------------------------------------------------------------------------
    // Test 4: Permits fully released on success
    // -------------------------------------------------------------------------

    @Test
    void permitsFullyReleasedAfterSuccess() throws Exception {
        long budget = 10_000;
        String path = FakeUdsEngine.tmpSocketPath();
        try (FakeUdsEngine engine = new FakeUdsEngine(path);
             UdsConnection conn = new UdsConnection(path, 5_000, 5_000, budget)) {

            IcebergStreamTemplate template = new IcebergStreamTemplate(conn);
            int n = 5;
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                futures.add(template.sendAsync(new TestEvent("id-" + i, "data")));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                             .get(10, TimeUnit.SECONDS);

            // All permits must be returned to the initial total.
            int expectedPermits = (int) Math.min(budget, Integer.MAX_VALUE);
            assertThat(conn.availablePermitCount())
                .as("all permits should be released after sends complete")
                .isEqualTo(expectedPermits);
        }
    }

    // -------------------------------------------------------------------------
    // Test 5: ACK timeout — future completes exceptionally, permits released
    // -------------------------------------------------------------------------

    @Test
    void ackTimeoutCompletesExceptionallyAndReleasesPermits() throws Exception {
        int ackTimeoutMs = 300;
        long budget = 10_000;
        String path = FakeUdsEngine.tmpSocketPath();
        // Engine never sends ACKs
        try (FakeUdsEngine engine = new FakeUdsEngine(path, 0, false, 0);
             UdsConnection conn = new UdsConnection(path, 5_000, ackTimeoutMs, budget)) {

            IcebergStreamTemplate template = new IcebergStreamTemplate(conn);
            CompletableFuture<Void> future = template.sendAsync(new TestEvent("t", "timeout"));

            assertThatThrownBy(() -> future.get(ackTimeoutMs * 3L, TimeUnit.MILLISECONDS))
                .isInstanceOf(ExecutionException.class);

            // Brief wait for whenComplete permit-release callbacks to execute.
            Thread.sleep(50);
            assertThat(conn.availablePermitCount())
                .as("permits must be released after timeout")
                .isEqualTo((int) Math.min(budget, Integer.MAX_VALUE));
        }
    }

    // -------------------------------------------------------------------------
    // Test 6: Backward compatibility — synchronous send() and flushNow() unchanged
    // -------------------------------------------------------------------------

    @Test
    void synchronousSendAndFlushNowStillWork() throws Exception {
        String path = FakeUdsEngine.tmpSocketPath();
        try (FakeUdsEngine engine = new FakeUdsEngine(path);
             UdsConnection conn = new UdsConnection(path, 5_000, 5_000);
             IcebergStreamTemplate template = new IcebergStreamTemplate(conn)) {

            assertThatCode(() -> template.send(new TestEvent("sync", "data")))
                .doesNotThrowAnyException();

            assertThatCode(() -> template.flushNow())
                .doesNotThrowAnyException();

            assertThat(engine.receivedCount()).isEqualTo(2);
        }
    }

    // -------------------------------------------------------------------------
    // Test 7: Payload wire format correctness
    // -------------------------------------------------------------------------

    @Test
    void payloadMatchesExpectedWireFormat() throws Exception {
        String path = FakeUdsEngine.tmpSocketPath();
        try (FakeUdsEngine engine = new FakeUdsEngine(path);
             UdsConnection conn = new UdsConnection(path, 5_000, 5_000);
             IcebergStreamTemplate template = new IcebergStreamTemplate(conn)) {

            template.send(new TestEvent("abc", "hello"));

            assertThat(engine.receivedCount()).isEqualTo(1);
            byte[] frame = engine.receivedPayloads().get(0);

            // Byte 0: MSG_INGEST_RECORD = 0x02
            assertThat(frame[0]).isEqualTo((byte) 0x02);

            // Bytes 1-2: table name length (u16 big-endian)
            int tableLen = ((frame[1] & 0xFF) << 8) | (frame[2] & 0xFF);
            assertThat(tableLen).isGreaterThan(0);

            // Bytes 3..(3+tableLen): table name matches @IcebergEntity(table="test_events")
            String tableName = new String(frame, 3, tableLen, java.nio.charset.StandardCharsets.UTF_8);
            assertThat(tableName).isEqualTo("test_events");

            // Remaining bytes: valid JSON containing the entity fields
            String json = new String(frame, 3 + tableLen, frame.length - 3 - tableLen,
                                     java.nio.charset.StandardCharsets.UTF_8);
            assertThat(json).contains("\"id\"").contains("\"abc\"");
            assertThat(json).contains("\"data\"").contains("\"hello\"");
        }
    }
}
