# Implementation Plan: Bounded-Async Pipelining for the mIceWriter Java SDK (v1)

> **Audience:** an engineer/agent working in the `micewriter-sdk-java-v1` repo (branch `v1`).
> **Status:** ready to implement. Self-contained — all current code paths referenced below have been read and quoted.

---

## 1. Context & goal

Load testing showed the SDK→engine ingest path tops out at **~104 records/s for 1 MB payloads, regardless of offered rate** (cell 11 @ 100/s and cell 15 @ 500/s both achieved ~104/s against a *drop-sink* engine that does zero storage work). That number is the **transport floor**, not a storage limit.

**Root cause (measured + code-confirmed):** the data path is a strictly serial, synchronous request/response. The sandbox load generator (`micewriter-sandbox` `LoadTestService`) drives sends from a **single thread** and `IcebergStreamTemplate.send()` → `UdsConnection.send()` **blocks on the ACK** for each record before the next is issued. So:

```
throughput  ≈  1 / per_send_wall_time  ≈  1 / 9.6 ms  ≈  104 /s
```

where `per_send_wall_time = JSON serialize + 3× ~1 MB array copies + UDS write + idle wait for ACK round-trip`.

**Design constraint from the product owner:** the host application's main job is *not* telemetry, so the SDK must impose **low CPU and memory pressure** on the host app. "A little is okay." This rules out the brute-force fixes (extra sender threads, large batching).

**Goal of this change:** let throughput exceed the ~104/s single-caller ceiling by **pipelining** (multiple sends in flight before their ACKs), while keeping host memory bounded by a small, configurable budget, and lowering per-record CPU/GC by removing redundant 1 MB copies. Use the **threads that already exist** (the single Netty epoll event loop) — add none.

### Why pipelining is the right lever (and the transport already supports it)

`UdsConnection.send()` already holds its `sendLock` **only** for framing + `writeAndFlush`, and **releases it before awaiting the ACK**. ACKs are matched to sends by a FIFO `ackFutures` queue drained in wire order by a single inbound handler. So having many frames in flight is already protocol-correct; the *only* thing serializing the path is the blocking `future.get()`. We expose a non-blocking send and add admission control.

---

## 2. Scope

**In scope (this plan):**
1. `UdsConnection`: add a non-blocking `sendAsync(byte[]) → CompletableFuture<AckResponse>` with a **bounded in-flight byte budget** (admission control) and a per-send ACK timeout. Leave the existing synchronous `send(byte[])` untouched.
2. `IcebergStreamTemplate`: add `sendAsync(T) → CompletableFuture<Void>`; share a single payload-builder that **eliminates the redundant 1 MB copies** for both `send` and `sendAsync`.
3. Config plumbing for the new budget in core (constructor), Spring (`IcebergProperties` + autoconfig), and Dropwizard (`MicewriterConfig` + `MicewriterBundle`).
4. Unit tests.

**Out of scope (but required to *measure* the win — see §8):** changing the sandbox `LoadTestService` to call `sendAsync`. That lives in `micewriter-sandbox` / `micewriter-sandbox-v1`, a different repo. Note it in the PR description as a follow-up.

**Explicitly NOT doing** (conflicts with the low-host-load constraint): adding sender threads / extra UDS connections; batching many 1 MB records per frame.

---

## 3. Files to change

| File (relative to repo root) | Change |
|---|---|
| `micewriter-sdk-java-core/.../ipc/UdsConnection.java` | Add `sendAsync`, in-flight `Semaphore`, per-send timeout; new constructor overload. |
| `micewriter-sdk-java-core/.../template/IcebergStreamTemplate.java` | Add `sendAsync(T)`; extract `buildIngestPayload(T)` with reduced copies; reuse in `send(T)`. |
| `micewriter-sdk-java-spring/.../config/IcebergProperties.java` | Add `maxInFlightBytes` (+ optional `maxInFlightMessages`). |
| `micewriter-sdk-java-spring/.../config/IcebergAutoConfiguration.java` | Pass new prop(s) to the `UdsConnection` bean. |
| `micewriter-sdk-java-dropwizard/.../dropwizard/MicewriterConfig.java` | Add `maxInFlightBytes` (+ optional). |
| `micewriter-sdk-java-dropwizard/.../dropwizard/MicewriterBundle.java` | Pass new field(s) into the `UdsConnection`. |
| `micewriter-sdk-java-core/src/test/...` | New unit tests (§7). |

---

## 4. Detailed design

### 4.1 `UdsConnection` — `sendAsync` + admission control

Current relevant fields (`ipc/UdsConnection.java`):
- `EpollEventLoopGroup group = new EpollEventLoopGroup(1)` — single event-loop thread; all ACK callbacks + scheduled tasks run here.
- `ConcurrentLinkedQueue<CompletableFuture<AckResponse>> ackFutures` — FIFO, one slot per in-flight send.
- `ReentrantLock sendLock` — serializes framing + `writeAndFlush`.
- `AckHandler.channelRead0` polls `ackFutures` and completes futures in order; `cancelPendingFutures` completes all pending exceptionally on channel error/inactive.

**Add:**

```java
private final long maxInFlightBytes;          // memory budget for unacked data-path sends
private final java.util.concurrent.Semaphore inFlightBytes; // permits == bytes

// new constructor (keep the existing 3-arg one delegating with a default)
public UdsConnection(String socketPath, int connectTimeoutMs, int ackTimeoutMs, long maxInFlightBytes) {
    this.socketPath = socketPath;
    this.connectTimeoutMs = connectTimeoutMs;
    this.ackTimeoutMs = ackTimeoutMs;
    this.maxInFlightBytes = maxInFlightBytes;
    // Semaphore permits are int; cap budget and guard the cast.
    int permits = (int) Math.min(maxInFlightBytes, Integer.MAX_VALUE);
    this.inFlightBytes = new java.util.concurrent.Semaphore(permits, /*fair*/ true);
    connect();
}

public UdsConnection(String socketPath, int connectTimeoutMs, int ackTimeoutMs) {
    this(socketPath, connectTimeoutMs, ackTimeoutMs, DEFAULT_MAX_IN_FLIGHT_BYTES); // 8 MiB, see §5
}
```

**`sendAsync` (illustrative):**

```java
/**
 * Non-blocking send. Returns a future completed with the engine ACK.
 * Applies backpressure ONLY when the in-flight byte budget is exhausted:
 * the calling thread blocks on permit acquisition (bounded by ackTimeoutMs),
 * which bounds host heap to ~maxInFlightBytes of unacked payloads.
 * Unlike send(), this does NOT transparently retry — a timeout or channel
 * error completes the future exceptionally (see §4.3 rationale).
 */
public CompletableFuture<AckResponse> sendAsync(byte[] payload) {
    final int permits = (int) Math.min(payload.length, maxInFlightBytes);

    // 1) Admission control OUTSIDE sendLock so a full window never blocks
    //    control-plane sends. Bound the wait so we can't hang forever.
    try {
        if (!inFlightBytes.tryAcquire(permits, ackTimeoutMs, TimeUnit.MILLISECONDS)) {
            CompletableFuture<AckResponse> f = new CompletableFuture<>();
            f.completeExceptionally(new RuntimeException(
                "in-flight budget exhausted (" + maxInFlightBytes + " bytes) — engine backpressure"));
            return f;
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        CompletableFuture<AckResponse> f = new CompletableFuture<>();
        f.completeExceptionally(new RuntimeException("interrupted acquiring in-flight budget", e));
        return f;
    }

    final CompletableFuture<AckResponse> future = new CompletableFuture<>();

    // 2) Release permits EXACTLY ONCE on any completion (ok / error / timeout / channel drop).
    future.whenComplete((ack, err) -> inFlightBytes.release(permits));

    // 3) Per-send ACK timeout on the event loop. On fire, drop the channel;
    //    channelInactive -> cancelPendingFutures completes this (and any other
    //    in-flight) future exceptionally, which releases their permits.
    final io.netty.util.concurrent.ScheduledFuture<?> timeoutTask =
        group.next().schedule(() -> {
            if (!future.isDone()) {
                log.warn("async send ACK timeout after {}ms, dropping channel", ackTimeoutMs);
                dropChannel();
            }
        }, ackTimeoutMs, TimeUnit.MILLISECONDS);
    future.whenComplete((ack, err) -> timeoutTask.cancel(false));

    // 4) Frame + enqueue + write under sendLock so ackFutures order == wire order.
    sendLock.lock();
    try {
        ensureConnected();
        ByteBuf buf = Unpooled.buffer(4 + payload.length);
        buf.writeInt(payload.length);
        buf.writeBytes(payload);
        ackFutures.offer(future);
        try {
            channel.writeAndFlush(buf);
        } catch (Exception writeEx) {
            ackFutures.remove(future);
            dropChannel();
            future.completeExceptionally(new RuntimeException("IPC channel write failed", writeEx));
        }
    } catch (Exception e) {
        dropChannel();
        future.completeExceptionally(new RuntimeException("IPC channel setup failed", e));
    } finally {
        sendLock.unlock();
    }
    return future;
}
```

Also add a constant:
```java
private static final long DEFAULT_MAX_IN_FLIGHT_BYTES = 8L * 1024 * 1024; // 8 MiB
```

**Leave `send(byte[])` exactly as-is.** It keeps its proven retry-on-timeout/reconnect behavior and is used only by the control plane (`SchemaRegistrar.registerSchema`, `IcebergStreamTemplate.flushNow`) where volume is negligible. Do **not** route it through the budget — keeping the blast radius small. (A data-path `send(T)` that calls the sync `connection.send()` still benefits from the copy reduction in §4.2.)

### 4.2 `IcebergStreamTemplate` — `sendAsync(T)` + copy reduction

Current `send(T)` (`template/IcebergStreamTemplate.java:85-114`) does, per record:
`writeValueAsBytes` (alloc JSON) → `body[]` (alloc + arraycopy) → `prependTypeByte` (alloc + arraycopy) → `Unpooled.buffer` (alloc + copy in `UdsConnection`). That's ~4 large allocations and the JSON bytes are copied 3× after creation.

**Extract a single builder that streams JSON once into the combined frame body:**

```java
/** Builds [type=0x02][tableNameLen u16][tableName][JSON] in one pass (no redundant full-payload copies). */
private <T> byte[] buildIngestPayload(T entity) {
    byte[] tableNameBytes = IcebergEntityCache.getTableNameBytes(entity.getClass());
    // Initial size: header + a generous guess; ByteArrayOutputStream grows as needed.
    java.io.ByteArrayOutputStream baos =
        new java.io.ByteArrayOutputStream(3 + tableNameBytes.length + 1024);
    try {
        baos.write(MSG_INGEST_RECORD);                       // type discriminant
        baos.write((tableNameBytes.length >> 8) & 0xFF);     // u16 BE high
        baos.write(tableNameBytes.length & 0xFF);            // u16 BE low
        baos.write(tableNameBytes, 0, tableNameBytes.length);
        jsonMapper.writeValue(baos, entity);                 // JSON streamed directly into the buffer
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
```

This drops the two `System.arraycopy` of the full payload and the separate `prependTypeByte` allocation; JSON is written once into the frame buffer (then `toByteArray` + the `ByteBuf` copy in `UdsConnection`). Net: ~4 large allocations → ~2, and no full-payload `arraycopy`.

**Rewrite `send(T)` and add `sendAsync(T)`:**

```java
public <T> void send(T entity) {
    byte[] payload = buildIngestPayload(entity);
    AckResponse ack = connection.send(payload);             // sync path unchanged
    if (!ack.isOk()) throw new RuntimeException("Engine rejected record: " + ack.getMsg());
}

/**
 * Pipelined, non-blocking send. Completes when the engine ACKs.
 * Blocks the caller only when the connection's in-flight byte budget is full
 * (natural backpressure). Use this to exceed the single-caller throughput ceiling.
 */
public <T> CompletableFuture<Void> sendAsync(T entity) {
    byte[] payload = buildIngestPayload(entity);            // serialization happens on caller thread
    return connection.sendAsync(payload).thenAccept(ack -> {
        if (!ack.isOk()) throw new RuntimeException("Engine rejected record: " + ack.getMsg());
    });
}
```

Remove the stray `e.printStackTrace()` (old line 92) during the refactor. Update the class Javadoc to document `sendAsync`, the in-flight budget, and that exceptions surface via the returned future.

> **Optional stretch (max GC reduction, higher risk — only if time permits):** add a `sendAsync(ByteBuf)` overload to `UdsConnection` and have the template serialize directly into a *pooled* `ByteBuf` (reserve 4 leading bytes, write header+JSON via `ByteBufOutputStream`, back-fill the length prefix, hand ownership to the connection). This removes the final copy entirely but requires careful buffer `release()` on **every** completion path (ok/error/timeout/drop). If you do this, the `whenComplete` permit-release is the natural place to also release the buffer. Keep the `byte[]` path as the default to limit risk.

### 4.3 Why no transparent retry in `sendAsync`

The sync `send()` retries by dropping+reconnecting and re-sending the *single* in-flight frame. With N frames in flight, a timeout/drop affects all of them and re-sending one would reorder the stream (corrupting the FIFO ACK matching). So `sendAsync` completes the affected futures **exceptionally** and lets the caller decide. Reconnection still happens lazily via `ensureConnected()` on the next send, and `SchemaRegistrar`'s reconnect listener re-registers schemas. Document this clearly in the method Javadoc.

---

## 5. Configuration

Add **`maxInFlightBytes`** (primary; bounds host heap) and optionally **`maxInFlightMessages`** (guards the tiny-record case from unbounded future objects; can be deferred).

**Default: `8 * 1024 * 1024` (8 MiB).** Rationale: bounds worst-case unacked data-path heap to ~8 MiB; with 1 MiB records that's up to 8 pipelined sends — enough to hide the ~10 ms RTT — while staying tiny relative to host app heaps and the engine's 512 MiB. Fully configurable for apps with larger payloads or tighter budgets.

> Note: if `maxInFlightBytes` < a single payload, `sendAsync` acquires `min(len, budget)` permits, so an oversized record still sends (alone). To actually pipeline large records, set the budget ≥ a few × the max expected payload. Document this.

**Spring** — `IcebergProperties.java` (mirror the existing int props):
```java
/** Max bytes of un-ACKed data-path sends held in memory (pipelining/backpressure window). */
private long maxInFlightBytes = 8L * 1024 * 1024;
public long getMaxInFlightBytes() { return maxInFlightBytes; }
public void setMaxInFlightBytes(long v) { this.maxInFlightBytes = v; }
```
`IcebergAutoConfiguration.udsConnection(...)` → use the 4-arg constructor:
```java
return new UdsConnection(props.getSocketPath(), props.getConnectTimeoutMs(),
                         props.getAckTimeoutMs(), props.getMaxInFlightBytes());
```

**Dropwizard** — `MicewriterConfig.java` (mirror the existing `@JsonProperty @Min` fields):
```java
@Min(1) @JsonProperty private long maxInFlightBytes = 8L * 1024 * 1024;
public long getMaxInFlightBytes() { return maxInFlightBytes; }
public void setMaxInFlightBytes(long v) { this.maxInFlightBytes = v; }
```
`MicewriterBundle.run(...)` → 4-arg constructor with `cfg.getMaxInFlightBytes()`.

---

## 6. Concurrency correctness checklist (verify each)

- [ ] Permits acquired **before** `ackFutures.offer` / write; released **exactly once** via `future.whenComplete` (covers ok, engine-error ack, timeout, and `cancelPendingFutures` on channel drop).
- [ ] `ackFutures` order == wire order: `offer` + `writeAndFlush` stay under `sendLock` (unchanged invariant).
- [ ] Admission `tryAcquire` happens **outside** `sendLock` (a full window must not block control-plane `send()`).
- [ ] Per-send timeout scheduled on `group`; **cancelled** when the future completes; on fire it calls `dropChannel()` (mirrors sync semantics) rather than completing just one future out of band.
- [ ] `whenComplete` callbacks are cheap and must **not** block the event-loop thread.
- [ ] Reconnect interplay: after a drop, in-flight futures complete exceptionally (permits released), schemas re-register via the existing listener, subsequent `sendAsync` calls reconnect lazily. Add a test.
- [ ] No permit leak on the early `tryAcquire`-fail and write-failure paths (in those we either never acquired or must release — re-check the sketch: on `tryAcquire` failure we return before acquiring → no release; on write failure the future is completed exceptionally → `whenComplete` releases. Good.)

---

## 7. Testing

Add tests under `micewriter-sdk-java-core/src/test/java/...`. Use a **fake UDS engine**: bind an `EpollServerDomainSocketChannel` to a temp socket path that decodes the 4-byte-length frames and writes back JSON ACKs (`{"status":"ok"}`), with a configurable per-frame delay to simulate RTT. (If the existing repo already has a test harness/fake server, reuse it — check `src/test` first.)

Cases:
1. **Ordering under pipelining:** fire N `sendAsync` without waiting; assert all complete OK and ACKs map 1:1 in order.
2. **Throughput improvement:** with the fake engine delaying each ACK by D ms, N pipelined `sendAsync` complete in ≈ `N·serialize + D` rather than `N·(serialize + D)`; assert wall-clock well below the serial bound.
3. **Memory budget enforced:** set `maxInFlightBytes` small (e.g. 2 MiB) with a slow fake engine; assert no more than `budget/payload` sends are outstanding at once (e.g., instrument the fake server's concurrent-in-flight count, or assert the caller blocks).
4. **Permit release on success and on error:** after K sends complete, all permits are available again (`inFlightBytes.availablePermits()` back to full) — may need a package-private accessor or reflection.
5. **ACK timeout:** fake engine that never ACKs → future completes exceptionally within ~`ackTimeoutMs`; permits released; channel dropped.
6. **Backward compatibility:** existing synchronous `send(T)` / `flushNow()` still work unchanged against the fake engine.
7. **Payload correctness:** decode a frame on the fake server and assert bytes == `[0x02][tableLen u16][table][JSON]` (guards the §4.2 refactor).

Run: `mvn -q test` from the repo root (multi-module reactor; the core module holds the logic).

---

## 8. Validation (end-to-end, after merge)

The SDK change alone won't move the load-test number until a caller uses `sendAsync`. To re-measure the transport ceiling:

1. In `micewriter-sandbox-v1` `LoadTestService`, change the tick to call `icebergTemplate.sendAsync(event)` instead of the blocking `send(event)` (and record latency via the future completion). This is a **separate repo / separate task** — call it out as a follow-up.
2. Rebuild+push the sandbox image, recreate the pod.
3. With the engine still in **drop-sink** mode (isolates transport), re-run cell 11 (1 MB @ 100/s) and cell 15 (1 MB @ 500/s) for 3 min via the `run-load-test-sweep` skill / `POST /loadtest/sweep`.
4. **Expected:** achieved rate rises above ~104/s (toward the serialize-bound ceiling) with host engine memory still bounded near the configured window; SDK p95 may rise (queuing) — that's expected and fine.

> Reminder unrelated to this plan: the engine is currently in a temporary drop-sink mode (`micewriter-engine-v1` `uds_server.rs` returns `AckResponse::ok()` early). Revert + rebuild + redeploy before any real use.

**Caveat to set expectations:** against the *real* engine the bottleneck becomes RocksDB + flush (measured ~4–45 MB/s on this cell), so faster SDK sends will hit engine backpressure sooner rather than increasing end-to-end throughput. This SDK change ensures the SDK is never the limiter and that a faster/scaled engine isn't starved by a single synchronous caller — it is not a standalone throughput win.

---

## 9. Backward compatibility & delivery

- All changes are **additive**: new `sendAsync` methods, a new constructor overload (old 3-arg retained), new config props with defaults. No existing signatures change. SemVer **minor**.
- The repo uses conventional commits + release-please (`.github/workflows/`). Use `feat:` commits, e.g. `feat(core): add bounded-async sendAsync to UdsConnection/IcebergStreamTemplate`. Keep config and core changes coherent in the PR.
- Update module READMEs (`micewriter-sdk-java-spring/README.md`, `micewriter-sdk-java-dropwizard/README.md`) with the `maxInFlightBytes` setting and a short `sendAsync` usage example.

## 10. Suggested commit breakdown

1. `feat(core): add buildIngestPayload + reduce per-record copies in IcebergStreamTemplate`
2. `feat(core): add bounded-async sendAsync + in-flight byte budget to UdsConnection`
3. `feat(core): IcebergStreamTemplate.sendAsync`
4. `feat(spring,dropwizard): expose maxInFlightBytes config`
5. `test(core): pipelining ordering, budget, timeout, backcompat`
6. `docs: README updates for sendAsync / maxInFlightBytes`
