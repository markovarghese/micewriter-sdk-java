# micewriter-sdk-java-spring

Spring Boot starter for the mIceWriter SDK. Add this single dependency to any Spring Boot
application to start streaming telemetry to the `micewriter-engine` sidecar.

## Dependency

```xml
<dependency>
    <groupId>com.micewriter</groupId>
    <artifactId>micewriter-sdk-java-spring</artifactId>
    <version>0.2.0</version>
</dependency>
```

Transitively pulls in `micewriter-sdk-java-core` (Netty, Arrow, Jackson).
Does **not** pull in Dropwizard.

## Quickstart

### 1. Annotate your entity

```java
@IcebergEntity(table = "telemetry_events", namespace = "analytics")
public class TelemetryEvent {
    @IcebergId private String id;
    private String source;
    private String payload;
    private int severity;
    private Instant occurredAt;
    // getters / constructors …
}
```

### 2. Inject and send

```java
@Service
public class EventService {
    @Autowired
    private IcebergStreamTemplate icebergTemplate;

    // Pipelined with automatic retry (recommended):
    public CompletableFuture<Void> record(TelemetryEvent event) {
        return icebergTemplate.sendAsyncWithRetry(event);  // 3 attempts, 100 ms → 200 ms backoff
    }

    // Custom retry policy:
    public CompletableFuture<Void> recordWithPolicy(TelemetryEvent event) {
        return icebergTemplate.sendAsyncWithRetry(event, /*maxAttempts*/ 5, /*initialMs*/ 50, /*maxMs*/ 10_000);
    }
}
```

`sendAsyncWithRetry` pipelines multiple records before their ACKs arrive (breaking the
single-caller throughput ceiling) and retries with exponential backoff on channel drops or
timeouts. The in-flight byte budget (default 8 MiB, see `max-in-flight-bytes`) bounds host
memory. Errors surface via the returned future only after all retry attempts are exhausted.

### 3. Configure

```yaml
# application.yml
micewriter:
  socket-path: /var/run/app/iceberg.sock
  base-package: com.example.events   # narrows @IcebergEntity classpath scan; omit to scan all
  connect-timeout-ms: 5000
  ack-timeout-ms: 5000
  enabled: true
  # Pipelining window: max un-ACKed bytes in flight (default 8 MiB).
  # Set to several × your max payload size to allow effective concurrent sends.
  max-in-flight-bytes: 8388608
```

## How it works

On `ContextRefreshedEvent`, `SpringSchemaRegistrar` classpath-scans for `@IcebergEntity`
classes and calls `SchemaRegistrar.register()` which sends a `REGISTER_SCHEMA` (0x01)
JSON message to the engine for each class. After that, every `sendAsyncWithRetry()` call
serialises the POJO as JSON and sends it as `INGEST_RECORD` (0x02), pipelining up to
`max-in-flight-bytes` of unacknowledged data and retrying automatically on transient errors.
