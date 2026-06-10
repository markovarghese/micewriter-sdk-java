# micewriter-sdk-java-dropwizard

Dropwizard bundle for the mIceWriter SDK. Provides the same zero-boilerplate experience as
the Spring Boot starter: lifecycle management, config binding from `config.yml`, and
automatic schema registration.

## Dependency

```xml
<dependency>
    <groupId>com.micewriter</groupId>
    <artifactId>micewriter-sdk-java-dropwizard</artifactId>
    <version>0.2.0</version>
</dependency>
```

Transitively pulls in `micewriter-sdk-java-core` (Netty, Arrow, Jackson).
Does **not** pull in Spring.

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

### 2. Add `MicewriterConfig` to your application config

```java
public class AppConfig extends Configuration {

    @Valid @NotNull
    private MicewriterConfig micewriter = new MicewriterConfig();

    @JsonProperty("micewriter")
    public MicewriterConfig getMicewriter() { return micewriter; }
}
```

### 3. Register the bundle in `Application.initialize()`

```java
public class App extends Application<AppConfig> {

    private final MicewriterBundle<AppConfig> micewriter =
        new MicewriterBundle<>(AppConfig::getMicewriter)
            .entities(TelemetryEvent.class);   // list every @IcebergEntity class

    @Override
    public void initialize(Bootstrap<AppConfig> bootstrap) {
        bootstrap.addBundle(micewriter);
    }

    @Override
    public void run(AppConfig config, Environment env) {
        // retrieve the template after the bundle has run
        IcebergStreamTemplate template = micewriter.getTemplate();
        env.jersey().register(new EventResource(template));
    }
}
```

### 4. Configure (`config.yml`)

```yaml
micewriter:
  socketPath: /var/run/app/iceberg.sock
  connectTimeoutMs: 5000
  ackTimeoutMs: 5000
  # Pipelining window: max un-ACKed bytes in flight (default 8 MiB).
  # Set to several × your max payload size to allow effective concurrent sends.
  maxInFlightBytes: 8388608
```

### 5. Pipelined sends with retry

```java
// In your resource/handler:
IcebergStreamTemplate template = micewriter.getTemplate();

// Pipelined with automatic retry (recommended):
CompletableFuture<Void> f = template.sendAsyncWithRetry(event);  // 3 attempts, 100 ms → 200 ms backoff

// Custom retry policy:
CompletableFuture<Void> f2 = template.sendAsyncWithRetry(event, /*maxAttempts*/ 5, /*initialMs*/ 50, /*maxMs*/ 10_000);
```

`sendAsyncWithRetry` pipelines multiple records before their ACKs arrive (breaking the
single-caller throughput ceiling) and retries with exponential backoff on channel drops or
timeouts. The in-flight byte budget (default 8 MiB, see `maxInFlightBytes`) bounds host
memory. Errors surface via the returned future only after all retry attempts are exhausted.

## How it works

`MicewriterBundle.run()` creates the `UdsConnection`, `IcebergStreamTemplate`, and
`SchemaRegistrar`, then registers them as a Dropwizard `Managed` object.
On server start, `Managed.start()` calls `SchemaRegistrar.register(entityClasses)` which
sends `REGISTER_SCHEMA` (0x01) for each declared entity class.
On server stop, `Managed.stop()` closes the Arrow allocator and Netty channel cleanly.
Every `sendAsyncWithRetry()` call serialises the POJO as JSON and sends it as
`INGEST_RECORD` (0x02), pipelining up to `maxInFlightBytes` of unacknowledged data and
retrying automatically on transient errors.

Unlike the Spring starter, entity classes must be listed **explicitly** via `.entities(…)`
because Dropwizard does not provide a classpath scanner.
