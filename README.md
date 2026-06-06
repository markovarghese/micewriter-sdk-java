# micewriter-sdk-java

Java SDK for the [mIceWriter telemetry ingestion pipeline](../micewriter-hub/README.md).
Streams `@IcebergEntity`-annotated POJOs to the `micewriter-engine` sidecar over a Unix Domain
Socket using Apache Arrow IPC encoding.

## Modules

| Module | Artifact | Use when |
|---|---|---|
| **core** | `micewriter-sdk-java-core` | Framework-agnostic base — used transitively by both starters |
| **spring** | `micewriter-sdk-java-spring` | Spring Boot applications |
| **dropwizard** | `micewriter-sdk-java-dropwizard` | Dropwizard applications |

All modules are in this repo and released together at the same version.

---

## Spring Boot

### Dependency

```xml
<dependency>
    <groupId>com.micewriter</groupId>
    <artifactId>micewriter-sdk-java-spring</artifactId>
    <version>0.2.0</version>
</dependency>
```

### Annotate your entity

```java
@IcebergEntity(table = "telemetry_events", namespace = "analytics")
public class TelemetryEvent {
    @IcebergId private String id;
    private String source;
    private String payload;
    private int severity;
    private Instant occurredAt;
}
```

### Inject and send

```java
@Service
public class EventService {
    @Autowired
    private IcebergStreamTemplate icebergTemplate;

    public void record(TelemetryEvent event) {
        icebergTemplate.send(event);   // blocks until engine ACKs RocksDB append
    }
}
```

### Configuration (`application.yml`)

```yaml
micewriter:
  socket-path: /var/run/app/iceberg.sock   # must match SOCKET_PATH in the engine
  base-package: com.example.events         # narrows @IcebergEntity classpath scan
  connect-timeout-ms: 5000
  ack-timeout-ms: 5000
  enabled: true                            # set false to disable the SDK entirely
```

Schema registration happens automatically on `ContextRefreshedEvent`. No code changes needed.

---

## Dropwizard

### Dependency

```xml
<dependency>
    <groupId>com.micewriter</groupId>
    <artifactId>micewriter-sdk-java-dropwizard</artifactId>
    <version>0.2.0</version>
</dependency>
```

### Add config to your Configuration class

```java
public class AppConfig extends Configuration {
    @Valid @NotNull
    private MicewriterConfig micewriter = new MicewriterConfig();

    @JsonProperty("micewriter")
    public MicewriterConfig getMicewriter() { return micewriter; }
}
```

### Register the bundle

```java
public class App extends Application<AppConfig> {

    private final MicewriterBundle<AppConfig> micewriter =
        new MicewriterBundle<>(AppConfig::getMicewriter)
            .entities(TelemetryEvent.class);   // explicit entity list

    @Override
    public void initialize(Bootstrap<AppConfig> bootstrap) {
        bootstrap.addBundle(micewriter);
    }

    @Override
    public void run(AppConfig config, Environment env) {
        env.jersey().register(new EventResource(micewriter.getTemplate()));
    }
}
```

### Configuration (`config.yml`)

```yaml
micewriter:
  socketPath: /var/run/app/iceberg.sock
  connectTimeoutMs: 5000
  ackTimeoutMs: 5000
```

Schema registration and UDS lifecycle (connect on start, close on stop) are handled
by the bundle automatically.

---

## Wire protocol

Both starters produce identical IPC frames — the engine sees no difference.

```
[4-byte big-endian length][1-byte msg type][payload bytes]
```

| Message | Type byte | Payload encoding |
|---|---|---|
| `REGISTER_SCHEMA` | `0x01` | JSON `{ table, namespace, fields }` |
| `INGEST_RECORD` | `0x02` | `[table_name_len u16][table_name UTF-8][schema_id i32=0][Arrow IPC stream]` |
| ACK (engine → SDK) | — | JSON `{ status: "ok"\|"error", msg? }` |

See [system-overview.md](../micewriter-hub/docs/system-overview.md) for the full protocol spec.

## Type mapping

| Java type | Iceberg type | Arrow type | Notes |
|---|---|---|---|
| `String` | `string` | `Utf8` | |
| `long` / `Long` | `long` | `Int64` | |
| `int` / `Integer` | `int` | `Int32` | |
| `double` / `Double` | `double` | `Float64` | |
| `float` / `Float` | `float` | `Float32` | |
| `boolean` / `Boolean` | `boolean` | `Bool` | |
| `Instant` / `OffsetDateTime` / `ZonedDateTime` | `timestamptz` | `Timestamp(µs, UTC)` | microseconds since epoch |
| `LocalDateTime` | `timestamp` | `Timestamp(µs)` | microseconds since epoch, no tz |
| `LocalDate` | `date` | `Date32` | days since epoch |
| `byte[]` | `binary` | `Binary` | |

## Notes

- **Linux only:** `UdsConnection` uses Netty Epoll. Running locally on Windows or macOS
  requires WSL or a Docker container.
- **Append-only:** Row-level updates and deletes are not supported by this SDK.
- `IcebergStreamTemplate.send()` blocks until the engine ACKs the RocksDB write — this is a
  local memory operation and completes in microseconds under normal conditions.

## Building

```powershell
docker run --rm `
  -v "${PWD}:/project" -v "$env:USERPROFILE\.m2:/root/.m2" `
  -w /project maven:3.9-eclipse-temurin-25 `
  mvn clean install
```
