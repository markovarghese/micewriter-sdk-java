# micewriter-sdk-java
> Part of the [mIceWriter Ingestion Ecosystem](../micewriter-hub/README.md)

Spring Boot Starter SDK. Drop it into any Spring Boot app and stream POJOs to the micewriter-engine sidecar over a Unix Domain Socket — no infrastructure code required.

## Usage

### 1. Add the dependency

```xml
<dependency>
    <groupId>com.micewriter</groupId>
    <artifactId>micewriter-sdk-java</artifactId>
    <version>0.1.0</version>
</dependency>
```

### 2. Annotate your POJO

```java
@IcebergEntity(table = "telemetry_events", namespace = {"micewriter"})
public class TelemetryEvent {

    @IcebergId
    private String id;

    private Instant ts;
    private String payload;

    // getters / constructors ...
}
```

### 3. Inject and send

```java
@Service
public class EventService {

    @Autowired
    private IcebergStreamTemplate icebergTemplate;

    public void record(TelemetryEvent event) {
        icebergTemplate.send(event);   // ACKs within microseconds
    }
}
```

The SDK sends `REGISTER_SCHEMA` for every `@IcebergEntity` class automatically on startup. No further configuration is needed if you are running inside a pod managed by the `micewriter-k8s-injector` webhook.

## Configuration

```yaml
micewriter:
  enabled: true                            # set false to disable entirely
  socket-path: /var/run/app/iceberg.sock   # must match SOCKET_PATH in the engine
  connect-timeout-ms: 5000
  ack-timeout-ms: 5000
  base-package: com.example.myapp          # narrows @IcebergEntity classpath scan
```

## IPC Protocol

| Step | Description |
|------|-------------|
| Frame | 4-byte big-endian length prefix + payload |
| `REGISTER_SCHEMA` (0x01) | JSON `{ table, namespace, fields[{name, type, required}] }` |
| `INGEST_RECORD` (0x02) | JSON `{ table, fields: [["name", value], ...] }` |
| ACK | JSON `{ status: "ok" \| "error", msg?: "..." }` |

## Type Mapping

| Java type | Iceberg type |
|-----------|-------------|
| `String` | `string` |
| `long` / `Long` | `long` |
| `int` / `Integer` | `int` |
| `double` / `Double` | `double` |
| `boolean` / `Boolean` | `boolean` |
| `Instant` / `OffsetDateTime` / `ZonedDateTime` | `timestamptz` (microseconds since epoch) |
| `LocalDate` | `date` (days since epoch) |
| `byte[]` | `binary` |

## Building

```bash
mvn clean install          # builds and installs to local Maven repo
mvn clean install -DskipTests
```

## Notes

- **Linux only:** `UdsConnection` uses Netty Epoll + `EpollDomainSocketChannel`. Running locally on Windows or macOS requires WSL or a Docker container.
- **Schema registration** happens once on `ContextRefreshedEvent`. Restarting the engine while the app is running requires an app restart to re-register schemas.
- `IcebergStreamTemplate.send()` blocks until the engine ACKs the RocksDB write — this is a local memory operation and completes in microseconds under normal conditions.
