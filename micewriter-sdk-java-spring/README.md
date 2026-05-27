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

    public void record(TelemetryEvent event) {
        icebergTemplate.send(event);
    }
}
```

### 3. Configure

```yaml
# application.yml
micewriter:
  socket-path: /var/run/app/iceberg.sock
  base-package: com.example.events   # narrows @IcebergEntity classpath scan; omit to scan all
  connect-timeout-ms: 5000
  ack-timeout-ms: 5000
  enabled: true
```

## How it works

On `ContextRefreshedEvent`, `SpringSchemaRegistrar` classpath-scans for `@IcebergEntity`
classes and calls `SchemaRegistrar.register()` which sends a `REGISTER_SCHEMA` (0x01)
JSON message to the engine for each class. After that, every `send()` call serialises the
POJO as an Arrow IPC RecordBatch and sends it as `INGEST_RECORD` (0x02).
