# micewriter-sdk-java

Java SDK for the [mIceWriter telemetry ingestion ecosystem](../micewriter-hub/README.md).

## Branches and Versioning

This SDK maintains two active, parallel release lines:

- **`main` branch (2.x.x)**: Uses gRPC over HTTP/2 to stream payloads to central per-table `micewriter-engine` pipelines.
- **`v1` branch (1.x.x)**: Uses Unix Domain Sockets (UDS) and Apache Arrow IPC to stream payloads to a per-pod `micewriter-engine` sidecar.

> [!TIP]
> **Which one should I use?**
> Use `2.x.x` for all new projects. The `1.x.x` line is maintained for legacy architectures where sidecar deployment and UDS transports are already established.

## Modules

We provide a Bill of Materials (BOM) to manage dependency versions. All modules are released together at the exact same version.

| Module | Artifact | Purpose |
|---|---|---|
| **BOM** | `micewriter-sdk-bom` | Import this into your `<dependencyManagement>` to align versions |
| **api** | `micewriter-sdk-java-api` | Contains *only* the `@IcebergEntity` and `@IcebergId` annotations (Zero dependencies). Use this in shared domain libraries! |
| **core** | `micewriter-sdk-java-core` | Framework-agnostic client engine (transitively included by the starters) |
| **spring** | `micewriter-sdk-java-spring` | Auto-configuration starter for Spring Boot applications |
| **dropwizard** | `micewriter-sdk-java-dropwizard` | Bundle for Dropwizard applications |

---

## Spring Boot

### 1. Import the BOM and Dependency

In your `pom.xml`, import the BOM so you don't need to specify versions for the starter:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.micewriter</groupId>
            <artifactId>micewriter-sdk-bom</artifactId>
            <version>2.0.0</version> <!-- Use 1.x.x if using the v1 UDS architecture -->
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>com.micewriter</groupId>
        <artifactId>micewriter-sdk-java-spring</artifactId>
    </dependency>
</dependencies>
```

### 2. Annotate your entity

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

### 3. Inject and send

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

### 4. Configuration (`application.yml`)

*(Note: If using v1, you will configure `socket-path` instead of `resolver`)*

```yaml
micewriter:
  resolver: "engine-{table}.micewriter.svc:9090" # v2 gRPC endpoint template
  base-package: com.example.events               # narrows @IcebergEntity classpath scan
  connect-timeout-ms: 5000
  ack-timeout-ms: 5000
  enabled: true                                  # set false to disable the SDK entirely
```

Schema registration happens automatically on `ContextRefreshedEvent`. No code changes needed.

---

## Dropwizard

### 1. Import the BOM and Dependency

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.micewriter</groupId>
            <artifactId>micewriter-sdk-bom</artifactId>
            <version>2.0.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>com.micewriter</groupId>
        <artifactId>micewriter-sdk-java-dropwizard</artifactId>
    </dependency>
</dependencies>
```

### 2. Add config to your Configuration class

```java
public class AppConfig extends Configuration {
    @Valid @NotNull
    private MicewriterConfig micewriter = new MicewriterConfig();

    @JsonProperty("micewriter")
    public MicewriterConfig getMicewriter() { return micewriter; }
}
```

### 3. Register the bundle

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

### 4. Configuration (`config.yml`)

```yaml
micewriter:
  resolver: "engine-{table}.micewriter.svc:9090"
  connectTimeoutMs: 5000
  ackTimeoutMs: 5000
```

Schema registration and lifecycle are handled by the bundle automatically.

---

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

## Building

```powershell
docker run --rm `
  -v "${PWD}:/project" -v "$env:USERPROFILE\.m2:/root/.m2" `
  -w /project maven:3.9-eclipse-temurin-25 `
  mvn clean install
```
