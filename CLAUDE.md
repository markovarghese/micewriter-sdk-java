# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```powershell
# Run via Docker (mvn not installed on host)
docker run --rm `
  -v "${PWD}:/project" -v "$env:USERPROFILE\.m2:/root/.m2" `
  -w /project maven:3.9-eclipse-temurin-25 `
  mvn clean install            # build all modules and install to local Maven repo

# Single module
  mvn clean install -pl micewriter-sdk-java-core -DskipTests
  mvn test -pl micewriter-sdk-java-spring -Dtest=ClassName
```

## Module Structure

This is a **Maven multi-module project** — three jars released at the same version:

```
micewriter-sdk-java/                       ← parent pom (no source)
├── micewriter-sdk-java-core/              ← framework-agnostic core
├── micewriter-sdk-java-spring/            ← Spring Boot starter
└── micewriter-sdk-java-dropwizard/        ← Dropwizard bundle
```

### `micewriter-sdk-java-core`
Zero Spring/Dropwizard deps. Contains:
- `annotation/` — `@IcebergEntity`, `@IcebergId`
- `ipc/` — `UdsConnection` (Netty Epoll), `AckResponse`
- `schema/` — `PojoInspector` (Arrow type mapping + IPC serialisation), `SchemaRegistrar`
- `template/` — `IcebergStreamTemplate` (public send API)

`SchemaRegistrar.register(Class<?>...)` is the explicit, framework-agnostic registration path.
`SchemaRegistrar.prependTypeByte()` is the shared IPC framing utility.

### `micewriter-sdk-java-spring`
Depends on core + `spring-boot-autoconfigure`. Contains:
- `config/IcebergAutoConfiguration` — creates beans, activated via `AutoConfiguration.imports`
- `config/IcebergProperties` — `@ConfigurationProperties(prefix="micewriter")`
- `spring/SpringSchemaRegistrar` — `ApplicationListener<ContextRefreshedEvent>` that classpath-scans for `@IcebergEntity` and delegates to core `SchemaRegistrar`

### `micewriter-sdk-java-dropwizard`
Depends on core + `dropwizard-core:4.0.7`. Contains:
- `dropwizard/MicewriterConfig` — plain POJO for `config.yml` binding
- `dropwizard/MicewriterBundle` — `ConfiguredBundle<T>` with `Managed` lifecycle and `getTemplate()` accessor

## Startup Flows

### Spring Boot
1. `IcebergAutoConfiguration` creates `UdsConnection`, `IcebergStreamTemplate`, `SchemaRegistrar`, `SpringSchemaRegistrar` beans.
2. On `ContextRefreshedEvent`, `SpringSchemaRegistrar` classpath-scans for `@IcebergEntity` (bounded by `micewriter.base-package`) and calls `registrar.register(discoveredClasses)`.
3. `register()` sends one `REGISTER_SCHEMA` (0x01) JSON message per class. `AtomicBoolean` prevents double-registration in multi-context apps.
4. App calls `IcebergStreamTemplate.send(entity)` → Arrow IPC bytes → `INGEST_RECORD` (0x02) → engine ACK.

### Dropwizard
1. App calls `bootstrap.addBundle(new MicewriterBundle<>(AppConfig::getMicewriter).entities(...))`.
2. On server start, `Managed.start()` calls `registrar.register(entityClasses)` with the explicitly declared entity classes.
3. On server stop, `Managed.stop()` closes the Arrow allocator and Netty channel.
4. App retrieves `IcebergStreamTemplate` via `bundle.getTemplate()` from `Application.run()`.

## Wire Protocol

```
[4-byte big-endian length][1-byte msg type][payload bytes]
```

| Message | Type | Payload |
|---|---|---|
| `REGISTER_SCHEMA` | `0x01` | JSON `{ table, namespace, fields[{name, type, required}] }` |
| `INGEST_RECORD` | `0x02` | `[table_name_len u16][table_name UTF-8][schema_id i32=0][Arrow IPC stream]` |
| ACK (inbound) | — | JSON `{ status: "ok"\|"error", msg? }` — framed the same way |

`UdsConnection` writes the 4-byte length prefix. `SchemaRegistrar.prependTypeByte()` prepends the type discriminant. ACK frames are decoded by `LengthFieldBasedFrameDecoder` in `UdsConnection`.

## Arrow Serialisation

`PojoInspector.buildArrowSchema(Class<?>)` builds and caches an Arrow `Schema` per POJO class.
`PojoInspector.toArrowIpcStream(entity, schema, allocator)` serialises one row as an Arrow IPC stream (schema message + RecordBatch + EOS). The `BufferAllocator` is owned by `IcebergStreamTemplate` (one per template instance, closed via `Closeable`).

Iceberg ↔ Arrow type mapping:
- `string` / `Utf8`, `long` / `Int64`, `int` / `Int32`, `double` / `Float64`, `float` / `Float32`, `boolean` / `Bool`
- `timestamptz` / `Timestamp(MICROSECOND, "UTC")` — microseconds since epoch
- `timestamp` / `Timestamp(MICROSECOND, null)` — microseconds since epoch (no tz)
- `date` / `Date(DAY)` — days since epoch
- `binary` / `Binary`

## Key Configuration

### Spring Boot (`micewriter.*`)

| Property | Default | Purpose |
|---|---|---|
| `enabled` | `true` | Set `false` to disable entirely |
| `socket-path` | `/var/run/app/iceberg.sock` | Must match `SOCKET_PATH` in the engine |
| `base-package` | `""` (scan all) | Narrow `@IcebergEntity` classpath scan |
| `connect-timeout-ms` | `5000` | UDS connect timeout |
| `ack-timeout-ms` | `5000` | Per-message ACK timeout |

### Dropwizard (`MicewriterConfig`)

| Field | Default | Purpose |
|---|---|---|
| `socketPath` | `/var/run/app/iceberg.sock` | UDS path |
| `connectTimeoutMs` | `5000` | UDS connect timeout |
| `ackTimeoutMs` | `5000` | Per-message ACK timeout |

## Linux-only Constraint

`UdsConnection` uses Netty Epoll (`EpollDomainSocketChannel`). Running locally on Windows or macOS requires WSL or a Linux container. There is no NIO fallback.
