package com.micewriter.sdk.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "micewriter")
public class IcebergProperties {

    /** Set to false to disable the SDK entirely (no UDS connection, no schema registration). */
    private boolean enabled = true;

    /** Path to the Unix Domain Socket exposed by the micewriter-engine sidecar. */
    private String socketPath = "/var/run/app/iceberg.sock";

    /** Initial connection timeout in milliseconds. */
    private int connectTimeoutMs = 5_000;

    /**
     * Base package for {@code @IcebergEntity} classpath scanning.
     * Defaults to the root package (scan everything). Narrow this in large apps.
     */
    private String basePackage = "";

    /** How long to wait for an ACK from the engine before throwing (ms). */
    private int ackTimeoutMs = 5_000;

    /** Max bytes of un-ACKed data-path sends held in memory (pipelining/backpressure window). */
    private long maxInFlightBytes = 8L * 1024 * 1024;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getSocketPath() { return socketPath; }
    public void setSocketPath(String socketPath) { this.socketPath = socketPath; }

    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public void setConnectTimeoutMs(int connectTimeoutMs) { this.connectTimeoutMs = connectTimeoutMs; }

    public String getBasePackage() { return basePackage; }
    public void setBasePackage(String basePackage) { this.basePackage = basePackage; }

    public int getAckTimeoutMs() { return ackTimeoutMs; }
    public void setAckTimeoutMs(int ackTimeoutMs) { this.ackTimeoutMs = ackTimeoutMs; }

    public long getMaxInFlightBytes() { return maxInFlightBytes; }
    public void setMaxInFlightBytes(long maxInFlightBytes) { this.maxInFlightBytes = maxInFlightBytes; }
}
