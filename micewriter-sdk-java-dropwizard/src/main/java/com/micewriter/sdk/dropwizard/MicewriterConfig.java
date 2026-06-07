package com.micewriter.sdk.dropwizard;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;

/**
 * Dropwizard configuration fragment for the mIceWriter SDK.
 *
 * <p>Add a field of this type to your application's {@code Configuration} subclass
 * and bind it from {@code config.yml}:
 *
 * <pre>{@code
 * public class AppConfig extends Configuration {
 *     @Valid @NotNull
 *     private MicewriterConfig micewriter = new MicewriterConfig();
 *
 *     @JsonProperty("micewriter")
 *     public MicewriterConfig getMicewriter() { return micewriter; }
 * }
 * }</pre>
 *
 * <pre>{@code
 * # config.yml
 * micewriter:
 *   socketPath: /var/run/app/iceberg.sock
 *   connectTimeoutMs: 5000
 *   ackTimeoutMs: 5000
 * }</pre>
 */
public class MicewriterConfig {

    @NotEmpty
    @JsonProperty
    private String socketPath = "/var/run/app/iceberg.sock";

    @Min(1)
    @JsonProperty
    private int connectTimeoutMs = 5_000;

    @Min(1)
    @JsonProperty
    private int ackTimeoutMs = 5_000;

    @Min(1)
    @JsonProperty
    private long maxInFlightBytes = 8L * 1024 * 1024;

    public String getSocketPath() { return socketPath; }
    public void setSocketPath(String socketPath) { this.socketPath = socketPath; }

    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public void setConnectTimeoutMs(int connectTimeoutMs) { this.connectTimeoutMs = connectTimeoutMs; }

    public int getAckTimeoutMs() { return ackTimeoutMs; }
    public void setAckTimeoutMs(int ackTimeoutMs) { this.ackTimeoutMs = ackTimeoutMs; }

    public long getMaxInFlightBytes() { return maxInFlightBytes; }
    public void setMaxInFlightBytes(long maxInFlightBytes) { this.maxInFlightBytes = maxInFlightBytes; }
}
