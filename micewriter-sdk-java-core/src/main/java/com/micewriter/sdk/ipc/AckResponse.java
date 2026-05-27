package com.micewriter.sdk.ipc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Deserialized JSON ACK frame from the micewriter-engine.
 * Matches the Rust {@code AckResponse} struct: {@code { "status": "ok" | "error", "msg": "..." }}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AckResponse {

    private String status;
    private String msg;

    public AckResponse() {}

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getMsg() { return msg; }
    public void setMsg(String msg) { this.msg = msg; }

    public boolean isOk() { return "ok".equals(status); }
}
