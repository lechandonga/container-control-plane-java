package com.github.lechandonga.containercontrolplanejava.exception;

import org.springframework.http.HttpStatus;

/**
 * 带 HTTP 状态码与机器可读 reason 的业务异常。
 */
public class ApiException extends RuntimeException {
    private final HttpStatus status;
    private final String reason;

    public ApiException(HttpStatus status, String reason, String message) {
        super(message);
        this.status = status;
        this.reason = reason;
    }

    public static ApiException notFound(String message) {
        return new ApiException(HttpStatus.NOT_FOUND, "NOT_FOUND", message);
    }

    public static ApiException conflict(String message) {
        return new ApiException(HttpStatus.CONFLICT, "RESOURCE_VERSION_CONFLICT", message);
    }

    public static ApiException badRequest(String reason, String message) {
        return new ApiException(HttpStatus.BAD_REQUEST, reason, message);
    }

    public HttpStatus getStatus() { return status; }
    public String getReason() { return reason; }
}
