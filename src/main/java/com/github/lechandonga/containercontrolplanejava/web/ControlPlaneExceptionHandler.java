package com.github.lechandonga.containercontrolplanejava.web;

import com.github.lechandonga.containercontrolplanejava.event.EventLog;
import com.github.lechandonga.containercontrolplanejava.store.ConflictException;
import com.github.lechandonga.containercontrolplanejava.store.NotFoundException;
import com.github.lechandonga.containercontrolplanejava.web.dto.ErrorView;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 把并发冲突、资源不存在、非法参数映射成可区分的 HTTP 状态与结构化原因，
 * 调用方可以据此明确“被拒绝的原因”并重试，而不会遭遇静默覆盖。
 */
@RestControllerAdvice
public class ControlPlaneExceptionHandler {

    private final EventLog eventLog;

    public ControlPlaneExceptionHandler(EventLog eventLog) {
        this.eventLog = eventLog;
    }

    @ExceptionHandler(ConflictException.class)
    public ResponseEntity<ErrorView> conflict(ConflictException ex) {
        eventLog.record(EventLog.BUSINESS_CONFLICT, ex.getWorkloadUid(),
                "OptimisticLockRejected",
                "并发修改被拒绝: supplied=" + ex.getSuppliedResourceVersion()
                        + " current=" + ex.getCurrentResourceVersion());
        ErrorView view = new ErrorView("RESOURCE_VERSION_CONFLICT", ex.getMessage(),
                ex.getWorkloadUid(), ex.getSuppliedResourceVersion(),
                ex.getCurrentResourceVersion());
        return ResponseEntity.status(HttpStatus.CONFLICT).body(view);
    }

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<ErrorView> notFound(NotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorView("NOT_FOUND", ex.getMessage(), null, null, null));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorView> badRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest()
                .body(new ErrorView("INVALID_SPEC", ex.getMessage(), null, null, null));
    }
}
