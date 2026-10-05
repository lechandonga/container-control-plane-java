package com.github.lechandonga.containercontrolplanejava.model;

import java.time.Instant;

/**
 * 可解释的状态条目：每个阶段/异常都附带确定性的原因码与说明。
 */
public record Condition(String type,
                        boolean status,
                        String reason,
                        String message,
                        long generation,
                        Instant lastTransitionTime) {

    public static Condition of(String type, boolean status, String reason, String message, long generation) {
        return new Condition(type, status, reason, message, generation, Instant.now());
    }
}
