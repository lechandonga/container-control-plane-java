package com.github.lechandonga.containercontrolplanejava.model;

/**
 * 调谐事件（只追加，用于给出“可解释的结果”）。
 */
public record Event(
        long tick,
        String workloadId,
        String type,
        String message
) {
}
