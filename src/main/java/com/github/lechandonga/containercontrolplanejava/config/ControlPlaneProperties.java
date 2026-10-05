package com.github.lechandonga.containercontrolplanejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 控制面可调参数（全部有默认值，单机零依赖即可运行）。
 *
 * @param reconcileIntervalMs      控制循环调谐间隔
 * @param heartbeatIntervalMs      模拟节点心跳间隔
 * @param heartbeatTimeoutSeconds  心跳超时，超时即判定节点失联并 fence
 * @param maxActionsPerReconcile   单次调谐最大变更动作数（默认 1，保证滚动更新逐步可解释）
 */
@ConfigurationProperties(prefix = "controlplane")
public record ControlPlaneProperties(long reconcileIntervalMs,
                                     long heartbeatIntervalMs,
                                     long heartbeatTimeoutSeconds,
                                     int maxActionsPerReconcile) {
    public ControlPlaneProperties {
        if (reconcileIntervalMs <= 0) {
            reconcileIntervalMs = 100;
        }
        if (heartbeatIntervalMs <= 0) {
            heartbeatIntervalMs = 50;
        }
        if (heartbeatTimeoutSeconds <= 0) {
            heartbeatTimeoutSeconds = 8;
        }
        if (maxActionsPerReconcile <= 0) {
            maxActionsPerReconcile = 1;
        }
    }

    public static ControlPlaneProperties defaults() {
        return new ControlPlaneProperties(100, 50, 8, 1);
    }
}
