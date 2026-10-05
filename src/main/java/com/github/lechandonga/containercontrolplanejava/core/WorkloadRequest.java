package com.github.lechandonga.containercontrolplanejava.core;

/**
 * 提交/更新工作负载期望状态的请求。
 * resourceVersion 为 null 表示强制覆盖；否则做乐观锁校验。
 */
public record WorkloadRequest(
        String id,
        String image,
        String version,
        int replicas,
        int minAvailable,
        long startTimeoutTicks,
        long rolloutTimeoutTicks,
        Long resourceVersion
) {
}
