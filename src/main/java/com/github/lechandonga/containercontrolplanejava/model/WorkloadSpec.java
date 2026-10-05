package com.github.lechandonga.containercontrolplanejava.model;

/**
 * 工作负载的期望状态（声明式）。
 *
 * @param replicas               期望实例数
 * @param image                  期望版本（镜像:标签）
 * @param maxSurge               滚动更新时允许超过 replicas 的峰值实例数
 * @param minAvailable           滚动更新全过程可用实例数下限
 * @param maxStartRetries        单次更新内新版本启动失败的最大尝试次数
 * @param rolloutDeadlineSeconds 滚动更新超时时间（秒），从首个新版本实例创建时起算
 */
public record WorkloadSpec(int replicas,
                           String image,
                           int maxSurge,
                           int minAvailable,
                           int maxStartRetries,
                           long rolloutDeadlineSeconds) {

    public static WorkloadSpec of(int replicas, String image) {
        return new WorkloadSpec(replicas, image, 1, Math.max(0, replicas), 5, 120);
    }

    public WorkloadSpec validate() {
        if (replicas < 0) {
            throw new IllegalArgumentException("replicas 不能为负数: " + replicas);
        }
        if (image == null || image.isBlank()) {
            throw new IllegalArgumentException("image 不能为空");
        }
        if (maxSurge < 0) {
            throw new IllegalArgumentException("maxSurge 不能为负数");
        }
        if (minAvailable < 0 || minAvailable > replicas + maxSurge) {
            throw new IllegalArgumentException("minAvailable 必须落在 [0, replicas+maxSurge] 区间内");
        }
        if (maxStartRetries < 0 || rolloutDeadlineSeconds <= 0) {
            throw new IllegalArgumentException("maxStartRetries/rolloutDeadlineSeconds 取值非法");
        }
        return this;
    }
}
