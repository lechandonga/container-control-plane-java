package com.github.lechandonga.containercontrolplanejava.model;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 一个被模拟的容器实例。
 *
 * @param id        全局唯一且稳定的实例 ID（workload-seq 形式，seq 单调递增，删除后不复用）
 * @param workloadId 所属工作负载
 * @param nodeId    承载节点
 * @param version   实例镜像版本
 * @param state     运行状态
 * @param bornTick  创建时的模拟时钟刻度，用于判断启动超时
 * @param failTick  进入 FAILED 的刻度（-1 表示从未失败）
 */
public record Instance(
        String id,
        String workloadId,
        String nodeId,
        String version,
        InstanceState state,
        long bornTick,
        long failTick
) {
    public Instance withState(InstanceState newState, long tick) {
        return new Instance(id, workloadId, nodeId, version, newState, bornTick,
                newState == InstanceState.FAILED ? tick : failTick);
    }

    public static String nextId(String workloadId, AtomicLong sequence) {
        return workloadId + "-" + sequence.incrementAndGet();
    }
}
