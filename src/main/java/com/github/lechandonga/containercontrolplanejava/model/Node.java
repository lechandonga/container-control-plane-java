package com.github.lechandonga.containercontrolplanejava.model;

/**
 * 模拟节点。容量表示该节点最多承载多少个实例（所有工作负载共享）。
 */
public record Node(
        String id,
        int capacity,
        NodeStatus status,
        /** 失联时刻的模拟刻度，-1 表示健康。 */
        long lostTick
) {
    public Node withStatus(NodeStatus newStatus, long tick) {
        return new Node(id, capacity, newStatus, newStatus == NodeStatus.NOT_READY ? tick : -1);
    }
}
