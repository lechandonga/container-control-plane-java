package com.github.lechandonga.containercontrolplanejava.scheduler;

import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;

/**
 * 调度结果：成功则携带目标节点，失败携带确定性原因码。
 */
public record SchedulingDecision(boolean schedulable, SimulatedNode node, String reason) {

    public static SchedulingDecision placed(SimulatedNode node) {
        return new SchedulingDecision(true, node, null);
    }

    public static SchedulingDecision noHealthyNode() {
        return new SchedulingDecision(false, null, "NoHealthyNode: 集群中没有健康节点可承载实例");
    }

    public static SchedulingDecision insufficientCapacity(int required, int available) {
        return new SchedulingDecision(false, null,
                "InsufficientCapacity: 剩余健康节点容量不足，需要 " + required + " 个槽位，仅剩 " + available + " 个");
    }
}
