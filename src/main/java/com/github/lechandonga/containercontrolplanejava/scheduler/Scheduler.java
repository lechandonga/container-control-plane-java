package com.github.lechandonga.containercontrolplanejava.scheduler;

import com.github.lechandonga.containercontrolplanejava.model.InstanceRecord;
import com.github.lechandonga.containercontrolplanejava.model.InstanceState;
import com.github.lechandonga.containercontrolplanejava.model.NodeState;
import com.github.lechandonga.containercontrolplanejava.node.NodeRegistry;
import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;
import com.github.lechandonga.containercontrolplanejava.store.WorkloadStore;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * 容量感知装箱调度器。
 *
 * <p>容量规则：
 * <ul>
 *   <li>只有 HEALTHY 且未被分区的节点可承载新实例。</li>
 *   <li>节点已占用槽位 = 该节点上非 LOST 实例记录数（LOST 实例在重建前会先摘除，不占旧槽）。</li>
 *   <li>选节点策略：剩余容量最多者优先（most-available），平局按 uid 字典序，保证确定性。</li>
 *   <li>无健康节点或全部满载：返回带原因的不可调度决策，调谐循环据此告停而不是无限重试。</li>
 * </ul>
 */
@Component
public class Scheduler {

    private final NodeRegistry nodeRegistry;
    private final WorkloadStore store;

    public Scheduler(NodeRegistry nodeRegistry, WorkloadStore store) {
        this.nodeRegistry = nodeRegistry;
        this.store = store;
    }

    /**
     * 各节点当前已占用槽位。注意：失联节点上的 LOST 记录仍计入其节点槽位——
     * 节点可能随时恢复，恢复后其上的旧容器会被 wipe；在控制面视角中该槽位处于
     * “已 fence、待清理”状态，不能再被调度，直到实例在健康节点完成重建并摘除旧记录。
     */
    private Map<String, Integer> currentLoadByNode() {
        Map<String, Integer> load = new HashMap<>();
        for (InstanceRecord record : store.allInstances().values()) {
            load.merge(record.getNodeUid(), 1, Integer::sum);
        }
        return load;
    }

    public SchedulingDecision schedule() {
        Map<String, Integer> load = currentLoadByNode();

        boolean anyHealthy = false;
        int freeCapacity = 0;
        SimulatedNode best = null;
        int bestFree = -1;

        for (SimulatedNode node : nodeRegistry.all()) {
            if (node.getInfo().getState() != NodeState.HEALTHY || node.isPartitioned()) {
                continue;
            }
            anyHealthy = true;
            int used = load.getOrDefault(node.getInfo().getUid(), 0);
            int free = node.getInfo().getCapacity() - used;
            freeCapacity += Math.max(free, 0);
            if (free > 0 && (best == null || free > bestFree
                    || (free == bestFree && node.getInfo().getUid().compareTo(best.getInfo().getUid()) < 0))) {
                best = node;
                bestFree = free;
            }
        }

        if (!anyHealthy) {
            return SchedulingDecision.noHealthyNode();
        }
        if (best == null) {
            return SchedulingDecision.insufficientCapacity(1, freeCapacity);
        }
        return SchedulingDecision.placed(best);
    }

    /** 供状态查询：集群剩余健康容量。 */
    public int totalFreeCapacity() {
        Map<String, Integer> load = currentLoadByNode();
        return nodeRegistry.all().stream()
                .filter(n -> n.getInfo().getState() == NodeState.HEALTHY && !n.isPartitioned())
                .mapToInt(n -> Math.max(0, n.getInfo().getCapacity()
                        - load.getOrDefault(n.getInfo().getUid(), 0)))
                .sum();
    }

    /** 稳定的节点展示顺序。 */
    public Comparator<SimulatedNode> displayOrder() {
        return Comparator.comparing(n -> n.getInfo().getUid());
    }
}
