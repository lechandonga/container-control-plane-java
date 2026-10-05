package com.github.lechandonga.containercontrolplanejava.tests;

import com.github.lechandonga.containercontrolplanejava.model.InstanceRecord;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadPhase;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;
import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;
import com.github.lechandonga.containercontrolplanejava.tests.support.BusinessTestLogger;
import com.github.lechandonga.containercontrolplanejava.tests.support.BusinessUnderTest;
import com.github.lechandonga.containercontrolplanejava.tests.support.Scenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务域：节点故障检测、fence 与在健康节点上的自愈重建（含容量约束与防脑裂）。
 */
@BusinessUnderTest("节点故障自愈：失联 fence、剩余健康节点重建、不超容量、防双实例脑裂")
@ExtendWith(BusinessTestLogger.class)
class NodeFailureTest extends ControlPlaneTestBase {

    @Test
    @Scenario("节点失联后其上实例被 fence 为 LOST，并在剩余健康节点重建到期望副本数")
    void rebuildsOnRemainingHealthyNodes() {
        SimulatedNode a = node(3);
        SimulatedNode b = node(3);
        SimulatedNode c = node(3);
        service.createWorkload("wl-nf-1", WorkloadSpec.of(6, "app:v1"));
        tickUntilConverged("wl-nf-1", 40);
        long onA = store.instancesOf("wl-nf-1").stream()
                .filter(r -> r.getNodeUid().equals(a.getInfo().getUid())).count();
        assertTrue(onA > 0, "前置条件：故障节点上确有实例");

        service.partitionNode(a.getInfo().getUid());
        advanceSeconds(120); // 超过心跳超时
        tick(5);
        assertEventually("wl-nf-1", 60,
                wl -> wl.getPhase() == WorkloadPhase.RUNNING && wl.getReadyReplicas() == 6,
                "节点故障后必须在剩余节点重建并恢复 6 个可用实例");

        List<InstanceRecord> remaining = store.instancesOf("wl-nf-1");
        assertEquals(6, remaining.size(), "重建不得改变副本总数");
        assertTrue(remaining.stream().noneMatch(r -> r.getNodeUid().equals(a.getInfo().getUid())),
                "失联节点上不得保留有效实例记录");
        Set<String> ids = new HashSet<>(remaining.stream().map(InstanceRecord::getId).toList());
        assertEquals(6, ids.size(), "实例 id 不得重复");
    }

    @Test
    @Scenario("故障重建严格受容量约束：剩余容量不足时不超额调度，并给出明确原因")
    void rebuildRespectsCapacityAndFailsWithReason() {
        SimulatedNode a = node(2);
        SimulatedNode b = node(2);
        // 4 个实例恰好装满 2 个节点
        service.createWorkload("wl-nf-2", WorkloadSpec.of(4, "app:v1"));
        tickUntilConverged("wl-nf-2", 30);

        service.partitionNode(a.getInfo().getUid());
        advanceSeconds(120);
        tick(20);

        // b 容量只有 2，无法承载 a 上的 2 个实例
        int runningOnB = (int) store.instancesOf("wl-nf-2").stream()
                .filter(r -> r.getNodeUid().equals(b.getInfo().getUid()))
                .count();
        assertTrue(runningOnB <= 2, "绝不能超出健康节点容量，实际=" + runningOnB);
        assertEquals(WorkloadPhase.FAILED, store.get("wl-nf-2").getPhase(),
                "容量不足时应明确告停而非无限重试");
        String conditions = store.get("wl-nf-2").getConditions().toString();
        assertTrue(conditions.contains("Unschedulable") || conditions.contains("容量"),
                "必须给出容量相关的可解释原因: " + conditions);

        // 新节点扩容加入后，下一轮调谐自动恢复
        node(2);
        tickUntilConverged("wl-nf-2", 30);
        assertEquals(4, countAvailable("wl-nf-2"));
    }

    @Test
    @Scenario("失联节点恢复后残留容器被清空，重建实例继续运行，不出现新旧双实例脑裂")
    void recoveredNodeIsWipedNoSplitBrain() {
        SimulatedNode a = node(3);
        SimulatedNode b = node(3);
        service.createWorkload("wl-nf-3", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-nf-3", 20);
        String failedNodeUid = a.getInfo().getUid();

        service.partitionNode(failedNodeUid);
        advanceSeconds(120);
        tickUntilConverged("wl-nf-3", 60);
        assertEquals(3, store.instancesOf("wl-nf-3").stream()
                .filter(r -> !r.getNodeUid().equals(failedNodeUid)).count());

        // 分区期间 a 的本地容器仍然“活着”（模拟机器未宕机）；恢复后必须被控制面 wipe
        service.recoverNode(failedNodeUid);
        tick(5);

        assertEquals(0, a.runningCount(), "恢复节点上的残留容器必须清空，防止脑裂");
        long totalDistinctIds = store.instancesOf("wl-nf-3").stream()
                .map(InstanceRecord::getId).distinct().count();
        assertEquals(totalDistinctIds, store.instancesOf("wl-nf-3").size(),
                "同一工作负载不得出现重复实例 id");
        assertFalse(store.instancesOf("wl-nf-3").stream()
                        .anyMatch(r -> r.getNodeUid().equals(failedNodeUid)),
                "恢复的故障节点在重新调度前不应承载旧记录");
    }
}
