package com.github.lechandonga.containercontrolplanejava.tests;

import com.github.lechandonga.containercontrolplanejava.model.Instance;
import com.github.lechandonga.containercontrolplanejava.model.Phase;
import com.github.lechandonga.containercontrolplanejava.support.BusinessLoggingExtension;
import com.github.lechandonga.containercontrolplanejava.support.CcpTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务二：节点故障自愈与容量约束。
 */
@DisplayName("业务2-节点故障自愈与容量")
@ExtendWith(BusinessLoggingExtension.class)
class NodeFailureTest {

    @Test
    @DisplayName("用例2.1 节点失联后实例在剩余健康节点重建并重新收敛")
    void lostNodeInstancesAreRebuiltElsewhere() {
        var t = CcpTestSupport.create();
        t.node("n1", 2);
        t.node("n2", 2);
        t.node("n3", 2);
        t.deploy("app", "img", "v1", 4, 1);
        t.runUntilStable("app", 5);

        long onN2Before = t.instancesOn("app", "n2");
        assertTrue(onN2Before >= 1, "调度应当分布到 n2");

        t.cp.markNodeLost("n2");
        // 失联后该节点上的实例立即被视为丢失
        assertEquals(0, t.instancesOn("app", "n2"), "失联节点上不得残留实例");

        var w = t.runUntilStable("app", 5);
        assertEquals(Phase.RUNNING, w.getPhase());
        assertEquals(4, t.runningCount("app"), "必须在健康节点重建到 4 个就绪实例");
        t.assertCapacityNotExceeded();
        assertTrue(t.hasEvent("app", "InstanceEvicted"::equals), "应记录实例驱逐事件");
        System.out.println("    n2 失联，驱逐 " + onN2Before + " 个实例并在 n1/n3 重建，恢复 4/4");
    }

    @Test
    @DisplayName("用例2.2 剩余健康节点容量不足时绝不超卖：保持缺口并给出可解释原因")
    void noOversubscriptionWhenCapacityInsufficient() {
        var t = CcpTestSupport.create();
        t.node("n1", 5);
        t.node("n2", 1);
        t.deploy("app", "img", "v1", 4, 1);
        t.runUntilStable("app", 5);

        // n1 承载 3 个、n2 承载 1 个；n1 失联后只剩 n2 的 1 个槽位。
        t.cp.markNodeLost("n1");
        t.cp.tick(3);

        var w = t.cp.snapshotView().getWorkloads().get("app");
        assertEquals(1, w.getInstances().size(), "容量只有 1，最多重建 1 个");
        t.assertCapacityNotExceeded();
        assertTrue(w.getReason().contains("容量不足"), "必须说明容量不足原因: " + w.getReason());
        System.out.println("    健康容量=1，实际=1（未超卖），原因: " + w.getReason());

        // 节点恢复/扩容后自动补齐
        t.cp.markNodeReady("n1");
        t.runUntilStable("app", 5);
        assertEquals(4, t.runningCount("app"), "容量恢复后应自愈收敛到 4");
        System.out.println("    n1 恢复后自动补齐到 4/4");
    }

    @Test
    @DisplayName("用例2.3 失联节点恢复后旧实例不会复活，只能通过调谐重新分配")
    void recoveredNodeStartsEmpty() {
        var t = CcpTestSupport.create();
        t.node("n1", 3);
        t.node("n2", 3);
        t.deploy("app", "img", "v1", 3, 1);
        t.runUntilStable("app", 5);

        t.cp.markNodeLost("n1");
        t.cp.tick(3);
        t.cp.markNodeReady("n1");
        t.cp.tick(3);

        List<Instance> all = t.instances("app");
        assertEquals(3, all.size());
        assertEquals(3, all.stream().filter(i -> i.nodeId().equals("n2")).count(),
                "恢复时副本已满，不应在 n1 凭空复活旧实例");
        System.out.println("    n1 恢复后保持空，3 个实例均为 n2 上重建的副本");
    }

    @Test
    @DisplayName("用例2.4 多工作负载共享容量：故障重建遵守集群级槽位，互不挤占超限")
    void capacitySharedAcrossWorkloads() {
        var t = CcpTestSupport.create();
        t.node("n1", 2);
        t.node("n2", 2);
        t.deploy("a", "imgA", "v1", 2, 1);
        t.deploy("b", "imgB", "v1", 2, 1);
        t.runUntilStable("a", 5);
        t.runUntilStable("b", 5);

        t.cp.markNodeLost("n2");
        t.cp.tick(3);
        t.assertCapacityNotExceeded();

        // n1 只有 2 槽位，a+b 共 4 个期望副本 => 总共只能存活 2 个
        long total = t.runningCount("a") + t.runningCount("b");
        assertTrue(total <= 2, "集群仅剩 2 槽位，两工作负载总实例不得超过 2，实际=" + total);
        System.out.println("    n2 失联后 a+b 总存活=" + total + "（容量 2），无超卖");
    }
}
