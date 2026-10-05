package com.github.lechandonga.containercontrolplanejava.tests;

import com.github.lechandonga.containercontrolplanejava.core.ControlPlane;
import com.github.lechandonga.containercontrolplanejava.core.WorkloadRequest;
import com.github.lechandonga.containercontrolplanejava.model.Phase;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.sim.NodeSimulator;
import com.github.lechandonga.containercontrolplanejava.sim.SimulationClock;
import com.github.lechandonga.containercontrolplanejava.store.StateStore;
import com.github.lechandonga.containercontrolplanejava.support.BusinessLoggingExtension;
import com.github.lechandonga.containercontrolplanejava.support.CcpTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务一：期望状态声明与持续调谐的幂等性。
 * 覆盖：重复提交、重复 tick、调谐中途重启、副本扩缩容漂移。
 */
@DisplayName("业务1-调谐幂等与重启恢复")
@ExtendWith(BusinessLoggingExtension.class)
class ReconcileIdempotencyTest {

    @Test
    @DisplayName("用例1.1 重复提交相同期望状态不会产生重复实例")
    void duplicateSubmissionsDoNotDuplicateInstances() {
        var t = CcpTestSupport.create();
        t.node("n1", 10);
        var first = t.deploy("app", "img", "v1", 3, 1);

        for (int i = 0; i < 5; i++) {
            t.deploy("app", "img", "v1", 3, 1);
        }
        t.cp.reconcile();
        t.cp.reconcile();

        assertEquals(3, t.instances("app").size(), "重复提交后实例数必须仍为 3");
        assertEquals(first.getResourceVersion(),
                t.cp.snapshotView().getWorkloads().get("app").getResourceVersion(),
                "相同期望重复提交不得推进 resourceVersion");
        assertTrue(t.hasEvent("app", "SubmitDedup"::equals), "应记录幂等去重事件");
        System.out.println("    实际实例数=3，resourceVersion 未漂移，事件含 SubmitDedup");
    }

    @Test
    @DisplayName("用例1.2 重复调谐(tick)不产生计数漂移，最终精确收敛到 replicas")
    void repeatedTicksConvergeExactly() {
        var t = CcpTestSupport.create();
        t.node("n1", 10);
        t.deploy("app", "img", "v1", 4, 1);

        for (int i = 0; i < 10; i++) {
            t.cp.tick(1);
        }

        Workload w = t.runUntilStable("app", 1);
        assertEquals(Phase.RUNNING, w.getPhase());
        assertEquals(4, t.instances("app").size());
        assertEquals(4, t.runningCount("app"));
        t.assertCapacityNotExceeded();
        System.out.println("    10 轮 tick 后实例=就绪=4，无漂移，相位 RUNNING");
    }

    @Test
    @DisplayName("用例1.3 调谐中途重启：从持久化快照恢复后继续收敛，实例不重复、状态不回退")
    void restartMidReconcileResumesFromSnapshot() {
        var t = CcpTestSupport.create();
        t.node("n1", 10);
        t.deploy("app", "img", "v1", 3, 1);
        t.cp.tick(1);
        int before = t.instances("app").size();
        assertEquals(3, before);

        // 模拟控制面进程在调谐间隙崩溃后重启：用同一个状态文件重建全部组件。
        SimulationClock clock2 = new SimulationClock(false);
        NodeSimulator sim2 = new NodeSimulator(1);
        StateStore store2 = new StateStore(t.store.getFile().toString());
        ControlPlane restarted = new ControlPlane(store2, clock2, sim2);
        restarted.restore();
        sim2.registerNode("n1", 1);
        restarted.reconcile();

        Workload w = restarted.snapshotView().getWorkloads().get("app");
        assertEquals(3, w.getInstances().size(), "重启恢复后实例数必须仍为 3");
        long maxSeq = w.getInstances().stream().mapToLong(i ->
                Long.parseLong(i.id().substring(i.id().lastIndexOf('-') + 1))).max().orElse(0);
        assertEquals(3, maxSeq, "seq 必须单调延续，不得复用或重建重复 ID");

        restarted.tick(1);
        w = restarted.snapshotView().getWorkloads().get("app");
        assertEquals(Phase.RUNNING, w.getPhase());
        assertEquals(3, w.getInstances().size());
        System.out.println("    重启后实例仍为 3 个，seq 延续到 app-3，继续 tick 后 RUNNING");
    }

    @Test
    @DisplayName("用例1.4 扩容/缩容幂等：重复提交新副本数只生效一次且精确收敛")
    void scaleIdempotently() {
        var t = CcpTestSupport.create();
        t.node("n1", 10);
        t.deploy("app", "img", "v1", 2, 1);
        t.runUntilStable("app", 5);

        WorkloadRequest scaleUp = new WorkloadRequest("app", "img", "v1", 5, 1, 5, 20, null);
        t.cp.submit(scaleUp);
        t.cp.submit(scaleUp);
        t.runUntilStable("app", 5);
        assertEquals(5, t.instances("app").size());

        WorkloadRequest scaleDown = new WorkloadRequest("app", "img", "v1", 2, 1, 5, 20, null);
        t.cp.submit(scaleDown);
        t.runUntilStable("app", 5);
        assertEquals(2, t.instances("app").size());
        System.out.println("    2 -> 5 -> 2，重复提交只生效一次，实例数精确");
    }
}
