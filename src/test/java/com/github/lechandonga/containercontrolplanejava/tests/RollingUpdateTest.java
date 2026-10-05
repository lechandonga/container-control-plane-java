package com.github.lechandonga.containercontrolplanejava.tests;

import com.github.lechandonga.containercontrolplanejava.core.WorkloadRequest;
import com.github.lechandonga.containercontrolplanejava.model.Phase;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.support.BusinessLoggingExtension;
import com.github.lechandonga.containercontrolplanejava.support.CcpTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务三：版本滚动更新 —— 可用下限、彻底清理、失败告停与回滚、禁止长期混跑。
 */
@DisplayName("业务3-滚动更新与失败回滚")
@ExtendWith(BusinessLoggingExtension.class)
class RollingUpdateTest {

    private WorkloadRequest update(String id, String version, int replicas, int minAvailable,
                                   long startTimeout, long rolloutTimeout) {
        return new WorkloadRequest(id, "img", version, replicas, minAvailable,
                startTimeout, rolloutTimeout, null);
    }

    @Test
    @DisplayName("用例3.1 正常滚动：可用数全程不低于 minAvailable，旧版本被彻底清理")
    void rollingUpdateKeepsAvailabilityAndCleansOldVersion() {
        var t = CcpTestSupport.create();
        t.node("n1", 8);
        t.deploy("app", "img", "v1", 3, 2);
        t.runUntilStable("app", 5);

        t.cp.submit(update("app", "v2", 3, 2, 5, 50));

        // 逐 tick 推进，检查更新过程中的可用下限不变量
        int minObserved = Integer.MAX_VALUE;
        boolean sawTwoVersions = false;
        for (int i = 0; i < 30; i++) {
            t.cp.tick(1);
            Workload w = t.cp.snapshotView().getWorkloads().get("app");
            long available = t.runningCount("app", "v1") + t.runningCount("app", "v2");
            minObserved = Math.min(minObserved, (int) available);
            if (t.runningCount("app", "v1") > 0 && t.runningCount("app", "v2") > 0) {
                sawTwoVersions = true;
            }
            if (w.getPhase() == Phase.RUNNING) {
                break;
            }
        }

        Workload w = t.cp.snapshotView().getWorkloads().get("app");
        assertEquals(Phase.RUNNING, w.getPhase(), w.getReason());
        assertEquals("v2", w.getVersion());
        assertEquals(3, t.runningCount("app", "v2"));
        assertEquals(0, t.runningCount("app", "v1"), "旧版本实例必须被彻底清理");
        assertTrue(minObserved >= 2, "滚动期间可用数最低=" + minObserved + "，必须 >= minAvailable=2");
        assertTrue(sawTwoVersions, "滚动过程中应出现短暂的过渡混跑，但结束后不得残留");
        System.out.println("    滚动期间最低可用=" + minObserved + " >= 2；收敛后仅 v2 x3，无 v1 残留");
    }

    @Test
    @DisplayName("用例3.2 新版本启动失败：明确告停 UPDATE_FAILED 并回滚，不允许两版本长期混跑")
    void failedNewVersionAbortsAndRollsBack() {
        var t = CcpTestSupport.create();
        t.node("n1", 8);
        t.deploy("app", "img", "v1", 3, 1);
        t.runUntilStable("app", 5);

        // 注入：img 在节点上启动失败（模拟坏版本）
        t.simulator.injectImageFailure("n1", "img", "v2");
        t.cp.submit(update("app", "v2", 3, 1, 5, 50));

        Workload w = null;
        for (int i = 0; i < 15; i++) {
            t.cp.tick(1);
            w = t.cp.snapshotView().getWorkloads().get("app");
            if (w.getPhase() == Phase.UPDATE_FAILED) {
                break;
            }
        }
        assertEquals(Phase.UPDATE_FAILED, w.getPhase(), "新版本起不来必须明确告停");
        assertTrue(w.getReason().contains("回滚"), "原因中必须说明已回滚: " + w.getReason());
        assertEquals("v1", w.getTargetVersion(), "目标版本必须回退为 v1");
        assertEquals(0, t.instances("app").stream().filter(i -> i.version().equals("v2")).count(),
                "失败的新版本实例必须全部清理，不允许混跑");

        // 清除故障后，回滚补齐的 v1 副本最终收敛
        t.simulator.clearImageFailure("n1", "img", "v2");
        t.runUntilStable("app", 5);
        assertEquals(3, t.runningCount("app", "v1"));
        System.out.println("    告停=" + w.getReason());
    }

    @Test
    @DisplayName("用例3.3 更新无法在 rolloutTimeout 内推进完成：告停并确定性回滚")
    void rolloutTimeoutAbortsWithReason() {
        var t = CcpTestSupport.create();
        // 新版本在节点上需要 3 个 tick 才就绪，而 rolloutTimeout=2：第一批新版本 PENDING 期间
        // 总超时先到，更新必须告停并回滚旧版本（用显式 startupTicks 模拟“慢节点/卡死”）。
        t.node("n1", 3);
        t.cp.registerNode("n1", 3, 3);
        t.deploy("app", "img", "v1", 3, 1);
        t.runUntilStable("app", 5);

        t.cp.submit(update("app", "v2", 3, 1, 5, 2));
        Workload w = null;
        for (int i = 0; i < 15; i++) {
            t.cp.tick(1);
            w = t.cp.snapshotView().getWorkloads().get("app");
            if (w.getPhase() == Phase.UPDATE_FAILED) {
                break;
            }
        }
        assertEquals(Phase.UPDATE_FAILED, w.getPhase(), "总超时后必须告停");
        assertTrue(w.getReason().contains("rolloutTimeout"), "必须说明是总超时: " + w.getReason());
        assertTrue(t.instances("app").stream().noneMatch(i -> i.version().equals("v2")),
                "不得残留新版本实例");
        t.runUntilStable("app", 5);
        assertEquals(3, t.runningCount("app", "v1"), "回滚后旧版本补齐到 3");
        System.out.println("    新版本无法在总超时内就绪 -> 告停回滚，最终仅 v1 x3");
    }

    @Test
    @DisplayName("用例3.4 UPDATE_FAILED 后提交修复版本可重新更新并成功")
    void retryAfterFailureSucceeds() {
        var t = CcpTestSupport.create();
        t.node("n1", 8);
        t.deploy("app", "img", "v1", 2, 1);
        t.runUntilStable("app", 5);

        t.simulator.injectImageFailure("n1", "img", "v2-bad");
        t.cp.submit(update("app", "v2-bad", 2, 1, 3, 50));
        for (int i = 0; i < 10; i++) {
            t.cp.tick(1);
            if (t.cp.snapshotView().getWorkloads().get("app").getPhase() == Phase.UPDATE_FAILED) {
                break;
            }
        }
        assertEquals(Phase.UPDATE_FAILED, t.cp.snapshotView().getWorkloads().get("app").getPhase());

        t.simulator.clearImageFailure("n1", "img", "v2-bad");
        t.cp.submit(update("app", "v3-good", 2, 1, 5, 50));
        t.runUntilStable("app", 10);
        Workload w = t.cp.snapshotView().getWorkloads().get("app");
        assertEquals(Phase.RUNNING, w.getPhase());
        assertEquals("v3-good", w.getVersion());
        assertEquals(2, t.runningCount("app", "v3-good"));
        assertFalse(t.instances("app").stream().anyMatch(i -> i.version().startsWith("v2")),
                "失败版本不得残留");
        System.out.println("    失败后重新提交 v3-good 成功，最终仅 v3-good x2");
    }
}
