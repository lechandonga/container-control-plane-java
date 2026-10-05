package com.github.lechandonga.containercontrolplanejava.tests;

import com.github.lechandonga.containercontrolplanejava.model.UpdatePhase;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadPhase;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;
import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;
import com.github.lechandonga.containercontrolplanejava.tests.support.BusinessTestLogger;
import com.github.lechandonga.containercontrolplanejava.tests.support.BusinessUnderTest;
import com.github.lechandonga.containercontrolplanejava.tests.support.Scenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务域：版本滚动更新（surge、可用性下限、旧版本清理、失败回退、超时告停、禁止长期混跑）。
 */
@BusinessUnderTest("滚动更新：maxSurge 先建新后删旧、可用数不低于下限、失败/超时回退告停、旧版本彻底清理")
@ExtendWith(BusinessTestLogger.class)
class RollingUpdateTest extends ControlPlaneTestBase {

    @Test
    @Scenario("正常滚动更新：新版本全部就绪、旧版本零残留，最终 RUNNING/SUCCEEDED")
    void rollingUpdateSucceedsAndCleansOldVersion() {
        node(4);
        node(4);
        service.createWorkload("wl-ru-1", WorkloadSpec.of(4, "app:v1"));
        tickUntilConverged("wl-ru-1", 30);

        Workload wl = store.get("wl-ru-1");
        service.submit("wl-ru-1", WorkloadSpec.of(4, "app:v2"), wl.getResourceVersion());
        tickUntilConverged("wl-ru-1", 60);

        assertEquals(4, countImage("wl-ru-1", "app:v2"));
        assertEquals(0, countImage("wl-ru-1", "app:v1"), "旧版本必须被彻底清理");
        assertEquals(UpdatePhase.SUCCEEDED, store.get("wl-ru-1").getUpdatePhase());
    }

    @Test
    @Scenario("滚动更新全过程可用实例数始终不低于 minAvailable（逐步检测每个中间状态）")
    void availabilityNeverBelowMinAvailable() {
        node(3);
        node(3);
        WorkloadSpec initial = new WorkloadSpec(5, "app:v1", 0, 4, 5, 1000);
        service.createWorkload("wl-ru-2", initial);
        tickUntilConverged("wl-ru-2", 40);

        Workload wl = store.get("wl-ru-2");
        // maxSurge=0：不能先建后删，只能一个一个替换，minAvailable=4 是硬下限
        WorkloadSpec next = new WorkloadSpec(5, "app:v2", 0, 4, 5, 1000);
        service.submit("wl-ru-2", next, wl.getResourceVersion());

        List<Integer> observedAvailability = new ArrayList<>();
        for (int i = 0; i < 80; i++) {
            tick();
            observedAvailability.add((int) countAvailable("wl-ru-2"));
            Workload current = store.get("wl-ru-2");
            if (current.getUpdatePhase() == UpdatePhase.SUCCEEDED) {
                break;
            }
        }
        System.out.println("[BUSINESS-TESTS]   滚动期间逐轮可用实例数序列: " + observedAvailability);
        assertTrue(observedAvailability.stream().allMatch(v -> v >= 4),
                "任何中间状态可用数都不得低于 minAvailable=4，实际序列=" + observedAvailability);
        assertEquals(5, countImage("wl-ru-2", "app:v2"));
        assertEquals(0, countImage("wl-ru-2", "app:v1"));
    }

    @Test
    @Scenario("新版本在所有节点启动失败：超过尝试预算后回退到旧版本并明确告停，不两版本混跑")
    void failingNewVersionRollsBackAndStopsWithReason() {
        SimulatedNode a = node(4);
        SimulatedNode b = node(4);
        service.createWorkload("wl-ru-3", WorkloadSpec.of(4, "app:v1"));
        tickUntilConverged("wl-ru-3", 30);

        // v2 镜像在两个节点上都无法启动
        service.failImageOnNode(a.getInfo().getUid(), "app:v2");
        service.failImageOnNode(b.getInfo().getUid(), "app:v2");

        Workload wl = store.get("wl-ru-3");
        WorkloadSpec next = new WorkloadSpec(4, "app:v2", 1, 3, 3, 1000);
        service.submit("wl-ru-3", next, wl.getResourceVersion());

        assertEventually("wl-ru-3", 120,
                w -> w.getUpdatePhase() == UpdatePhase.FAILED && w.getPhase() == WorkloadPhase.FAILED,
                "更新失败必须进入 FAILED 终态并说明原因");

        tick(10);
        assertEquals(4, countImage("wl-ru-3", "app:v1"), "失败后服务必须回退并保持旧版本 4 副本");
        assertEquals(0, countImage("wl-ru-3", "app:v2"), "失败的新版本必须被彻底回收");
        String message = store.get("wl-ru-3").getConditions().toString();
        assertTrue(message.contains("RolloutFailed"), "必须记录失败原因: " + message);
    }

    @Test
    @Scenario("容量不足导致更新无法推进：超过 deadline 后超时告停并回退，原因可解释")
    void deadlineExceededFailsAndRollsBack() {
        // 两个节点总容量 4，v1 占满；v2 maxSurge=1 需要 5 个槽位，且 minAvailable=4 禁止先删旧
        node(2);
        node(2);
        service.createWorkload("wl-ru-4", new WorkloadSpec(4, "app:v1", 0, 4, 5, 1000));
        tickUntilConverged("wl-ru-4", 30);

        Workload wl = store.get("wl-ru-4");
        // maxSurge=0 且 minAvailable=4：4 个旧实例占满 4 容量，既不能先建新也不能先删旧 => 死锁
        service.submit("wl-ru-4", new WorkloadSpec(4, "app:v2", 0, 4, 5, 30),
                wl.getResourceVersion());

        boolean sawCapacityBlock = false;
        for (int i = 0; i < 40; i++) {
            tick();
            advanceSeconds(5);
            if (store.get("wl-ru-4").getConditions().stream()
                    .anyMatch(c -> c.reason().contains("Unschedulable"))) {
                sawCapacityBlock = true;
            }
            if (store.get("wl-ru-4").getUpdatePhase() == UpdatePhase.FAILED) {
                break;
            }
        }
        assertTrue(sawCapacityBlock, "更新被容量阻塞的原因应当被记录");
        assertEquals(UpdatePhase.FAILED, store.get("wl-ru-4").getUpdatePhase());
        String cond = store.get("wl-ru-4").getConditions().toString();
        assertTrue(cond.contains("DeadlineExceeded"), "必须明确是超时告停: " + cond);

        tick(30);
        assertEquals(4, countImage("wl-ru-4", "app:v1"), "超时回退后旧版本继续提供服务");
        assertEquals(0, countImage("wl-ru-4", "app:v2"), "新版本不得残留，杜绝长期混跑");
    }
}
