package com.github.lechandonga.containercontrolplanejava.tests;

import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadPhase;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;
import com.github.lechandonga.containercontrolplanejava.tests.support.BusinessTestLogger;
import com.github.lechandonga.containercontrolplanejava.tests.support.BusinessUnderTest;
import com.github.lechandonga.containercontrolplanejava.tests.support.Scenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * 业务域：期望状态声明与持续调谐的幂等性。
 */
@BusinessUnderTest("调谐幂等性：重复提交、重复事件、调谐中重启都不得造成实例重复/计数漂移/状态回退")
@ExtendWith(BusinessTestLogger.class)
class ReconcileIdempotencyTest extends ControlPlaneTestBase {

    @Test
    @Scenario("期望状态提交后逐轮收敛，副本数与镜像精确等于声明值")
    void convergesToDeclaredState() {
        node(4);
        node(4);
        Workload wl = service.createWorkload("wl-idem-1", WorkloadSpec.of(5, "app:v1"));
        tickUntilConverged("wl-idem-1", 30);

        assertEquals(5, store.instancesOf("wl-idem-1").size(), describe("wl-idem-1"));
        assertEquals(5, countAvailable("wl-idem-1"));
        assertEquals(WorkloadPhase.RUNNING, store.get("wl-idem-1").getPhase());
    }

    @Test
    @Scenario("重复提交完全相同的期望状态：generation/resourceVersion 不变，实例不重复")
    void duplicateSubmissionIsNoop() {
        node(3);
        service.createWorkload("wl-idem-2", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-idem-2", 20);

        Workload before = store.get("wl-idem-2");
        int instanceCount = store.instancesOf("wl-idem-2").size();
        long generation = before.getGeneration();
        String rv = before.getResourceVersion();

        for (int i = 0; i < 5; i++) {
            service.submit("wl-idem-2", WorkloadSpec.of(3, "app:v1"), before.getResourceVersion());
        }
        tick(10);

        Workload after = store.get("wl-idem-2");
        assertEquals(generation, after.getGeneration(), "重复提交不得推进 generation");
        assertEquals(rv, after.getResourceVersion(), "重复提交不得换发 resourceVersion");
        assertEquals(instanceCount, store.instancesOf("wl-idem-2").size(),
                "重复提交不得产生重复实例");
    }

    @Test
    @Scenario("重复触发调谐事件（空转多轮）：计数零漂移，状态不回退")
    void duplicateReconcileEventsCauseNoDrift() {
        node(3);
        service.createWorkload("wl-idem-3", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-idem-3", 20);

        tick(50); // 大量重复事件

        assertEquals(3, store.instancesOf("wl-idem-3").size());
        assertEquals(3, countAvailable("wl-idem-3"));
        assertEquals(WorkloadPhase.RUNNING, store.get("wl-idem-3").getPhase());
    }

    @Test
    @Scenario("调谐过程中模拟控制面重启：从存储重新调谐，不产生重复实例，最终仍收敛")
    void restartDuringReconcileDoesNotDuplicate() {
        node(4);
        service.createWorkload("wl-idem-4", WorkloadSpec.of(4, "app:v1"));
        tick(2); // 只推进两步，人为制造“调谐中途”
        int partial = store.instancesOf("wl-idem-4").size();
        assertNotEquals(4, partial, "前置条件：此时确实尚未收敛完成");

        service.restartControlPlane(); // 重启：没有任何内存事件，仅靠存储恢复
        tickUntilConverged("wl-idem-4", 30);

        assertEquals(4, store.instancesOf("wl-idem-4").size(),
                "重启恢复后实例数必须精确等于期望，不能因重放产生重复");
        assertEquals(4, countAvailable("wl-idem-4"));
    }

    @Test
    @Scenario("节点幂等性：同一实例 id 被重复下发 start 不会叠加容器计数")
    void repeatedNodeStartIsIdempotent() {
        var n = node(3);
        service.createWorkload("wl-idem-5", WorkloadSpec.of(1, "app:v1"));
        tickUntilConverged("wl-idem-5", 10);
        String instanceId = store.instancesOf("wl-idem-5").get(0).getId();
        String image = store.instancesOf("wl-idem-5").get(0).getImage();

        for (int i = 0; i < 5; i++) {
            n.startContainer(instanceId, image);
        }
        assertEquals(1, n.runningCount(), "节点对同一实例 id 的重复 start 必须幂等");
    }
}
