package com.github.lechandonga.containercontrolplanejava.tests;

import com.github.lechandonga.containercontrolplanejava.model.WorkloadPhase;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;
import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;
import com.github.lechandonga.containercontrolplanejava.revision.Revision;
import com.github.lechandonga.containercontrolplanejava.revision.RevisionStatus;
import com.github.lechandonga.containercontrolplanejava.revision.RevisionTrigger;
import com.github.lechandonga.containercontrolplanejava.store.ConflictException;
import com.github.lechandonga.containercontrolplanejava.store.InvalidRollbackException;
import com.github.lechandonga.containercontrolplanejava.tests.support.BusinessTestLogger;
import com.github.lechandonga.containercontrolplanejava.tests.support.BusinessUnderTest;
import com.github.lechandonga.containercontrolplanejava.tests.support.Scenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务域：发布历史（修订）与按修订回滚。
 */
@BusinessUnderTest("发布历史与回滚：修订连续留痕可查、重复/冲突不入历史、按修订回滚单版本收敛、"
        + "回滚失败告清零残留、回滚受并发保护")
@ExtendWith(BusinessTestLogger.class)
class RevisionHistoryTest extends ControlPlaneTestBase {

    @Test
    @Scenario("每次真正改变期望状态的提交产生一条修订：序号连续、含前后声明，收敛后标记成功")
    void revisionsAreGeneratedForRealChangesAndQueryable() {
        node(6);
        service.createWorkload("wl-rh-1", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-rh-1", 30);

        String rv = store.get("wl-rh-1").getResourceVersion();
        service.submit("wl-rh-1", WorkloadSpec.of(4, "app:v1"), rv);
        tickUntilConverged("wl-rh-1", 30);
        rv = store.get("wl-rh-1").getResourceVersion();
        service.submit("wl-rh-1", WorkloadSpec.of(4, "app:v2"), rv);
        tickUntilConverged("wl-rh-1", 60);

        List<Revision> revisions = revisionLog.list("wl-rh-1");
        assertEquals(3, revisions.size(), "创建 + 两次真实变更 = 3 条修订");
        assertEquals(1, revisions.get(0).getRevision());
        assertEquals(2, revisions.get(1).getRevision());
        assertEquals(3, revisions.get(2).getRevision(), "修订号必须严格连续不跳号");

        Revision first = revisions.get(0);
        assertEquals(RevisionTrigger.CREATE, first.getTrigger());
        assertNull(first.getBeforeSpec(), "首条修订没有变更前声明");
        assertEquals("app:v1", first.getAfterSpec().image());
        assertEquals(3, first.getAfterSpec().replicas());
        assertNotNull(first.getCreatedAt());

        Revision third = revisions.get(2);
        assertEquals("app:v1", third.getBeforeSpec().image());
        assertEquals(4, third.getBeforeSpec().replicas());
        assertEquals("app:v2", third.getAfterSpec().image());
        assertEquals(RevisionStatus.SUCCEEDED, third.getStatus());
        assertNotNull(third.getFinishedAt());

        Revision single = revisionLog.get("wl-rh-1", 2).orElseThrow();
        assertEquals(4, single.getAfterSpec().replicas());
        assertEquals(RevisionStatus.SUCCEEDED, single.getStatus());
    }

    @Test
    @Scenario("完全相同的重复提交与乐观锁冲突被拒：不写历史、修订号不跳号")
    void duplicateAndConflictSubmissionsProduceNoRevision() {
        node(4);
        service.createWorkload("wl-rh-2", WorkloadSpec.of(2, "app:v1"));
        tickUntilConverged("wl-rh-2", 20);

        String stale = store.get("wl-rh-2").getResourceVersion();
        // 一次成功更新把 resourceVersion 推进
        service.submit("wl-rh-2", WorkloadSpec.of(3, "app:v1"), stale);
        tickUntilConverged("wl-rh-2", 20);
        String current = store.get("wl-rh-2").getResourceVersion();
        for (int i = 0; i < 5; i++) {
            service.submit("wl-rh-2", WorkloadSpec.of(3, "app:v1"), current);
        }
        // 拿着过期版本的变更必须冲突被拒
        assertThrows(ConflictException.class,
                () -> service.submit("wl-rh-2", WorkloadSpec.of(2, "app:v9"), stale));
        tick(10);

        List<Revision> revisions = revisionLog.list("wl-rh-2");
        assertEquals(2, revisions.size(), "重复提交与冲突被拒都不能产生修订");
        assertEquals(2, revisions.get(1).getRevision(), "修订号连续不跳号");
    }

    @Test
    @Scenario("按修订号回滚成功：回到目标修订的副本数与版本，单版本精确收敛，回滚本身是新修订")
    void rollbackToRevisionConvergesToOneVersionAndAppendsRevision() {
        node(6);
        service.createWorkload("wl-rh-3", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-rh-3", 30);
        String rv = store.get("wl-rh-3").getResourceVersion();
        service.submit("wl-rh-3", WorkloadSpec.of(4, "app:v2"), rv);
        tickUntilConverged("wl-rh-3", 60);

        String rv2 = store.get("wl-rh-3").getResourceVersion();
        service.rollback("wl-rh-3", 1L, rv2);
        tickUntilConverged("wl-rh-3", 80);

        assertEquals(3, countImage("wl-rh-3", "app:v1"), "回滚后必须精确收敛到 3 个 v1");
        assertEquals(0, countImage("wl-rh-3", "app:v2"), "v2 必须零残留，不允许混跑");
        assertEquals(3, store.instancesOf("wl-rh-3").size(), "实例数精确等于期望，不能多出来");

        List<Revision> revisions = revisionLog.list("wl-rh-3");
        assertEquals(3, revisions.size(), "回滚作为一条新修订留痕，不覆盖旧历史");
        Revision rollbackRevision = revisions.get(2);
        assertEquals(RevisionTrigger.ROLLBACK, rollbackRevision.getTrigger());
        assertEquals(1L, rollbackRevision.getTargetRevision());
        assertEquals("app:v1", rollbackRevision.getAfterSpec().image());
        assertEquals(3, rollbackRevision.getAfterSpec().replicas());
        assertEquals(RevisionStatus.SUCCEEDED, rollbackRevision.getStatus());
        assertEquals(RevisionStatus.ROLLED_BACK, revisions.get(1).getStatus());
        assertEquals(RevisionStatus.SUCCEEDED, revisions.get(0).getStatus());
    }

    @Test
    @Scenario("“退回上一个修订”省略目标修订号：回到倒数第二条修订声明")
    void rollbackToPreviousRevisionShorthand() {
        node(6);
        service.createWorkload("wl-rh-4", WorkloadSpec.of(2, "app:v1"));
        tickUntilConverged("wl-rh-4", 20);
        String rv = store.get("wl-rh-4").getResourceVersion();
        service.submit("wl-rh-4", WorkloadSpec.of(3, "app:v2"), rv);
        tickUntilConverged("wl-rh-4", 60);
        rv = store.get("wl-rh-4").getResourceVersion();
        service.submit("wl-rh-4", WorkloadSpec.of(3, "app:v3"), rv);
        tickUntilConverged("wl-rh-4", 60);

        service.rollback("wl-rh-4", null, store.get("wl-rh-4").getResourceVersion());
        tickUntilConverged("wl-rh-4", 80);

        assertEquals(3, countImage("wl-rh-4", "app:v2"), "“上一个修订”即修订 2：v2/3 副本");
        assertEquals(0, countImage("wl-rh-4", "app:v3"));
        Revision latest = revisionLog.latest("wl-rh-4").orElseThrow();
        assertEquals(RevisionTrigger.ROLLBACK, latest.getTrigger());
        assertEquals(2L, latest.getTargetRevision());
    }

    @Test
    @Scenario("回滚的目标版本起不来：明确告停且说明原因，非目标版本实例被彻底清理，不两版本对外")
    void rollbackFailsWhenTargetImageCannotStartAndLeavesNoOtherVersion() {
        SimulatedNode a = node(4);
        SimulatedNode b = node(4);
        service.createWorkload("wl-rh-5", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-rh-5", 30);
        String rv = store.get("wl-rh-5").getResourceVersion();
        service.submit("wl-rh-5", WorkloadSpec.of(3, "app:v2"), rv);
        tickUntilConverged("wl-rh-5", 60);

        service.failImageOnNode(a.getInfo().getUid(), "app:v1");
        service.failImageOnNode(b.getInfo().getUid(), "app:v1");

        service.rollback("wl-rh-5", 1L, store.get("wl-rh-5").getResourceVersion());
        assertEventually("wl-rh-5", 200,
                w -> w.getPhase() == WorkloadPhase.FAILED,
                "回滚目标版本起不来必须明确告停");
        tick(20);

        assertEquals(0, countImage("wl-rh-5", "app:v2"),
                "告停后非目标版本 v2 必须零残留，杜绝两版本同时对外");
        assertEquals(WorkloadPhase.FAILED, store.get("wl-rh-5").getPhase());
        Revision rollbackRevision = revisionLog.latest("wl-rh-5").orElseThrow();
        assertEquals(RevisionStatus.FAILED, rollbackRevision.getStatus());
        assertNotNull(rollbackRevision.getReason());
        System.out.println("[BUSINESS-TESTS]   回滚告停原因: " + rollbackRevision.getReason());

        int instancesAfterFailure = store.instancesOf("wl-rh-5").size();
        tick(30);
        assertEquals(WorkloadPhase.FAILED, store.get("wl-rh-5").getPhase(),
                "告停后不得自行反复重试");
        assertEquals(instancesAfterFailure, store.instancesOf("wl-rh-5").size(),
                "告停现场空转前后保持原样");
    }

    @Test
    @Scenario("回滚请求受与更新相同的并发保护：过期版本被拒、不带版本被拒，均不产生修订")
    void rollbackConflictsAreRejectedWithoutRevision() {
        node(4);
        service.createWorkload("wl-rh-6", WorkloadSpec.of(2, "app:v1"));
        tickUntilConverged("wl-rh-6", 20);
        String rv = store.get("wl-rh-6").getResourceVersion();
        service.submit("wl-rh-6", WorkloadSpec.of(2, "app:v2"), rv);
        tickUntilConverged("wl-rh-6", 60);

        String stale = rv;
        String rv2 = store.get("wl-rh-6").getResourceVersion();
        service.submit("wl-rh-6", WorkloadSpec.of(3, "app:v2"), rv2);
        tickUntilConverged("wl-rh-6", 30);

        assertThrows(ConflictException.class,
                () -> service.rollback("wl-rh-6", 1L, stale),
                "过期版本的回滚必须按既有规则明确拒绝");
        assertThrows(ConflictException.class,
                () -> service.rollback("wl-rh-6", 1L, null),
                "不带 resourceVersion 的回滚必须拒绝");

        assertEquals(3, revisionLog.list("wl-rh-6").size(),
                "被拒回滚不产生历史（创建 + 两次成功更新）");

        assertThrows(InvalidRollbackException.class,
                () -> service.rollback("wl-rh-6", 99L,
                        store.get("wl-rh-6").getResourceVersion()));
        assertEquals(3, revisionLog.list("wl-rh-6").size());
    }

    @Test
    @Scenario("回滚与并发更新基于同一版本竞争：恰一个胜出，另一个冲突拒绝，最终单版本精确收敛")
    void rollbackCompetingWithConcurrentUpdateExactlyOneWins() throws Exception {
        node(8);
        service.createWorkload("wl-rh-7", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-rh-7", 30);
        String rv = store.get("wl-rh-7").getResourceVersion();
        service.submit("wl-rh-7", WorkloadSpec.of(3, "app:v2"), rv);
        tickUntilConverged("wl-rh-7", 60);
        String base = store.get("wl-rh-7").getResourceVersion();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger rollbackWins = new AtomicInteger();
        AtomicInteger updateWins = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        pool.submit(() -> {
            try {
                start.await();
                service.rollback("wl-rh-7", 1L, base);
                rollbackWins.incrementAndGet();
            } catch (ConflictException e) {
                rejected.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        pool.submit(() -> {
            try {
                start.await();
                service.submit("wl-rh-7", WorkloadSpec.of(3, "app:v3"), base);
                updateWins.incrementAndGet();
            } catch (ConflictException e) {
                rejected.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(1, rollbackWins.get() + updateWins.get(), "只能有一个操作胜出");
        assertEquals(1, rejected.get(), "另一个必须被明确冲突拒绝");

        tickUntilConverged("wl-rh-7", 80);
        String expectedImage = rollbackWins.get() == 1 ? "app:v1" : "app:v3";
        String otherImage = rollbackWins.get() == 1 ? "app:v3" : "app:v1";
        assertEquals(3, countImage("wl-rh-7", expectedImage), describe("wl-rh-7"));
        assertEquals(0, countImage("wl-rh-7", otherImage), "最终只能有一个版本对外服务");
        assertEquals(3, store.instancesOf("wl-rh-7").size(), "实例数精确等于期望，不多不少");
    }
}
