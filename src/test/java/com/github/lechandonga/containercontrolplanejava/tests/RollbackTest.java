package com.github.lechandonga.containercontrolplanejava.tests;

import com.github.lechandonga.containercontrolplanejava.model.Revision;
import com.github.lechandonga.containercontrolplanejava.model.RevisionStatus;
import com.github.lechandonga.containercontrolplanejava.model.UpdatePhase;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadPhase;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;
import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;
import com.github.lechandonga.containercontrolplanejava.store.ConflictException;
import com.github.lechandonga.containercontrolplanejava.tests.support.BusinessTestLogger;
import com.github.lechandonga.containercontrolplanejava.tests.support.BusinessUnderTest;
import com.github.lechandonga.containercontrolplanejava.tests.support.Scenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务域：按修订回滚。回滚走正常滚动通道、守住可用下限与容量上限；目标版本起不来时
 * 告停且零非目标残留；回滚本身作为新修订留痕；回滚请求受与更新一致的乐观并发保护。
 */
@BusinessUnderTest("按修订回滚：指定修订/回退上一个、单版本精确收敛、目标起不来零残留告停、并发冲突拒绝")
@ExtendWith(BusinessTestLogger.class)
class RollbackTest extends ControlPlaneTestBase {

    @Test
    @Scenario("回滚到指定历史修订：回到该修订声明的副本数与版本，最终只有目标版本且副本数精确")
    void rollbackToNamedRevisionConvergesSingleVersion() {
        node(6);
        node(6);
        service.createWorkload("wl-rb-1", WorkloadSpec.of(4, "app:v1"));
        tickUntilConverged("wl-rb-1", 30);
        String rv1 = store.get("wl-rb-1").getResourceVersion();
        service.submit("wl-rb-1", WorkloadSpec.of(4, "app:v2"), rv1);
        tickUntilConverged("wl-rb-1", 60);
        String rv2 = store.get("wl-rb-1").getResourceVersion();
        service.submit("wl-rb-1", WorkloadSpec.of(2, "app:v3"), rv2);
        tickUntilConverged("wl-rb-1", 60);
        assertEquals(2, countImage("wl-rb-1", "app:v3"));

        String rv3 = store.get("wl-rb-1").getResourceVersion();
        service.rollback("wl-rb-1", 1L, rv3); // 回到 r1：4 副本、v1
        tickUntilConverged("wl-rb-1", 80);

        assertEquals(4, countImage("wl-rb-1", "app:v1"), describe("wl-rb-1"));
        assertEquals(0, countImage("wl-rb-1", "app:v2"));
        assertEquals(0, countImage("wl-rb-1", "app:v3"), "回滚后不得残留任何非目标版本");
        assertEquals(4, store.instancesOf("wl-rb-1").size(), "收敛后实例数必须精确等于期望副本数");
        assertEquals(UpdatePhase.SUCCEEDED, store.get("wl-rb-1").getUpdatePhase());

        Revision rollback = service.getRevision("wl-rb-1", 4);
        assertEquals(RevisionStatus.SUCCEEDED, rollback.getStatus());
        assertEquals(1L, rollback.getTargetRevision());
        assertEquals("app:v1", rollback.getAfter().image());
        assertEquals(4, rollback.getAfter().replicas());
    }

    @Test
    @Scenario("退回上一个修订：省略目标修订号，回滚成功且被撤销的中间修订标记 ROLLED_BACK")
    void rollbackToPreviousRevision() {
        node(5);
        service.createWorkload("wl-rb-2", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-rb-2", 30);
        String rv1 = store.get("wl-rb-2").getResourceVersion();
        service.submit("wl-rb-2", WorkloadSpec.of(3, "app:v2"), rv1);
        tickUntilConverged("wl-rb-2", 50);

        String rv2 = store.get("wl-rb-2").getResourceVersion();
        service.rollback("wl-rb-2", null, rv2);
        tickUntilConverged("wl-rb-2", 60);

        assertEquals(3, countImage("wl-rb-2", "app:v1"));
        assertEquals(0, countImage("wl-rb-2", "app:v2"));
        Revision rolledBack = service.getRevision("wl-rb-2", 2);
        assertEquals(RevisionStatus.ROLLED_BACK, rolledBack.getStatus());
        assertTrue(rolledBack.getReason().contains("RolledBackTo:r1"));
        Revision rollback = service.getRevision("wl-rb-2", 3);
        assertEquals(1L, rollback.getTargetRevision(),
                "省略目标时默认回退到当前修订（r2）的上一个修订 r1");
    }

    @Test
    @Scenario("回滚中目标版本起不来：明确告停、保留失败原因，且不残留任何非目标版本实例")
    void rollbackTargetCannotStartStopsWithNoOtherVersion() {
        SimulatedNode a = node(4);
        SimulatedNode b = node(4);
        service.createWorkload("wl-rb-3", WorkloadSpec.of(4, "app:v1"));
        tickUntilConverged("wl-rb-3", 30);
        String rv1 = store.get("wl-rb-3").getResourceVersion();
        service.submit("wl-rb-3", WorkloadSpec.of(4, "app:v2"), rv1);
        tickUntilConverged("wl-rb-3", 60);

        // 现在 v1 在所有节点都起不来；从 v2 回滚到 v1 必然失败
        service.failImageOnNode(a.getInfo().getUid(), "app:v1");
        service.failImageOnNode(b.getInfo().getUid(), "app:v1");

        String rv2 = store.get("wl-rb-3").getResourceVersion();
        service.rollback("wl-rb-3", 1L, rv2);

        assertEventually("wl-rb-3", 150,
                w -> w.getPhase() == WorkloadPhase.FAILED
                        && w.getUpdatePhase() == UpdatePhase.FAILED,
                "目标版本起不来时回滚必须告停");

        tick(20);
        assertEquals(0, countImage("wl-rb-3", "app:v2"), "非目标版本 v2 必须被彻底清退");
        assertEquals(0, countImage("wl-rb-3", "app:v1"), "目标版本 v1 起不来时不得有实例对外服务");
        long live = store.instancesOf("wl-rb-3").stream()
                .filter(r -> r.getState()
                        != com.github.lechandonga.containercontrolplanejava.model.InstanceState.LOST)
                .count();
        assertEquals(0, live, "不允许两个版本同时对外服务，起不来就零实例告停");
        Revision rollback = service.getRevision("wl-rb-3", 3);
        assertEquals(RevisionStatus.FAILED, rollback.getStatus());
        assertTrue(rollback.getReason().contains("RolloutFailed"));
        assertTrue(store.get("wl-rb-3").getConditions().toString().contains("回滚告停"));

        // 告停后不会自行反复重试：再多跑若干轮，终态与原因保持原样
        tick(20);
        assertEquals(UpdatePhase.FAILED, store.get("wl-rb-3").getUpdatePhase());
        assertEquals(0, countImage("wl-rb-3", "app:v1") + countImage("wl-rb-3", "app:v2"));
    }

    @Test
    @Scenario("回滚时健康容量不足以承载目标副本数：如实告停说明容量原因，非目标版本零残留")
    void rollbackWithoutCapacityStopsWithCapacityReason() {
        node(2);
        node(2);
        // r1：4 副本 v1，minAvailable=0（允许回滚清退时跌破可用性以保证零非目标残留）
        service.createWorkload("wl-rb-4", new WorkloadSpec(4, "app:v1", 1, 0, 5, 1000));
        tickUntilConverged("wl-rb-4", 30);
        // 滚到 2 副本 v2（有充足容量）
        String rv1 = store.get("wl-rb-4").getResourceVersion();
        service.submit("wl-rb-4", new WorkloadSpec(2, "app:v2", 1, 2, 5, 1000), rv1);
        tickUntilConverged("wl-rb-4", 80);

        // 一个节点失联且不恢复：健康容量只剩 2，无法承载回滚目标 4 副本 v1。
        SimulatedNode lost = nodeRegistry.all().get(0);
        service.partitionNode(lost.getInfo().getUid());
        tick(30);

        String rv2 = store.get("wl-rb-4").getResourceVersion();
        service.rollback("wl-rb-4", 1L, rv2);
        for (int i = 0; i < 120; i++) {
            tick();
            advanceSeconds(10);
            if (store.get("wl-rb-4").getUpdatePhase() == UpdatePhase.FAILED) {
                break;
            }
        }
        assertEquals(UpdatePhase.FAILED, store.get("wl-rb-4").getUpdatePhase(),
                "容量不足以完成回滚时必须告停: " + describe("wl-rb-4"));
        for (int i = 0; i < 40; i++) {
            tick();
        }
        assertEquals(0, countImage("wl-rb-4", "app:v2"), "非目标版本不得残留");
        assertTrue(store.get("wl-rb-4").getConditions().toString().contains("回滚告停"),
                store.get("wl-rb-4").getConditions().toString());
    }

    @Test
    @Scenario("回滚请求与并发修改相撞：过期版本/缺失版本被明确拒绝，被拒不产生修订")
    void rollbackConflictIsRejectedLikeUpdate() throws InterruptedException {
        node(5);
        service.createWorkload("wl-rb-5", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-rb-5", 20);
        String rv1 = store.get("wl-rb-5").getResourceVersion();
        service.submit("wl-rb-5", WorkloadSpec.of(3, "app:v2"), rv1);
        tickUntilConverged("wl-rb-5", 40);

        String stale = rv1;
        assertThrows(ConflictException.class,
                () -> service.rollback("wl-rb-5", 1L, stale), "过期版本回滚必须冲突拒绝");
        assertThrows(ConflictException.class,
                () -> service.rollback("wl-rb-5", 1L, null), "不带版本号的回滚必须拒绝");

        // 并发：回滚与扩缩容基于同一 resourceVersion，恰好一个胜出
        String rv2 = store.get("wl-rb-5").getResourceVersion();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        for (int i = 0; i < 2; i++) {
            final boolean doRollback = i == 0;
            pool.submit(() -> {
                try {
                    start.await();
                    if (doRollback) {
                        service.rollback("wl-rb-5", 1L, rv2);
                    } else {
                        service.submit("wl-rb-5", WorkloadSpec.of(2, "app:v2"), rv2);
                    }
                    accepted.incrementAndGet();
                } catch (ConflictException e) {
                    rejected.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(1, accepted.get());
        assertEquals(1, rejected.get(), "撞上的回滚/更新按既有规则冲突拒绝");

        tickUntilConverged("wl-rb-5", 60);
        long revisionCount = service.listRevisions("wl-rb-5").size();
        assertEquals(3, revisionCount, "无论谁胜出都只产生一条新修订，被拒操作不入史");
    }

    @Test
    @Scenario("重复回滚到当前已声明的修订：幂等空操作，不产生新修订、不跳号")
    void duplicateRollbackIsNoop() {
        node(4);
        service.createWorkload("wl-rb-6", WorkloadSpec.of(2, "app:v1"));
        tickUntilConverged("wl-rb-6", 20);
        String rv1 = store.get("wl-rb-6").getResourceVersion();
        service.submit("wl-rb-6", WorkloadSpec.of(2, "app:v2"), rv1);
        tickUntilConverged("wl-rb-6", 40);
        String rv2 = store.get("wl-rb-6").getResourceVersion();
        service.rollback("wl-rb-6", 1L, rv2);
        tickUntilConverged("wl-rb-6", 50);

        String rv3 = store.get("wl-rb-6").getResourceVersion();
        Workload again = service.rollback("wl-rb-6", 1L, rv3);
        assertEquals(rv3, again.getResourceVersion(), "重复回滚不得换发版本");
        assertEquals(3, service.listRevisions("wl-rb-6").size());
    }
}
