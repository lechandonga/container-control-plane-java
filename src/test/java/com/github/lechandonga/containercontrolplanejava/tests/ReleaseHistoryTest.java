package com.github.lechandonga.containercontrolplanejava.tests;

import com.github.lechandonga.containercontrolplanejava.model.Revision;
import com.github.lechandonga.containercontrolplanejava.model.RevisionStatus;
import com.github.lechandonga.containercontrolplanejava.model.UpdatePhase;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;
import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;
import com.github.lechandonga.containercontrolplanejava.store.ConflictException;
import com.github.lechandonga.containercontrolplanejava.tests.support.BusinessTestLogger;
import com.github.lechandonga.containercontrolplanejava.tests.support.BusinessUnderTest;
import com.github.lechandonga.containercontrolplanejava.tests.support.Scenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务域：发布历史。每次真正改变期望状态的提交留一条修订；重复提交/冲突被拒不写历史、
 * 不跳号；修订终态如实记录成功/失败/被回退；历史只能随工作负载删除而消失。
 */
@BusinessUnderTest("发布历史：修订序号连续、变更前后声明可查、终态可解释、重复与冲突不入史不跳号")
@ExtendWith(BusinessTestLogger.class)
class ReleaseHistoryTest extends ControlPlaneTestBase {

    @Test
    @Scenario("每次真正改变期望状态的提交都生成一条修订：序号、时间、前后声明齐全且连续")
    void revisionsRecordedWithSequentialNumbers() {
        node(6);
        service.createWorkload("wl-rh-1", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-rh-1", 20);

        String rv1 = store.get("wl-rh-1").getResourceVersion();
        service.submit("wl-rh-1", WorkloadSpec.of(4, "app:v1"), rv1);
        tickUntilConverged("wl-rh-1", 20);
        String rv2 = store.get("wl-rh-1").getResourceVersion();
        service.submit("wl-rh-1", WorkloadSpec.of(4, "app:v2"), rv2);
        tickUntilConverged("wl-rh-1", 40);

        List<Revision> revisions = service.listRevisions("wl-rh-1");
        assertEquals(3, revisions.size(), describe("wl-rh-1"));
        assertEquals(List.of(1L, 2L, 3L), revisions.stream().map(Revision::getRevision).toList());

        Revision first = revisions.get(0);
        assertNull(first.getBefore(), "创建修订没有变更前声明");
        assertEquals("app:v1", first.getAfter().image());
        assertEquals(3, first.getAfter().replicas());
        assertNotNull(first.getCreatedAt());

        Revision scale = revisions.get(1);
        assertEquals(3, scale.getBefore().replicas());
        assertEquals(4, scale.getAfter().replicas());
        assertEquals("app:v1", scale.getBefore().image());
        assertEquals(RevisionStatus.SUCCEEDED, scale.getStatus());

        Revision update = revisions.get(2);
        assertEquals("app:v1", update.getBefore().image());
        assertEquals("app:v2", update.getAfter().image());
        assertEquals(RevisionStatus.SUCCEEDED, update.getStatus());

        assertEquals(update, service.getRevision("wl-rh-1", 3), "单条修订查询");
    }

    @Test
    @Scenario("完全相同的重复提交不写历史、修订号不跳号")
    void duplicateSubmissionsCreateNoRevision() {
        node(4);
        service.createWorkload("wl-rh-2", WorkloadSpec.of(2, "app:v1"));
        tickUntilConverged("wl-rh-2", 20);

        Workload wl = store.get("wl-rh-2");
        for (int i = 0; i < 4; i++) {
            service.submit("wl-rh-2", WorkloadSpec.of(2, "app:v1"), wl.getResourceVersion());
        }
        tick(10);

        List<Revision> revisions = service.listRevisions("wl-rh-2");
        assertEquals(1, revisions.size());
        assertEquals(1, revisions.get(0).getRevision(), "重复提交不得造成跳号");
    }

    @Test
    @Scenario("因版本号冲突被拒的提交不写历史、修订号不跳号")
    void conflictingSubmissionsCreateNoRevision() {
        node(4);
        service.createWorkload("wl-rh-3", WorkloadSpec.of(2, "app:v1"));
        tickUntilConverged("wl-rh-3", 20);
        String stale = store.get("wl-rh-3").getResourceVersion();

        service.submit("wl-rh-3", WorkloadSpec.of(3, "app:v1"), stale);
        tickUntilConverged("wl-rh-3", 20);

        assertThrows(ConflictException.class,
                () -> service.submit("wl-rh-3", WorkloadSpec.of(5, "app:v9"), stale));
        assertThrows(ConflictException.class,
                () -> service.rollback("wl-rh-3", null, stale));
        tick(10);

        List<Revision> revisions = service.listRevisions("wl-rh-3");
        assertEquals(2, revisions.size(), "两次冲突被拒都不得入史");
        assertEquals(List.of(1L, 2L), revisions.stream().map(Revision::getRevision).toList());
        assertEquals(3, store.get("wl-rh-3").getSpec().replicas());
        assertEquals("app:v1", store.get("wl-rh-3").getSpec().image());
    }

    @Test
    @Scenario("发布失败的修订终态为 FAILED 且带原因；随后成功提交后失败修订原样保留")
    void failedRevisionKeepsReasonAndHistory() {
        SimulatedNode a = node(4);
        SimulatedNode b = node(4);
        service.createWorkload("wl-rh-4", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-rh-4", 20);

        service.failImageOnNode(a.getInfo().getUid(), "app:v2");
        service.failImageOnNode(b.getInfo().getUid(), "app:v2");
        String rv = store.get("wl-rh-4").getResourceVersion();
        service.submit("wl-rh-4", new WorkloadSpec(3, "app:v2", 1, 2, 2, 1000), rv);

        assertEventually("wl-rh-4", 120,
                w -> w.getUpdatePhase() == UpdatePhase.FAILED, "v2 发布应失败告停");

        Revision failed = service.getRevision("wl-rh-4", 2);
        assertEquals(RevisionStatus.FAILED, failed.getStatus());
        assertNotNull(failed.getReason());
        assertNotNull(failed.getCompletedAt());

        // 人工重新提交 v3：失败修订原样保留，不被改写，新修订继续取号
        String rv2 = store.get("wl-rh-4").getResourceVersion();
        service.submit("wl-rh-4", WorkloadSpec.of(3, "app:v1"), rv2);
        tickUntilConverged("wl-rh-4", 40);

        assertEquals(RevisionStatus.FAILED, service.getRevision("wl-rh-4", 2).getStatus(),
                "已形成的历史终态不得被后续操作改写");
        assertEquals(RevisionStatus.SUCCEEDED, service.getRevision("wl-rh-4", 3).getStatus());
    }

    @Test
    @Scenario("发布未完成就被新提交取代：进行中的修订终态为 SUPERSEDED，原因可解释")
    void inProgressRevisionMarkedSuperseded() {
        node(4);
        service.createWorkload("wl-rh-5", WorkloadSpec.of(2, "app:v1"));
        tickUntilConverged("wl-rh-5", 20);

        String rv = store.get("wl-rh-5").getResourceVersion();
        service.submit("wl-rh-5", WorkloadSpec.of(3, "app:v1"), rv);
        String rvMid = store.get("wl-rh-5").getResourceVersion();
        service.submit("wl-rh-5", WorkloadSpec.of(4, "app:v1"), rvMid); // 上一修订尚 IN_PROGRESS 即被取代
        tickUntilConverged("wl-rh-5", 30);

        assertEquals(RevisionStatus.SUPERSEDED, service.getRevision("wl-rh-5", 2).getStatus());
        assertTrue(service.getRevision("wl-rh-5", 2).getReason().contains("SupersededByUpdate"));
        assertEquals(RevisionStatus.SUCCEEDED, service.getRevision("wl-rh-5", 3).getStatus());
    }

    @Test
    @Scenario("工作负载删除后其修订历史一并消失（历史唯一的抹除途径）")
    void historyOnlyRemovedWithWorkload() {
        node(3);
        service.createWorkload("wl-rh-6", WorkloadSpec.of(2, "app:v1"));
        tickUntilConverged("wl-rh-6", 20);
        assertEquals(1, service.listRevisions("wl-rh-6").size());

        service.delete("wl-rh-6", store.get("wl-rh-6").getResourceVersion());
        tick(20);

        assertThrows(com.github.lechandonga.containercontrolplanejava.store.NotFoundException.class,
                () -> service.listRevisions("wl-rh-6"));
    }
}
