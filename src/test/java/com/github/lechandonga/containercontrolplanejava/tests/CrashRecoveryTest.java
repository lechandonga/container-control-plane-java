package com.github.lechandonga.containercontrolplanejava.tests;

import com.github.lechandonga.containercontrolplanejava.model.Revision;
import com.github.lechandonga.containercontrolplanejava.model.RevisionStatus;
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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务域：进程重启与崩溃恢复。不依赖任何外部服务：同一台机器重新拉起进程后，
 * 期望状态、发布历史、进行中的更新/回滚进度都在，并从中断处继续朝目标收敛；
 * 写盘中途强杀后重启只能恢复到变更前或变更后，不自相矛盾。
 */
@BusinessUnderTest("重启恢复：期望状态/历史/进行中进度本地持久化、中途重启幂等续跑、半写崩溃前后一致、损坏隔离")
@ExtendWith(BusinessTestLogger.class)
class CrashRecoveryTest extends ControlPlaneTestBase {

    @Test
    @Scenario("收敛后重启进程：期望状态与全部实例完整恢复，多轮空转不产生重复或漂移")
    void restartAfterConvergenceRestoresEverything() {
        node(4);
        node(4);
        service.createWorkload("wl-cr-1", WorkloadSpec.of(5, "app:v1"));
        tickUntilConverged("wl-cr-1", 30);
        List<String> beforeIds = store.instancesOf("wl-cr-1").stream()
                .map(r -> r.getId() + "@" + r.getNodeUid()).sorted().toList();
        String rv = store.get("wl-cr-1").getResourceVersion();

        restartProcess();

        assertEquals(5, store.instancesOf("wl-cr-1").size());
        assertEquals(beforeIds, store.instancesOf("wl-cr-1").stream()
                .map(r -> r.getId() + "@" + r.getNodeUid()).sorted().toList(),
                "重启后实例 id 与落点保持，不重建、不重复");
        assertEquals(WorkloadPhase.RUNNING, store.get("wl-cr-1").getPhase());
        assertEquals(rv, store.get("wl-cr-1").getResourceVersion());
        tick(20);
        assertEquals(5, store.instancesOf("wl-cr-1").size(), "恢复后空转零漂移");
        assertEquals(5, countAvailable("wl-cr-1"));
    }

    @Test
    @Scenario("滚动更新跑到一半重启：历史与进度都在，重启后继续完成替换，不重做已完成步骤、单版本收敛")
    void restartMidRolloutContinuesFromWhereItStopped() {
        node(4);
        node(4);
        service.createWorkload("wl-cr-2", WorkloadSpec.of(4, "app:v1"));
        tickUntilConverged("wl-cr-2", 30);
        String rv = store.get("wl-cr-2").getResourceVersion();
        service.submit("wl-cr-2", WorkloadSpec.of(4, "app:v2"), rv);
        tick(2); // 只推进两步：v1/v2 混跑的“进行中”现场

        int midV1 = (int) countImage("wl-cr-2", "app:v1");
        int midV2 = (int) countImage("wl-cr-2", "app:v2");
        assertTrue(midV1 > 0 && midV2 > 0, "前置条件：重启前确实在混跑");
        long seqBefore = store.get("wl-cr-2").getNextInstanceSeq();

        restartProcess();
        assertEquals(UpdatePhase.ROLLING, store.get("wl-cr-2").getUpdatePhase(),
                "进行中的更新进度必须恢复，而不是重头再来或丢失");

        tickUntilConverged("wl-cr-2", 60);
        assertEquals(4, countImage("wl-cr-2", "app:v2"));
        assertEquals(0, countImage("wl-cr-2", "app:v1"));
        assertEquals(4, store.instancesOf("wl-cr-2").size());
        // 重启前已创建的新版本实例不会被重做：实例序号单调、无重复 id
        long reused = store.instancesOf("wl-cr-2").stream()
                .filter(r -> r.getImage().equals("app:v2")).count();
        assertEquals(4, reused);
        assertTrue(store.get("wl-cr-2").getNextInstanceSeq() >= seqBefore);
        assertEquals(2, service.listRevisions("wl-cr-2").size());
        assertEquals(RevisionStatus.SUCCEEDED, service.getRevision("wl-cr-2", 2).getStatus());
    }

    @Test
    @Scenario("发布历史跨重启完整保留：修订序号、前后声明、终态全部可查")
    void revisionHistorySurvivesRestart() {
        node(5);
        service.createWorkload("wl-cr-3", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-cr-3", 20);
        String rv = store.get("wl-cr-3").getResourceVersion();
        service.submit("wl-cr-3", WorkloadSpec.of(3, "app:v2"), rv);
        tickUntilConverged("wl-cr-3", 40);

        restartProcess();

        List<Revision> revisions = service.listRevisions("wl-cr-3");
        assertEquals(2, revisions.size());
        assertEquals(List.of(1L, 2L), revisions.stream().map(Revision::getRevision).toList());
        assertEquals("app:v1", revisions.get(0).getAfter().image());
        assertEquals("app:v1", revisions.get(1).getBefore().image());
        assertEquals("app:v2", revisions.get(1).getAfter().image());
        assertEquals(RevisionStatus.SUCCEEDED, revisions.get(1).getStatus());
    }

    @Test
    @Scenario("写状态到一半被强杀：重启后要么是变更前要么是变更后，状态自洽且能继续收敛")
    void killDuringWriteRecoversToBeforeOrAfter() {
        node(4);
        service.createWorkload("wl-cr-4", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-cr-4", 20);

        String rv = store.get("wl-cr-4").getResourceVersion();
        service.submit("wl-cr-4", WorkloadSpec.of(4, "app:v1"), rv);
        // 在调谐落盘的瞬间“强杀”：只有半截 state.tmp，state.json 停留在某个完整快照
        armKillDuringWrite();
        try {
            tick();
        } catch (IllegalStateException expected) {
            assertEquals("SIMULATED_KILL_DURING_WRITE", expected.getMessage());
        }

        restartProcess(); // 模拟拉起新进程

        Workload wl = store.get("wl-cr-4");
        // 磁盘快照必然自洽：要么提交尚未落盘（gen=2 前），要么已落盘（gen=2 后）。
        assertTrue(wl.getGeneration() == 1L || wl.getGeneration() == 2L,
                "只允许恢复到变更前或变更后，实际 generation=" + wl.getGeneration());
        // 无论恢复到哪一侧，继续调谐最终都与“全程不中断跑完”一致：4 副本 v1 单版本
        if (wl.getGeneration() == 1L) {
            // 恢复到变更前：提交丢失（客户端会看到旧版本号后按 read-modify-write 重提）
            String latest = wl.getResourceVersion();
            service.submit("wl-cr-4", WorkloadSpec.of(4, "app:v1"), latest);
        }
        tickUntilConverged("wl-cr-4", 30);
        assertEquals(4, store.instancesOf("wl-cr-4").size(),
                "半写恢复后实例数不得自相矛盾，继续收敛结果与不中断一致");
        assertEquals(4, countAvailable("wl-cr-4"));
    }

    @Test
    @Scenario("已告停的失败现场重启后保持原样与原因，不会自己反复重试")
    void failedTerminalSurvivesRestartWithoutAutoRetry() {
        SimulatedNode a = node(4);
        SimulatedNode b = node(4);
        service.createWorkload("wl-cr-5", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-cr-5", 20);
        service.failImageOnNode(a.getInfo().getUid(), "app:v2");
        service.failImageOnNode(b.getInfo().getUid(), "app:v2");
        String rv = store.get("wl-cr-5").getResourceVersion();
        service.submit("wl-cr-5", new WorkloadSpec(3, "app:v2", 1, 2, 2, 1000), rv);
        assertEventually("wl-cr-5", 120,
                w -> w.getUpdatePhase() == UpdatePhase.FAILED, "v2 应失败告停");
        tick(10);

        restartProcess();
        assertEquals(UpdatePhase.FAILED, store.get("wl-cr-5").getUpdatePhase());
        assertEquals(WorkloadPhase.FAILED, store.get("wl-cr-5").getPhase());
        assertEquals(RevisionStatus.FAILED, service.getRevision("wl-cr-5", 2).getStatus());
        String conditionsBefore = store.get("wl-cr-5").getConditions().toString();
        assertTrue(conditionsBefore.contains("RolloutFailed"));

        tick(30); // 长时间空转
        assertEquals(UpdatePhase.FAILED, store.get("wl-cr-5").getUpdatePhase(),
                "失败现场不得在重启后自行重试");
        assertEquals(3, countImage("wl-cr-5", "app:v1"));
        assertEquals(0, countImage("wl-cr-5", "app:v2"));
    }

    @Test
    @Scenario("显式回滚进行到一半重启：回滚修订与进度恢复，重启后继续直到单版本精确收敛")
    void restartMidRollbackContinuesAndConverges() {
        node(5);
        service.createWorkload("wl-cr-6", WorkloadSpec.of(4, "app:v1"));
        tickUntilConverged("wl-cr-6", 30);
        String rv1 = store.get("wl-cr-6").getResourceVersion();
        service.submit("wl-cr-6", WorkloadSpec.of(4, "app:v2"), rv1);
        tickUntilConverged("wl-cr-6", 50);
        String rv2 = store.get("wl-cr-6").getResourceVersion();
        service.rollback("wl-cr-6", 1L, rv2);
        tick(2);

        restartProcess();
        Revision head = store.get("wl-cr-6").headRevision();
        assertEquals(3L, head.getRevision());
        assertEquals(1L, head.getTargetRevision());

        tickUntilConverged("wl-cr-6", 60);
        assertEquals(4, countImage("wl-cr-6", "app:v1"));
        assertEquals(0, countImage("wl-cr-6", "app:v2"));
        assertEquals(4, store.instancesOf("wl-cr-6").size());
        assertEquals(RevisionStatus.SUCCEEDED,
                service.getRevision("wl-cr-6", 3).getStatus());
        assertEquals(RevisionStatus.ROLLED_BACK,
                service.getRevision("wl-cr-6", 2).getStatus());
    }

    @Test
    @Scenario("状态文件损坏/格式不认识：问题文件被隔离备份，控制面以空状态正常启动，不带着矛盾状态运行")
    void corruptStateFileIsQuarantinedAndStartsClean() {
        node(2);
        service.createWorkload("wl-cr-7", WorkloadSpec.of(1, "app:v1"));
        tickUntilConverged("wl-cr-7", 10);

        localStateStore.corruptStateFileForTests("{ this is not valid json ][");
        restartProcess();

        assertTrue(store.list().isEmpty(), "损坏文件必须被拒绝，不得解析出半个状态");
        boolean backupExists = java.util.stream.Stream
                .of(localStateStore.getStateFile().getParent().toFile().listFiles())
                .anyMatch(f -> f.getName().startsWith("state.corrupt-"));
        assertTrue(backupExists, "损坏文件应被隔离备份为 state.corrupt-*");
        assertTrue(eventLog.all().stream()
                .anyMatch(e -> "CorruptStateQuarantined".equals(e.reason())),
                "应记录损坏隔离的 RECOVERY 告警事件");

        // 隔离后以空状态继续工作：新提交正常落盘
        node(3);
        service.createWorkload("wl-cr-7-fresh", WorkloadSpec.of(2, "app:v9"));
        tickUntilConverged("wl-cr-7-fresh", 20);
        restartProcess();
        assertEquals(2, store.instancesOf("wl-cr-7-fresh").size());
    }
}
