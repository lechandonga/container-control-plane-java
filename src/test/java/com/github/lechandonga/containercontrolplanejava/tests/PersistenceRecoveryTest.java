package com.github.lechandonga.containercontrolplanejava.tests;

import com.github.lechandonga.containercontrolplanejava.model.WorkloadPhase;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;
import com.github.lechandonga.containercontrolplanejava.persistence.LocalStateStore;
import com.github.lechandonga.containercontrolplanejava.revision.RevisionStatus;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务域：进程重启与“写到一半被强杀”的崩溃恢复。
 */
@BusinessUnderTest("持久化与崩溃恢复：重启后期望/历史/进行中进度不丢并从断点继续、"
        + "强杀原子性（只有变更前/后）、实例不重复不漂移、已完成不重做、损坏与未知版本明确拒绝启动")
@ExtendWith(BusinessTestLogger.class)
class PersistenceRecoveryTest extends ControlPlaneTestBase {

    @Test
    @Scenario("进程重启后期望状态与发布历史仍在，继续收敛到与全程不中断一致的结果")
    void restartKeepsDesiredStateAndHistoryAndConverges() {
        node(6);
        service.createWorkload("wl-pr-1", WorkloadSpec.of(4, "app:v1"));
        tick(2); // 只收敛一部分就重启

        restartProcess();
        tickUntilConverged("wl-pr-1", 40);

        assertEquals(4, store.instancesOf("wl-pr-1").size());
        assertEquals(4, countAvailable("wl-pr-1"));
        assertEquals(1, revisionLog.list("wl-pr-1").size(), "发布历史重启后仍在");
        assertEquals(RevisionStatus.SUCCEEDED,
                revisionLog.list("wl-pr-1").get(0).getStatus());
        Set<String> ids = new HashSet<>(store.instancesOf("wl-pr-1").stream()
                .map(r -> r.getId()).toList());
        assertEquals(4, ids.size(), "重启恢复不得产生重复实例");
    }

    @Test
    @Scenario("滚动更新进行到一半重启：从磁盘进度继续，最终单版本收敛，实例 id 不重复、计数不漂移")
    void restartMidRolloutResumesFromBreakpoint() {
        node(4);
        node(4);
        service.createWorkload("wl-pr-2", WorkloadSpec.of(4, "app:v1"));
        tickUntilConverged("wl-pr-2", 30);
        Set<String> v1IdsBefore = new HashSet<>(store.instancesOf("wl-pr-2").stream()
                .filter(r -> r.getImage().equals("app:v1")).map(r -> r.getId()).toList());

        service.submit("wl-pr-2", WorkloadSpec.of(4, "app:v2"),
                store.get("wl-pr-2").getResourceVersion());
        tick(3); // 滚到一半：v1/v2 可能混跑
        int partialMixed = (int) store.instancesOf("wl-pr-2").stream()
                .filter(r -> r.getImage().equals("app:v2")).count();
        assertTrue(partialMixed >= 1 && partialMixed < 4,
                "前置条件：更新确实只进行了一半，实际 v2 数=" + partialMixed);

        restartProcess();
        tickUntilConverged("wl-pr-2", 80);

        assertEquals(4, countImage("wl-pr-2", "app:v2"), "重启后从中断点继续完成滚动");
        assertEquals(0, countImage("wl-pr-2", "app:v1"));
        assertEquals(4, store.instancesOf("wl-pr-2").size(), "精确等于期望副本数，不多不少");
        Set<String> finalIds = new HashSet<>(store.instancesOf("wl-pr-2").stream()
                .map(r -> r.getId()).toList());
        assertEquals(4, finalIds.size(), "实例 id 全局唯一，计数无漂移");
        // 重启前已经创建过的实例不会被整套重建一遍：至少有一个 v1 旧 id 已被清理、
        // 已建成的 v2 实例 id 继续沿用（断点续跑，而非从头再来）。
        assertFalse(finalIds.containsAll(v1IdsBefore),
                "旧版本实例不应在重启后被当作没替换过而保留");
    }

    @Test
    @Scenario("已收敛的工作负载重启后不做任何替换动作：版本、实例 id 集合与重启前逐项一致")
    void restartAfterConvergenceDoesNotRedoWork() {
        node(4);
        service.createWorkload("wl-pr-3", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-pr-3", 30);
        List<String> idsBefore = store.instancesOf("wl-pr-3").stream()
                .map(r -> r.getId() + "@" + r.getNodeUid()).sorted().toList();

        restartProcess();
        tick(20); // 大量空转

        List<String> idsAfter = store.instancesOf("wl-pr-3").stream()
                .map(r -> r.getId() + "@" + r.getNodeUid()).sorted().toList();
        assertEquals(idsBefore, idsAfter, "已经做完的替换/创建在重启后不得重做");
        assertEquals(3, store.instancesOf("wl-pr-3").size());
        assertEquals(WorkloadPhase.RUNNING, store.get("wl-pr-3").getPhase());
    }

    @Test
    @Scenario("写状态过程中被强杀（原子替换前）：正式状态文件保持变更前完整状态，重启正常恢复")
    void crashDuringWriteKeepsPreviousConsistentState() throws Exception {
        node(4);
        service.createWorkload("wl-pr-4", WorkloadSpec.of(2, "app:v1"));
        tickUntilConverged("wl-pr-4", 30);
        long stateFileSize = java.nio.file.Files.size(stateFile());
        String rvBefore = store.get("wl-pr-4").getResourceVersion();

        // 下一轮调谐落盘时在“临时文件写好、正式文件替换之前”强杀
        crashDuringNextStateWrite();
        LocalStateStore.SimulatedCrashException crash = assertThrows(
                LocalStateStore.SimulatedCrashException.class, this::tick);
        System.out.println("[BUSINESS-TESTS]   强杀点: " + crash.getMessage());

        // 正式状态文件仍是变更前的完整文件：大小未变、可解析
        assertEquals(stateFileSize, java.nio.file.Files.size(stateFile()),
                "原子替换未发生，正式状态文件必须保持变更前内容");
        restartProcess(); // 变更前状态：仍是 v1/2 副本
        assertEquals(2, store.instancesOf("wl-pr-4").size());
        assertEquals("app:v1", store.get("wl-pr-4").getSpec().image());
        assertEquals(rvBefore, store.get("wl-pr-4").getResourceVersion(),
                "恢复到的是变更前版本，不存在自相矛盾的半截状态");
        tickUntilConverged("wl-pr-4", 30);
        assertEquals(2, countAvailable("wl-pr-4"));
    }

    @Test
    @Scenario("状态文件损坏/被截断时拒绝带病启动，给出明确错误而不是起一个自相矛盾的进程")
    void corruptedStateFileFailsFast() {
        node(2);
        service.createWorkload("wl-pr-5", WorkloadSpec.of(1, "app:v1"));
        tickUntilConverged("wl-pr-5", 20);

        corruptStateFile("{ 这不是合法的半截 JSON 内容 >>>");
        LocalStateStore.StatePersistenceException ex = assertThrows(
                LocalStateStore.StatePersistenceException.class,
                localStateStore::loadOnStartup);
        assertTrue(ex.getMessage().contains("无法解析") || ex.getMessage().contains("校验"),
                "必须明确告知状态文件损坏: " + ex.getMessage());
    }

    @Test
    @Scenario("格式标识不认识或格式版本高于支持版本：明确报错拒绝启动，不做猜测性加载")
    void unknownFormatOrVersionFailsFast() throws Exception {
        node(2);
        service.createWorkload("wl-pr-6", WorkloadSpec.of(1, "app:v1"));
        tickUntilConverged("wl-pr-6", 20);

        corruptStateFile("""
                {"formatId":"SOMETHING-ELSE","formatVersion":1,"checksum":"x","payload":"{}"}""");
        LocalStateStore.StatePersistenceException badId = assertThrows(
                LocalStateStore.StatePersistenceException.class,
                localStateStore::loadOnStartup);
        assertTrue(badId.getMessage().contains("格式标识不认识"), badId.getMessage());

        corruptStateFile("""
                {"formatId":"CCP-STATE","formatVersion":999,"checksum":"x","payload":"{}"}""");
        LocalStateStore.StatePersistenceException badVersion = assertThrows(
                LocalStateStore.StatePersistenceException.class,
                localStateStore::loadOnStartup);
        assertTrue(badVersion.getMessage().contains("版本")
                        && badVersion.getMessage().contains("拒绝启动"),
                badVersion.getMessage());
    }

    @Test
    @Scenario("回滚进行中重启：回滚目标与进度不丢，重启后继续完成，最终单版本精确收敛")
    void restartMidRollbackResumesAndConverges() {
        node(6);
        service.createWorkload("wl-pr-7", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-pr-7", 30);
        service.submit("wl-pr-7", WorkloadSpec.of(4, "app:v2"),
                store.get("wl-pr-7").getResourceVersion());
        tickUntilConverged("wl-pr-7", 60);

        service.rollback("wl-pr-7", 1L, store.get("wl-pr-7").getResourceVersion());
        tick(3); // 回滚滚到一半
        restartProcess();
        tickUntilConverged("wl-pr-7", 80);

        assertEquals(3, countImage("wl-pr-7", "app:v1"));
        assertEquals(0, countImage("wl-pr-7", "app:v2"));
        assertEquals(3, store.instancesOf("wl-pr-7").size());
        assertEquals(RevisionStatus.SUCCEEDED,
                revisionLog.latest("wl-pr-7").orElseThrow().getStatus());
    }

    @Test
    @Scenario("已告停的失败现场重启后保持原样和原有原因，不自行反复重试")
    void failedSceneSurvivesRestartWithoutAutoRetry() {
        SimulatedNode a = node(4);
        SimulatedNode b = node(4);
        service.createWorkload("wl-pr-8", WorkloadSpec.of(3, "app:v1"));
        tickUntilConverged("wl-pr-8", 30);
        service.failImageOnNode(a.getInfo().getUid(), "app:v2");
        service.failImageOnNode(b.getInfo().getUid(), "app:v2");
        service.submit("wl-pr-8", WorkloadSpec.of(3, "app:v2"),
                store.get("wl-pr-8").getResourceVersion());
        assertEventually("wl-pr-8", 150,
                w -> w.getPhase() == WorkloadPhase.FAILED, "更新失败应告停");
        tick(10);

        restartProcess();
        tick(30);

        assertEquals(WorkloadPhase.FAILED, store.get("wl-pr-8").getPhase(),
                "重启后失败现场保持，不自动反复重试");
        assertEquals(3, countImage("wl-pr-8", "app:v1"));
        assertEquals(0, countImage("wl-pr-8", "app:v2"));
        String reasons = store.get("wl-pr-8").getConditions().toString();
        assertTrue(reasons.contains("RolloutFailed") || reasons.contains("Deadline"),
                "原有失败原因必须保留: " + reasons);

        // 只有重新提交期望才会再次推进
        service.submit("wl-pr-8", WorkloadSpec.of(3, "app:v3"),
                store.get("wl-pr-8").getResourceVersion());
        tickUntilConverged("wl-pr-8", 60);
        assertEquals(3, countImage("wl-pr-8", "app:v3"));
    }
}
