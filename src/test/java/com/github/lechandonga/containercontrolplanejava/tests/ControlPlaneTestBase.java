package com.github.lechandonga.containercontrolplanejava.tests;

import com.github.lechandonga.containercontrolplanejava.api.ControlPlaneService;
import com.github.lechandonga.containercontrolplanejava.event.EventLog;
import com.github.lechandonga.containercontrolplanejava.persistence.LocalStateStore;
import com.github.lechandonga.containercontrolplanejava.model.InstanceState;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadPhase;
import com.github.lechandonga.containercontrolplanejava.revision.RevisionLog;
import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;
import com.github.lechandonga.containercontrolplanejava.reconcile.Reconciler;
import com.github.lechandonga.containercontrolplanejava.scheduler.Scheduler;
import com.github.lechandonga.containercontrolplanejava.store.WorkloadStore;
import com.github.lechandonga.containercontrolplanejava.time.ClockService;
import com.github.lechandonga.containercontrolplanejava.node.NodeRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.nio.file.Path;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 测试基类：
 * <ul>
 *   <li>控制循环/心跳间隔调得极大，测试完全通过 {@code tick()} 确定性驱动，杜绝时序抖动；</li>
 *   <li>心跳超时 100 秒，测试用 {@link #advanceSeconds} 精确推进虚拟时钟制造“失联/超时”。</li>
 * </ul>
 */
@SpringBootTest
@TestPropertySource(properties = {
        "controlplane.reconcile-interval-ms=3600000",
        "controlplane.heartbeat-interval-ms=3600000",
        "controlplane.heartbeat-timeout-seconds=100",
        "controlplane.state-dir=target/test-control-plane-state"
})
public abstract class ControlPlaneTestBase {

    protected static final Instant T0 = Instant.parse("2026-10-05T00:00:00Z");

    @Autowired protected ControlPlaneService service;
    @Autowired protected WorkloadStore store;
    @Autowired protected NodeRegistry nodeRegistry;
    @Autowired protected Scheduler scheduler;
    @Autowired protected Reconciler reconciler;
    @Autowired protected EventLog eventLog;
    @Autowired protected ClockService clockService;
    @Autowired protected RevisionLog revisionLog;
    @Autowired protected LocalStateStore localStateStore;

    @BeforeEach
    void resetWorld() {
        store.resetForTests();
        nodeRegistry.resetForTests();
        eventLog.resetForTests();
        revisionLog.resetForTests();
        localStateStore.deleteStateFileForTests();
        clockService.setClock(Clock.fixed(T0, ZoneOffset.UTC));
    }

    /** 模拟整个控制面进程重启：从磁盘状态文件恢复权威状态后继续调谐。 */
    protected void restartProcess() {
        service.restartControlPlane();
    }

    /** 模拟“写到一半被强杀”：下次状态落盘在原子替换前中断，正式状态文件保持变更前内容。 */
    protected void crashDuringNextStateWrite() {
        localStateStore.injectCrashBeforeAtomicSwap();
    }

    protected Path stateFile() {
        return localStateStore.stateFileForTests();
    }

    /** 直接破坏正式状态文件内容，模拟磁盘损坏/被截断。 */
    protected void corruptStateFile(String garbage) {
        localStateStore.corruptStateFileForTests(garbage);
    }

    protected SimulatedNode node(int capacity) {
        SimulatedNode n = service.registerNode("host-" + Math.abs(System.nanoTime()), capacity);
        n.getInfo().setLastHeartbeat(clockService.now());
        return n;
    }

    protected void advanceSeconds(long seconds) {
        Instant next = clockService.now().plusSeconds(seconds);
        clockService.setClock(Clock.fixed(next, ZoneOffset.UTC));
        // 模拟时间流逝期间，健康节点持续在发心跳（仅被分区的节点发不出来）。
        nodeRegistry.all().forEach(n -> nodeRegistry.heartbeat(n.getInfo().getUid()));
    }

    protected void tick() {
        service.drive();
    }

    protected void tick(int times) {
        for (int i = 0; i < times; i++) {
            tick();
        }
    }

    protected void tickUntilConverged(String uid, int maxTicks) {
        for (int i = 0; i < maxTicks; i++) {
            tick();
            Workload wl = store.find(uid).orElse(null);
            if (wl == null) {
                return;
            }
            if (wl.getPhase() == WorkloadPhase.RUNNING
                    && wl.getReadyReplicas() == wl.getSpec().replicas()) {
                return;
            }
        }
        throw new AssertionError("工作负载 " + uid + " 在 " + maxTicks + " 轮内未收敛: "
                + describe(uid));
    }

    protected long countByState(String uid, InstanceState state) {
        return store.instancesOf(uid).stream().filter(r -> r.getState() == state).count();
    }

    protected long countAvailable(String uid) {
        return store.instancesOf(uid).stream()
                .filter(r -> r.isAvailable(this::isHealthy)).count();
    }

    protected long countImage(String uid, String image) {
        return store.instancesOf(uid).stream()
                .filter(r -> r.getImage().equals(image)
                        && r.getState() == InstanceState.RUNNING
                        && isHealthy(r.getNodeUid()))
                .count();
    }

    private boolean isHealthy(String nodeUid) {
        return nodeRegistry.get(nodeUid)
                .map(n -> n.getInfo().getState().name().equals("HEALTHY") && !n.isPartitioned())
                .orElse(false);
    }

    protected String describe(String uid) {
        Workload wl = store.find(uid).orElse(null);
        if (wl == null) {
            return uid + "=<purged>";
        }
        return uid + "{phase=" + wl.getPhase()
                + ", update=" + wl.getUpdatePhase()
                + ", ready=" + wl.getReadyReplicas()
                + "/" + wl.getSpec().replicas()
                + ", image=" + wl.getSpec().image()
                + ", instances=" + store.instancesOf(uid).stream()
                        .map(r -> r.getId() + ":" + r.getState() + "@" + r.getNodeUid()
                                + "(" + r.getImage() + ")").toList()
                + "}";
    }

    protected void assertEventually(String uid, int maxTicks, Predicate<Workload> condition,
                                   String message) {
        for (int i = 0; i < maxTicks; i++) {
            tick();
            Workload wl = store.find(uid).orElse(null);
            if (wl != null && condition.test(wl)) {
                return;
            }
        }
        assertTrue(false, message + " | 最终状态: " + describe(uid));
    }
}
