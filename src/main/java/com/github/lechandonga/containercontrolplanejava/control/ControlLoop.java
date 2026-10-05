package com.github.lechandonga.containercontrolplanejava.control;

import com.github.lechandonga.containercontrolplanejava.event.EventLog;
import com.github.lechandonga.containercontrolplanejava.node.NodeRegistry;
import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;
import com.github.lechandonga.containercontrolplanejava.reconcile.Reconciler;
import com.github.lechandonga.containercontrolplanejava.store.WorkloadStore;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 控制循环（controller manager 的单机实现）：
 * <ol>
 *   <li>周期性做节点故障检测，发现失联立即 fence 并对受影响工作负载触发调谐；</li>
 *   <li>周期性对所有工作负载做声明式调谐（level-triggered，而非仅依赖事件），
 *       因此“重复事件 / 调谐过程中重启 / 漏事件”都不会影响最终收敛。</li>
 * </ol>
 */
@Component
public class ControlLoop {

    private static final Logger log = LoggerFactory.getLogger(ControlLoop.class);

    private final NodeRegistry nodeRegistry;
    private final WorkloadStore store;
    private final Reconciler reconciler;
    private final EventLog eventLog;
    private final ScheduledExecutorService executor;
    private final long intervalMs;

    public ControlLoop(NodeRegistry nodeRegistry, WorkloadStore store, Reconciler reconciler,
                       EventLog eventLog,
                       com.github.lechandonga.containercontrolplanejava.config.ControlPlaneProperties props) {
        this.nodeRegistry = nodeRegistry;
        this.store = store;
        this.reconciler = reconciler;
        this.eventLog = eventLog;
        this.intervalMs = props.reconcileIntervalMs();
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "control-loop");
            t.setDaemon(true);
            return t;
        });
        this.executor.scheduleWithFixedDelay(this::tickSafely,
                intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void tickSafely() {
        try {
            tick();
        } catch (Throwable t) {
            log.error("[CONTROL-LOOP] 调谐轮次异常（下一轮继续，不影响最终收敛）", t);
        }
    }

    /** 单轮控制循环：先检测故障，再全量调谐。对测试暴露以便确定性驱动。 */
    public void tick() {
        List<SimulatedNode> lostNodes = nodeRegistry.detectFailures();
        for (SimulatedNode node : lostNodes) {
            log.warn("[NODE-FAILURE] 节点 {} 心跳超时，已 fence 为 UNREACHABLE",
                    node.getInfo().getUid());
            eventLog.record(EventLog.BUSINESS_NODE_FAILURE, "-", "NodeFenced",
                    "节点 " + node.getInfo().getUid() + " 心跳超时被隔离，其上实例将被重建");
            // 失联节点上的所有工作负载立即调谐，不必等待下一次全量扫描。
            for (var instance : store.instancesOnNode(node.getInfo().getUid())) {
                reconciler.reconcile(instance.getWorkloadUid());
            }
        }
        reconciler.reconcileAll();
    }

    /** 模拟“控制面重启”：不依赖任何历史事件，直接从存储重建全部调谐动作。 */
    public void restartReconciliation() {
        log.info("[CONTROL-LOOP] 模拟控制面重启：从权威存储重新调谐全部工作负载");
        reconciler.reconcileAll();
    }

    @PreDestroy
    public void stop() {
        executor.shutdownNow();
    }
}
