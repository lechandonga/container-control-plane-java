package com.github.lechandonga.containercontrolplanejava.reconcile;

import com.github.lechandonga.containercontrolplanejava.event.EventLog;
import com.github.lechandonga.containercontrolplanejava.model.Condition;
import com.github.lechandonga.containercontrolplanejava.model.InstanceRecord;
import com.github.lechandonga.containercontrolplanejava.model.InstanceState;
import com.github.lechandonga.containercontrolplanejava.model.NodeState;
import com.github.lechandonga.containercontrolplanejava.model.UpdatePhase;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadPhase;
import com.github.lechandonga.containercontrolplanejava.node.NodeRegistry;
import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;
import com.github.lechandonga.containercontrolplanejava.scheduler.Scheduler;
import com.github.lechandonga.containercontrolplanejava.scheduler.SchedulingDecision;
import com.github.lechandonga.containercontrolplanejava.store.WorkloadStore;
import com.github.lechandonga.containercontrolplanejava.time.ClockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 调谐器：把“期望状态”持续收敛为“实际状态”的核心循环体。
 *
 * <p>一次 {@link #reconcile} 调用就是一次有界、确定、幂等的调谐事务：
 * <ol>
 *   <li>对单个工作负载加互斥锁，观察-决策-执行不会与其它调谐/提交交错。</li>
 *   <li>以存储中的权威记录 + 节点真实容器视图为输入，不信任过期缓存。</li>
 *   <li>每轮至多执行一个变更动作（建/删/驱逐），再由控制循环再次驱动，
 *       滚动更新因此严格遵循 surge/可用性下限，且每一步都留事件、可解释。</li>
 * </ol>
 */
@Component
public class Reconciler {

    private static final Logger log = LoggerFactory.getLogger(Reconciler.class);

    public static final String REASON_EVICTED = "NodeLost";
    public static final String REASON_REBUILT = "RebuiltAfterNodeLoss";
    public static final String REASON_SCALE = "ReplicaScaling";
    public static final String REASON_ROLLOUT_OK = "RolloutSucceeded";
    public static final String REASON_ROLLOUT_FAILED = "RolloutFailed";
    public static final String REASON_DEADLINE = "RolloutDeadlineExceeded";
    public static final String REASON_CAPACITY = "Unschedulable";
    public static final String REASON_CONVERGED = "Converged";

    private final WorkloadStore store;
    private final NodeRegistry nodeRegistry;
    private final Scheduler scheduler;
    private final ClockService clock;
    private final EventLog eventLog;

    public Reconciler(WorkloadStore store, NodeRegistry nodeRegistry, Scheduler scheduler,
                      ClockService clock, EventLog eventLog) {
        this.store = store;
        this.nodeRegistry = nodeRegistry;
        this.scheduler = scheduler;
        this.clock = clock;
        this.eventLog = eventLog;
    }

    private boolean isRunningHealthy(InstanceRecord r) {
        return r.getState() == InstanceState.RUNNING && isHealthyNode(r.getNodeUid());
    }

    private boolean isHealthyNode(String nodeUid) {
        return nodeRegistry.get(nodeUid)
                .map(n -> n.getInfo().getState() == NodeState.HEALTHY && !n.isPartitioned())
                .orElse(false);
    }

    /** 对单个工作负载执行一轮调谐；事件/循环/提交可任意重复调用，结果幂等。 */
    public void reconcile(String workloadUid) {
        synchronized (store.lockFor(workloadUid)) {
            Workload workload = store.find(workloadUid).orElse(null);
            if (workload != null) {
                reconcileLocked(workload);
            }
        }
    }

    /** 控制循环重启后对全部工作负载重新调谐，恢复所有收敛动作。 */
    public void reconcileAll() {
        for (Workload workload : store.list()) {
            reconcile(workload.getUid());
        }
    }

    private void reconcileLocked(Workload wl) {
        // 1) 以节点真实视图 fence：失联节点实例标 LOST，健康节点上消失的容器也标 LOST。
        fenceLostNodes(wl, store.instancesOf(wl.getUid()));

        List<InstanceRecord> records = store.instancesOf(wl.getUid());

        // 2) 删除路径：逐实例幂等清空，再物理移除聚合。
        if (wl.getPhase() == WorkloadPhase.DELETING) {
            handleDeleting(wl, records);
            return;
        }

        // 3) 观察到新 generation：只有“镜像版本确实发生变化”才开启滚动更新；
        //    纯副本数变化（扩缩容）直接采纳 generation 并走稳态伸缩，不做无谓的实例替换。
        if (wl.getObservedGeneration() != wl.getGeneration()
                && wl.getUpdatePhase() != UpdatePhase.ROLLING) {
            boolean imageMismatch = store.instancesOf(wl.getUid()).stream()
                    .anyMatch(r -> r.getState() != InstanceState.LOST
                            && !r.getImage().equals(wl.getSpec().image()));
            if (imageMismatch) {
                beginRollout(wl);
            } else {
                wl.setObservedGeneration(wl.getGeneration());
            }
        }

        // 4) 自愈优先：LOST / START_FAILED 先处理。每轮至多一个动作。
        if (recoverFailures(wl, store.instancesOf(wl.getUid()))) {
            refreshStatus(wl);
            store.bumpResourceVersion(wl);
            return;
        }

        // 5) 已告停的更新（FAILED 终态）：停止推进新版本，仅保留上面的稳态自愈。
        if (wl.getUpdatePhase() == UpdatePhase.FAILED
                && wl.getObservedGeneration() == wl.getGeneration()) {
            wl.setPhase(WorkloadPhase.FAILED);
            refreshStatus(wl);
            store.bumpResourceVersion(wl);
            return;
        }

        // 6) 失败回退阶段：把实际状态恢复到上一个良好版本并补齐副本数。
        if (wl.getRollbackTargetImage() != null) {
            if (handleRollback(wl, store.instancesOf(wl.getUid()))) {
                refreshStatus(wl);
                store.bumpResourceVersion(wl);
            }
            return;
        }

        // 7) 滚动更新。
        if (wl.getUpdatePhase() == UpdatePhase.ROLLING) {
            handleRollout(wl, store.instancesOf(wl.getUid()));
            refreshStatus(wl);
            store.bumpResourceVersion(wl);
            return;
        }

        // 7) 稳态扩缩容。
        if (handleSteadyScaling(wl, store.instancesOf(wl.getUid()))) {
            refreshStatus(wl);
            store.bumpResourceVersion(wl);
            return;
        }

        // 8) 收敛。
        refreshStatus(wl);
        if (wl.getPhase() != WorkloadPhase.RUNNING) {
            wl.setPhase(WorkloadPhase.RUNNING);
            wl.addCondition(Condition.of("Ready", true, REASON_CONVERGED,
                    "实际状态已收敛到期望状态: replicas=" + wl.getSpec().replicas()
                            + " image=" + wl.getSpec().image(), wl.getGeneration()));
            eventLog.record(EventLog.BUSINESS_RECONCILE, wl.getUid(), REASON_CONVERGED,
                    "期望状态达成，replicas=" + wl.getSpec().replicas()
                            + " image=" + wl.getSpec().image());
            store.bumpResourceVersion(wl);
        }
    }

    private void beginRollout(Workload wl) {
        wl.setObservedGeneration(wl.getGeneration());
        wl.setUpdatePhase(UpdatePhase.ROLLING);
        wl.setPhase(WorkloadPhase.PROGRESSING);
        wl.setRolloutAttempts(0);
        wl.setRolloutDeadline(clock.now().plusSeconds(wl.getSpec().rolloutDeadlineSeconds()));
        wl.clearRollbackTarget();
        wl.setRolloutFailureReason(null);
        wl.addCondition(Condition.of("Rollout", false, "RolloutStarted",
                "开始向版本 " + wl.getSpec().image() + " (generation=" + wl.getGeneration()
                        + ") 滚动更新，maxSurge=" + wl.getSpec().maxSurge()
                        + "，minAvailable=" + wl.getSpec().minAvailable(),
                wl.getGeneration()));
        eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, wl.getUid(), "RolloutStarted",
                "generation=" + wl.getGeneration() + " image=" + wl.getSpec().image());
    }

    /**
     * 节点故障 fence：UNREACHABLE 节点的实例标 LOST；节点健康但容器消失同样标 LOST。
     * 记录是唯一权威，节点恢复后的残留容器由 wipeRecoveredNode 清理，杜绝脑裂双实例。
     */
    private void fenceLostNodes(Workload wl, List<InstanceRecord> records) {
        for (InstanceRecord record : records) {
            SimulatedNode node = nodeRegistry.get(record.getNodeUid()).orElse(null);
            boolean healthy = node != null && node.getInfo().getState() == NodeState.HEALTHY
                    && !node.isPartitioned();
            if (!healthy && record.getState() != InstanceState.LOST) {
                record.setState(InstanceState.LOST);
                wl.addCondition(Condition.of("Available", false, REASON_EVICTED,
                        "节点 " + record.getNodeUid() + " 失联，实例 " + record.getId()
                                + " 被 fence 为 LOST 并将在健康节点重建", wl.getGeneration()));
                eventLog.record(EventLog.BUSINESS_NODE_FAILURE, wl.getUid(), REASON_EVICTED,
                        "node=" + record.getNodeUid() + " instance=" + record.getId()
                                + " image=" + record.getImage());
            } else if (healthy && record.getState() == InstanceState.RUNNING
                    && !node.isContainerRunning(record.getId())) {
                record.setState(InstanceState.LOST);
                log.warn("[NODE-DRIFT] workload={} node={} instance={} 容器在健康节点上消失，标记 LOST",
                        wl.getUid(), node.getInfo().getUid(), record.getId());
                eventLog.record(EventLog.BUSINESS_RECONCILE, wl.getUid(), "ContainerVanished",
                        "健康节点 " + node.getInfo().getUid() + " 上实例 " + record.getId()
                                + " 消失，将重建");
            }
        }
    }

    /** 处理 LOST（重建）与 START_FAILED（摘除重投，滚动更新受预算约束）。每轮至多一个动作。 */
    private boolean recoverFailures(Workload wl, List<InstanceRecord> records) {
        List<InstanceRecord> ordered = records.stream()
                .sorted(Comparator.comparing(InstanceRecord::getId)).toList();
        for (InstanceRecord record : ordered) {
            if (record.getState() == InstanceState.LOST) {
                rebuildLost(wl, record);
                return true;
            }
            if (record.getState() == InstanceState.START_FAILED) {
                boolean desiredVersion = record.getImage().equals(wl.getSpec().image());
                stopAndRemove(record);
                if (desiredVersion && wl.getUpdatePhase() == UpdatePhase.ROLLING) {
                    wl.setRolloutAttempts(wl.getRolloutAttempts() + 1);
                }
                eventLog.record(EventLog.BUSINESS_RECONCILE, wl.getUid(), "StartFailedRecordEvicted",
                        "instance=" + record.getId() + " image=" + record.getImage()
                                + (desiredVersion ? "（计入滚动更新失败预算）" : "（旧版本自愈）"));
                return true;
            }
        }
        return false;
    }

    /** 在健康节点重建 LOST 实例。沿用实例 id（旧节点已 fence，恢复时会被 wipe），天然幂等。 */
    private void rebuildLost(Workload wl, InstanceRecord lost) {
        SchedulingDecision decision = scheduler.schedule();
        if (!decision.schedulable()) {
            wl.setPhase(WorkloadPhase.FAILED);
            wl.addCondition(Condition.of("Ready", false, REASON_CAPACITY,
                    "节点故障后无法在剩余健康节点重建实例 " + lost.getId() + "：" + decision.reason(),
                    wl.getGeneration()));
            eventLog.record(EventLog.BUSINESS_CAPACITY, wl.getUid(), REASON_CAPACITY,
                    "重建被拒绝: " + decision.reason());
            return;
        }
        SimulatedNode target = decision.node();
        boolean ok = target.startContainer(lost.getId(), lost.getImage());
        InstanceState newState = ok ? InstanceState.RUNNING : InstanceState.START_FAILED;
        store.removeInstance(lost.getId());
        store.putInstance(new InstanceRecord(lost.getId(), wl.getUid(), target.getInfo().getUid(),
                lost.getImage(), lost.getGeneration(), newState));
        if (ok) {
            eventLog.record(EventLog.BUSINESS_NODE_FAILURE, wl.getUid(), REASON_REBUILT,
                    "instance=" + lost.getId() + " image=" + lost.getImage()
                            + " 从失联节点 " + lost.getNodeUid() + " 重建到 "
                            + target.getInfo().getUid());
        } else {
            eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, wl.getUid(), "StartFailedOnTarget",
                    "instance=" + lost.getId() + " 在节点 " + target.getInfo().getUid()
                            + " 启动失败，等待重试");
        }
    }

    private void handleDeleting(Workload wl, List<InstanceRecord> records) {
        if (records.isEmpty()) {
            store.purge(wl.getUid());
            eventLog.record(EventLog.BUSINESS_LIFECYCLE, wl.getUid(), "Deleted",
                    "工作负载及其全部实例已删除");
            return;
        }
        InstanceRecord target = records.stream()
                .sorted(Comparator.comparing(InstanceRecord::getId)).findFirst().orElseThrow();
        stopAndRemove(target);
        eventLog.record(EventLog.BUSINESS_LIFECYCLE, wl.getUid(), "Terminating",
                "删除中，停止实例 " + target.getId() + "，剩余 " + (records.size() - 1));
    }

    /**
     * 滚动更新。硬不变量：任何动作执行后，健康节点上 RUNNING 的实例数不得低于 minAvailable。
     * 顺序：先在 surge 空间内建新实例 → surge 不足且可用性允许时删一个旧实例腾位 →
     * 新版本就绪数达标后彻底清理旧实例 → SUCCEEDED。
     * 失败/超时：回收所有新版本实例并回退（旧版本随后由自愈/补齐逻辑重建），置 FAILED 明确告停。
     */
    private void handleRollout(Workload wl, List<InstanceRecord> records) {
        int desired = wl.getSpec().replicas();
        int maxSurge = wl.getSpec().maxSurge();
        int minAvailable = wl.getSpec().minAvailable();
        String desiredImage = wl.getSpec().image();
        long gen = wl.getGeneration();

        List<InstanceRecord> newRecords = new ArrayList<>();
        List<InstanceRecord> oldRecords = new ArrayList<>();
        for (InstanceRecord r : records) {
            if (r.getImage().equals(desiredImage)) {
                newRecords.add(r);
            } else {
                oldRecords.add(r);
            }
        }
        long newReady = newRecords.stream().filter(this::isRunningHealthy).count();
        long available = records.stream().filter(this::isRunningHealthy).count();
        long liveCount = records.stream().filter(r -> r.getState() != InstanceState.LOST).count();

        // 成功：新版本就绪数达标，且旧版本已彻底清理。
        if (newReady >= desired && oldRecords.isEmpty()) {
            wl.setUpdatePhase(UpdatePhase.SUCCEEDED);
            wl.setPhase(WorkloadPhase.RUNNING);
            wl.addCondition(Condition.of("Rollout", true, REASON_ROLLOUT_OK,
                    "版本 " + desiredImage + " 滚动更新完成：" + desired
                            + " 个新版本实例全部就绪，旧版本已彻底清理", gen));
            eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, wl.getUid(), REASON_ROLLOUT_OK,
                    "image=" + desiredImage + " ready=" + desired);
            return;
        }

        String failure = rolloutFailureReason(wl);
        if (failure != null) {
            wl.setRolloutFailureReason(reasonCode(failure));
            abortRollout(wl, newRecords, oldRecords, failure);
            return;
        }

        // 还需要新版本实例：优先在 surge 空间内“先建”。
        if (newReady < desired) {
            if (liveCount < (long) desired + maxSurge) {
                launchNewInstance(wl, gen, desiredImage);
                return;
            }
            // surge 已满：只有删掉一个旧实例才能腾位。删除后可用数不得跌破下限。
            InstanceRecord removableOld = pickRemovableOld(oldRecords, available, minAvailable);
            if (removableOld != null) {
                stopAndRemove(removableOld);
                eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, wl.getUid(), "EvictOldForSurge",
                        "surge 空间不足，删除旧实例 " + removableOld.getId()
                                + " 腾位；删除后可用实例数=" + (available - 1)
                                + " >= minAvailable=" + minAvailable);
                return;
            }
            // 既不能建（无容量/surge 满）也不能删（会跌破 minAvailable）：确定性阻塞，
            // 记录原因；持续到 rolloutDeadline 后会超时告停回退。
            wl.addCondition(Condition.of("Rollout", false, REASON_CAPACITY,
                    "滚动更新被阻塞：可用实例数=" + available + " 已等于 minAvailable="
                            + minAvailable + "，不能再删旧实例；同时没有 surge/容量空间建立新实例。"
                            + "将保持旧版本服务并等待至超时。", gen));
            eventLog.record(EventLog.BUSINESS_CAPACITY, wl.getUid(), REASON_CAPACITY,
                    "滚动更新腾不出可用空间: available=" + available
                            + " minAvailable=" + minAvailable + " live=" + liveCount
                            + " cap(desired+surge)=" + (desired + maxSurge));
            return;
        }

        // 新版本已就绪数达标：彻底清理剩余旧版本，每次一个，且不得跌破下限。
        if (newReady >= minAvailable) {
            InstanceRecord removableOld = pickRemovableOld(oldRecords, available, minAvailable);
            if (removableOld != null) {
                stopAndRemove(removableOld);
                eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, wl.getUid(), "CleanupOldVersion",
                        "新版本已可接管，清理旧版本实例 " + removableOld.getId()
                                + "（image=" + removableOld.getImage() + "）");
                return;
            }
        }
    }

    private InstanceRecord pickRemovableOld(List<InstanceRecord> oldRecords,
                                            long available, int minAvailable) {
        if (available - 1 < minAvailable) {
            return null;
        }
        return oldRecords.stream()
                .filter(r -> r.getState() != InstanceState.LOST)
                .sorted(Comparator.comparing(InstanceRecord::getId))
                .findFirst().orElse(null);
    }

    private void launchNewInstance(Workload wl, long gen, String desiredImage) {
        SchedulingDecision decision = scheduler.schedule();
        if (!decision.schedulable()) {
            // 容量暂时不足：记为进展受阻，等待节点恢复/旧实例腾位；超过 deadline 会告停。
            wl.addCondition(Condition.of("Rollout", false, REASON_CAPACITY,
                    "暂无可调度容量，滚动更新挂起等待：" + decision.reason(), gen));
            eventLog.record(EventLog.BUSINESS_CAPACITY, wl.getUid(), REASON_CAPACITY,
                    "新版本实例暂不可调度: " + decision.reason());
            return;
        }
        SimulatedNode target = decision.node();
        String instanceId = wl.getUid() + "-" + wl.allocateInstanceSeq();
        boolean ok = target.startContainer(instanceId, desiredImage);
        InstanceState state = ok ? InstanceState.RUNNING : InstanceState.START_FAILED;
        store.putInstance(new InstanceRecord(instanceId, wl.getUid(),
                target.getInfo().getUid(), desiredImage, gen, state));
        if (ok) {
            eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, wl.getUid(), "NewVersionLaunched",
                    "新实例 " + instanceId + " image=" + desiredImage
                            + " 调度到节点 " + target.getInfo().getUid());
        } else {
            // 目标节点健康但镜像启动失败：占一次尝试预算，记录将在后续自愈中摘除重投。
            wl.setRolloutAttempts(wl.getRolloutAttempts() + 1);
            eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, wl.getUid(), "NewVersionStartFailed",
                    "新实例 " + instanceId + " 在节点 " + target.getInfo().getUid()
                            + " 启动失败，attempts=" + wl.getRolloutAttempts());
        }
    }

    /**
     * 滚动更新失败判定：
     * <ul>
     *   <li>新版本启动失败次数超过 maxStartRetries；</li>
     *   <li>超过 rolloutDeadline 仍未完成（容量不足/可用性约束死锁都由超时收敛）。</li>
     * </ul>
     */
    private String rolloutFailureReason(Workload wl) {
        if (wl.getRolloutAttempts() > wl.getSpec().maxStartRetries()) {
            return REASON_ROLLOUT_FAILED + ": 新版本 " + wl.getSpec().image()
                    + " 连续启动失败 " + wl.getRolloutAttempts() + " 次，超过预算 maxStartRetries="
                    + wl.getSpec().maxStartRetries();
        }
        if (wl.getRolloutDeadline() != null && clock.now().isAfter(wl.getRolloutDeadline())) {
            return REASON_DEADLINE + ": 滚动更新在 " + wl.getSpec().rolloutDeadlineSeconds()
                    + " 秒内未能完成，显式告停并回退";
        }
        return null;
    }

    private String reasonCode(String reason) {
        int idx = reason.indexOf(':');
        return idx > 0 ? reason.substring(0, idx) : reason;
    }

    /**
     * 回退：回收全部新版本实例（START_FAILED 记录直接摘除，RUNNING 的逐轮停止），
     * 保留旧版本记录并在随后的调谐中自愈/补齐。回退期间不允许两版本长期混跑，
     * 一旦判定失败，新版本只会减少不会增加。
     */
    private void abortRollout(Workload wl, List<InstanceRecord> newRecords,
                              List<InstanceRecord> oldRecords, String reason) {
        // 确定回退目标：现存旧记录的镜像；若无（首轮部署即失败），则不存在可回退版本。
        String fallbackImage = oldRecords.stream()
                .map(InstanceRecord::getImage)
                .filter(img -> !img.equals(wl.getSpec().image()))
                .findFirst().orElse(null);

        InstanceRecord removable = newRecords.stream()
                .filter(r -> r.getState() != InstanceState.LOST)
                .sorted(Comparator.comparing(InstanceRecord::getId))
                .findFirst().orElse(null);
        if (removable != null) {
            stopAndRemove(removable);
            if (fallbackImage != null) {
                wl.setRollbackTarget(fallbackImage, wl.getObservedGeneration());
            }
            eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, wl.getUid(), "RollbackRemoveNew",
                    "更新失败，回退中：移除新版本实例 " + removable.getId()
                            + (fallbackImage != null ? "，回退目标=" + fallbackImage
                                    : "，无历史版本可回退"));
            return;
        }

        if (fallbackImage == null) {
            // 首轮部署就失败：没有旧版本可回退，直接终态告停并说明原因。
            String code = wl.getRolloutFailureReason() != null
                    ? wl.getRolloutFailureReason() : REASON_ROLLOUT_FAILED;
            wl.setRolloutFailureReason(null);
            wl.setUpdatePhase(UpdatePhase.FAILED);
            wl.setPhase(WorkloadPhase.FAILED);
            wl.addCondition(Condition.of("Rollout", false, code,
                    "版本 " + wl.getSpec().image() + " 部署失败且不存在可回退的旧版本。原因："
                            + reason + "。请修正镜像/容量后重新提交期望状态。", wl.getGeneration()));
            eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, wl.getUid(), REASON_ROLLOUT_FAILED,
                    reason + "（无旧版本可回退，直接告停）");
            return;
        }

        wl.setRollbackTarget(fallbackImage, wl.getObservedGeneration());
        eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, wl.getUid(), "RollbackStarted",
                "新版本实例已全部回收，开始在旧版本 " + fallbackImage + " 上补齐副本");
    }

    /**
     * 失败回退调谐：只允许旧版本实例存在（新版本已在 abort 中停止创建），
     * 逐轮把旧版本补齐到期望副本数；补齐完成后置 FAILED 终态并清空回退目标。
     * 回退过程同样受节点容量约束。
     *
     * @return true 表示本轮执行了动作
     */
    private boolean handleRollback(Workload wl, List<InstanceRecord> records) {
        String targetImage = wl.getRollbackTargetImage();
        int desired = wl.getSpec().replicas();

        List<InstanceRecord> oldLive = records.stream()
                .filter(r -> !r.getImage().equals(wl.getSpec().image())
                        && r.getState() != InstanceState.LOST)
                .toList();
        long oldReady = oldLive.stream().filter(this::isRunningHealthy).count();

        if (oldReady >= desired) {
            String code = wl.getRolloutFailureReason() != null
                    ? wl.getRolloutFailureReason() : REASON_ROLLOUT_FAILED;
            wl.clearRollbackTarget();
            wl.setRolloutFailureReason(null);
            wl.setUpdatePhase(UpdatePhase.FAILED);
            wl.setPhase(WorkloadPhase.FAILED);
            wl.addCondition(Condition.of("Rollout", false, code,
                    "滚动更新失败（" + code + "），已回退到旧版本 " + targetImage + "，" + desired
                            + " 个旧版本实例全部就绪，新版本已彻底清理，系统继续以旧版本提供服务。"
                            + "请修正后重新提交。", wl.getGeneration()));
            eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, wl.getUid(), "RollbackCompleted",
                    "回退完成，服务运行于旧版本 " + targetImage + "，ready=" + desired
                            + "，失败原因=" + code);
            return true;
        }

        if (oldLive.size() < desired) {
            SchedulingDecision decision = scheduler.schedule();
            if (!decision.schedulable()) {
                wl.setPhase(WorkloadPhase.FAILED);
                wl.addCondition(Condition.of("Rollout", false, REASON_CAPACITY,
                        "回退到旧版本 " + targetImage + " 时剩余健康节点容量不足："
                                + decision.reason(), wl.getGeneration()));
                eventLog.record(EventLog.BUSINESS_CAPACITY, wl.getUid(), REASON_CAPACITY,
                        "回退补齐失败: " + decision.reason());
                return false;
            }
            SimulatedNode target = decision.node();
            String instanceId = wl.getUid() + "-" + wl.allocateInstanceSeq();
            boolean ok = target.startContainer(instanceId, targetImage);
            store.putInstance(new InstanceRecord(instanceId, wl.getUid(),
                    target.getInfo().getUid(), targetImage, wl.getRollbackTargetGeneration(),
                    ok ? InstanceState.RUNNING : InstanceState.START_FAILED));
            eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, wl.getUid(), "RollbackLaunchOld",
                    "回退中：在节点 " + target.getInfo().getUid() + " 启动旧版本实例 "
                            + instanceId + " image=" + targetImage);
            return true;
        }
        return false;
    }

    /** 稳态（无进行中的更新）下的纯副本数漂移收敛：扩容逐个建，缩容逐个删。 */
    private boolean handleSteadyScaling(Workload wl, List<InstanceRecord> records) {
        int desired = wl.getSpec().replicas();
        List<InstanceRecord> live = records.stream()
                .filter(r -> r.getState() != InstanceState.LOST).toList();

        if (live.size() > desired) {
            InstanceRecord victim = live.stream()
                    .sorted(Comparator.comparing(InstanceRecord::getId).reversed())
                    .findFirst().orElseThrow();
            stopAndRemove(victim);
            eventLog.record(EventLog.BUSINESS_RECONCILE, wl.getUid(), REASON_SCALE,
                    "缩容：删除多余实例 " + victim.getId() + "，目标副本数=" + desired);
            return true;
        }
        if (live.size() < desired) {
            SchedulingDecision decision = scheduler.schedule();
            if (!decision.schedulable()) {
                // 稳态扩容暂时不可调度：保持进行中、持续等待并重试，不误报告停。
                wl.setPhase(WorkloadPhase.PROGRESSING);
                wl.addCondition(Condition.of("Ready", false, REASON_CAPACITY,
                        "暂未达到期望副本数 " + desired + "，等待可用容量：" + decision.reason(),
                        wl.getGeneration()));
                eventLog.record(EventLog.BUSINESS_CAPACITY, wl.getUid(), REASON_CAPACITY,
                        decision.reason());
                return false;
            }
            SimulatedNode target = decision.node();
            String instanceId = wl.getUid() + "-" + wl.allocateInstanceSeq();
            boolean ok = target.startContainer(instanceId, wl.getSpec().image());
            InstanceState state = ok ? InstanceState.RUNNING : InstanceState.START_FAILED;
            store.putInstance(new InstanceRecord(instanceId, wl.getUid(),
                    target.getInfo().getUid(), wl.getSpec().image(), wl.getGeneration(), state));
            eventLog.record(EventLog.BUSINESS_RECONCILE, wl.getUid(), REASON_SCALE,
                    "扩容：在节点 " + target.getInfo().getUid() + " 创建实例 " + instanceId
                            + "，目标副本数=" + desired);
            return true;
        }
        return false;
    }

    /** 幂等停止并摘除实例记录；节点失联时无法停容器也照常摘除记录（节点已 fence）。 */
    private void stopAndRemove(InstanceRecord record) {
        nodeRegistry.get(record.getNodeUid())
                .ifPresent(node -> node.stopContainer(record.getId()));
        store.removeInstance(record.getId());
    }

    /** 节点从分区恢复后由控制面调用：清空其残留容器，防止与重建出的实例形成脑裂。 */
    public void wipeRecoveredNode(String nodeUid) {
        nodeRegistry.get(nodeUid).ifPresent(SimulatedNode::wipeAll);
    }

    private void refreshStatus(Workload wl) {
        List<InstanceRecord> records = store.instancesOf(wl.getUid());
        int ready = (int) records.stream().filter(this::isRunningHealthy).count();
        int updatedReady = (int) records.stream()
                .filter(r -> isRunningHealthy(r)
                        && r.getImage().equals(wl.getSpec().image()))
                .count();
        wl.setReadyReplicas(ready);
        wl.setUpdatedReadyReplicas(updatedReady);
    }
}
