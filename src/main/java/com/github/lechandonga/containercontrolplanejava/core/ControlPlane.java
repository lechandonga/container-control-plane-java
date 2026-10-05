package com.github.lechandonga.containercontrolplanejava.core;

import com.github.lechandonga.containercontrolplanejava.exception.ApiException;
import com.github.lechandonga.containercontrolplanejava.model.Event;
import com.github.lechandonga.containercontrolplanejava.model.Instance;
import com.github.lechandonga.containercontrolplanejava.model.InstanceState;
import com.github.lechandonga.containercontrolplanejava.model.Node;
import com.github.lechandonga.containercontrolplanejava.model.NodeStatus;
import com.github.lechandonga.containercontrolplanejava.model.Phase;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.sim.NodeSimulator;
import com.github.lechandonga.containercontrolplanejava.sim.SimulationClock;
import com.github.lechandonga.containercontrolplanejava.store.Snapshot;
import com.github.lechandonga.containercontrolplanejava.store.StateStore;
import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;

/**
 * 单机容器编排控制面核心。
 *
 * <p>不变量（任何提交顺序 / 异常时序下都成立）：
 * <ol>
 *   <li>所有状态变更在一把全局锁内完成并原子落盘；并发写由乐观锁 resourceVersion 拒绝，
 *       或按“删除优先”的确定规则合并，绝不静默覆盖、绝不脑裂。</li>
 *   <li>实例 ID 全局唯一且单调（seq 不复用）；重复提交 / 重复 tick / 重启后重建均幂等，
 *       不会产生重复实例或计数漂移。</li>
 *   <li>节点容量是硬约束：健康节点上的实例数绝不超过 capacity；调度不可行时保持缺口并说明原因，
 *       绝不超卖。</li>
 *   <li>滚动更新 maxSurge=0：先把新版本补到 minAvailable，再逐个“新就绪后杀旧”；
 *       新版本失败/超时则确定性回滚旧版本，不允许两个版本长期混跑。</li>
 * </ol>
 */
@Service
public class ControlPlane {
    private static final int MAX_EVENTS = 500;

    private final StateStore store;
    private final SimulationClock clock;
    private final NodeSimulator simulator;
    private final Object lock = new Object();
    private Snapshot snapshot;

    public ControlPlane(StateStore store, SimulationClock clock, NodeSimulator simulator) {
        this.store = store;
        this.clock = clock;
        this.simulator = simulator;
    }

    @PostConstruct
    public void restore() {
        synchronized (lock) {
            snapshot = store.load();
            clock.advance(snapshot.getTick());
            // 重启后运行时为易失状态：重新注册已知节点（视为 kubelet 重新上报）。
            snapshot.getNodes().keySet().forEach(id -> simulator.registerNode(id, null));
        }
    }

    private void persist() {
        snapshot.setTick(clock.now());
        store.persist(snapshot);
    }

    private void recordEvent(String workloadId, String type, String message) {
        snapshot.getEvents().add(new Event(clock.now(), workloadId, type, message));
        List<Event> events = snapshot.getEvents();
        while (events.size() > MAX_EVENTS) {
            events.remove(0);
        }
    }

    public Snapshot snapshotView() {
        synchronized (lock) {
            return snapshot;
        }
    }

    // ------------------------------------------------------------ 节点管理

    public Node registerNode(String id, int capacity, Integer startupTicks) {
        if (id == null || id.isBlank()) {
            throw ApiException.badRequest("INVALID_NODE", "节点 ID 不能为空");
        }
        if (capacity <= 0) {
            throw ApiException.badRequest("INVALID_NODE", "节点容量必须为正数: " + capacity);
        }
        synchronized (lock) {
            Node existing = snapshot.getNodes().get(id);
            Node node = existing == null
                    ? new Node(id, capacity, NodeStatus.READY, -1)
                    : new Node(id, capacity, existing.status(), existing.lostTick());
            snapshot.getNodes().put(id, node);
            simulator.registerNode(id, startupTicks);
            recordEvent(null, "NodeRegistered", "节点 " + id + " 注册，容量 " + capacity);
            reconcileAll();
            return node;
        }
    }

    /** 模拟节点失联（心跳超时）。重复调用幂等。 */
    public Node markNodeLost(String nodeId) {
        synchronized (lock) {
            Node node = requireNode(nodeId);
            if (node.status() == NodeStatus.READY) {
                snapshot.getNodes().put(nodeId, node.withStatus(NodeStatus.NOT_READY, clock.now()));
                recordEvent(null, "NodeLost", "节点 " + nodeId + " 失联，其上实例将在健康节点重建");
                reconcileAll();
            }
            return snapshot.getNodes().get(nodeId);
        }
    }

    /** 节点恢复健康（空节点回归，旧实例不会复活，必须经调谐显式重建）。 */
    public Node markNodeReady(String nodeId) {
        synchronized (lock) {
            Node node = requireNode(nodeId);
            if (node.status() == NodeStatus.NOT_READY) {
                snapshot.getNodes().put(nodeId, node.withStatus(NodeStatus.READY, clock.now()));
                recordEvent(null, "NodeReady", "节点 " + nodeId + " 恢复健康");
                reconcileAll();
            }
            return snapshot.getNodes().get(nodeId);
        }
    }

    public void removeNode(String nodeId) {
        synchronized (lock) {
            requireNode(nodeId);
            snapshot.getNodes().remove(nodeId);
            simulator.removeNode(nodeId);
            recordEvent(null, "NodeRemoved", "节点 " + nodeId + " 已移除");
            reconcileAll();
        }
    }

    private Node requireNode(String nodeId) {
        Node node = snapshot.getNodes().get(nodeId);
        if (node == null) {
            throw ApiException.notFound("节点不存在: " + nodeId);
        }
        return node;
    }

    // ------------------------------------------------------------ 工作负载

    public Workload submit(WorkloadRequest req) {
        validate(req);
        synchronized (lock) {
            Workload existing = snapshot.getWorkloads().get(req.id());
            if (existing == null) {
                if (req.resourceVersion() != null) {
                    throw ApiException.conflict("工作负载 " + req.id()
                            + " 已被删除（删除优先），基于旧 resourceVersion 的更新被拒绝，不会重建对象");
                }
                Workload w = new Workload(req.id(), req.image(), req.version(), req.replicas(),
                        req.minAvailable(), req.startTimeoutTicks(), req.rolloutTimeoutTicks());
                snapshot.getWorkloads().put(w.getId(), w);
                recordEvent(w.getId(), "Submitted",
                        "提交期望状态: image=" + w.getImage() + ", version=" + w.getVersion()
                                + ", replicas=" + w.getReplicas() + ", minAvailable=" + w.getMinAvailable());
                reconcileAll();
                return w;
            }
            if (existing.getPhase() == Phase.TERMINATING) {
                throw ApiException.conflict("工作负载 " + req.id() + " 正在删除（删除优先），拒绝更新");
            }
            if (req.resourceVersion() != null && req.resourceVersion() != existing.getResourceVersion()) {
                throw ApiException.conflict("乐观锁冲突: 提交基于版本 " + req.resourceVersion()
                        + "，当前版本已是 " + existing.getResourceVersion()
                        + "；请重新读取最新状态后再提交（未携带 resourceVersion 可强制覆盖）");
            }
            if (sameSpec(existing, req)) {
                recordEvent(existing.getId(), "SubmitDedup", "重复提交完全相同的期望状态，已忽略（幂等）");
                return existing;
            }
            boolean changed = existing.applySpec(req.image(), req.version(), req.replicas(),
                    req.minAvailable(), req.startTimeoutTicks(), req.rolloutTimeoutTicks());
            if (changed) {
                recordEvent(existing.getId(), "SpecUpdated",
                        "期望状态更新: desiredVersion=" + req.version() + ", replicas=" + req.replicas()
                                + ", resourceVersion=" + existing.getResourceVersion());
            }
            reconcileAll();
            return existing;
        }
    }

    public void delete(String id) {
        synchronized (lock) {
            Workload w = snapshot.getWorkloads().get(id);
            if (w == null) {
                throw ApiException.notFound("工作负载不存在: " + id);
            }
            w.setPhase(Phase.TERMINATING);
            w.setReason("删除优先：终止全部实例");
            w.getInstances().clear();
            recordEvent(id, "Terminating", "工作负载进入删除，全部实例已终止");
            snapshot.getWorkloads().remove(id);
            recordEvent(id, "Deleted", "工作负载已从控制面移除");
            persist();
        }
    }

    private static boolean sameSpec(Workload w, WorkloadRequest r) {
        return w.getImage().equals(r.image()) && w.getVersion().equals(r.version())
                && w.getReplicas() == r.replicas() && w.getMinAvailable() == r.minAvailable()
                && w.getStartTimeoutTicks() == r.startTimeoutTicks()
                && w.getRolloutTimeoutTicks() == r.rolloutTimeoutTicks();
    }

    private void validate(WorkloadRequest req) {
        if (req == null || req.id() == null || req.id().isBlank()) {
            throw ApiException.badRequest("INVALID_WORKLOAD", "工作负载 ID 不能为空");
        }
        if (req.image() == null || req.image().isBlank()
                || req.version() == null || req.version().isBlank()) {
            throw ApiException.badRequest("INVALID_WORKLOAD", "image 与 version 不能为空");
        }
        if (req.replicas() < 0) {
            throw ApiException.badRequest("INVALID_WORKLOAD", "replicas 不能为负数");
        }
        if (req.minAvailable() < 0 || req.minAvailable() > req.replicas()) {
            throw ApiException.badRequest("INVALID_WORKLOAD",
                    "minAvailable 必须满足 0 <= minAvailable <= replicas");
        }
        if (req.startTimeoutTicks() <= 0 || req.rolloutTimeoutTicks() <= 0) {
            throw ApiException.badRequest("INVALID_WORKLOAD", "超时必须为正的 tick 数");
        }
    }

    // ------------------------------------------------------------ 时钟与调谐

    /** 推进模拟时钟并执行一轮全量调谐。重复 tick 幂等：观察不重复创建实例。 */
    public long tick(long steps) {
        synchronized (lock) {
            long now = clock.advance(steps);
            reconcileAll();
            return now;
        }
    }

    /** 不推进时钟，仅按当前观察结果调谐（节点事件等即时场景，同样幂等）。 */
    public void reconcile() {
        synchronized (lock) {
            reconcileAll();
        }
    }

    /** 测试支持：清空全部状态并重置时钟。 */
    public void reset() {
        synchronized (lock) {
            snapshot = new Snapshot();
            clock.advance(-clock.now());
            store.persist(snapshot);
        }
    }

    private void reconcileAll() {
        List<String> ids = new ArrayList<>(snapshot.getWorkloads().keySet());
        ids.sort(Comparator.naturalOrder());
        for (String id : ids) {
            Workload w = snapshot.getWorkloads().get(id);
            if (w != null) {
                reconcileOne(w);
            }
        }
        persist();
    }

    private void reconcileOne(Workload w) {
        long now = clock.now();

        // 1) 期望版本变化 => 进入滚动更新；UPDATE_FAILED 后提交新版本视为重试。
        if (!w.getVersion().equals(w.getTargetVersion())
                && (w.getPhase() == Phase.RUNNING || w.getPhase() == Phase.PENDING
                || w.getPhase() == Phase.UPDATE_FAILED)) {
            w.setRolloutGeneration(w.getGeneration());
            w.setRolloutStartTick(now);
            w.setPhase(Phase.UPDATING);
            w.setReason("滚动更新中: " + w.getVersion() + " -> " + w.getTargetVersion());
            recordEvent(w.getId(),
                    w.getObservedGeneration() == 0 ? "RolloutStarted" : "RolloutRetried",
                    w.getReason());
        }

        // 2) 观察实例实际状态（节点健康 + 模拟运行时）。失联节点上的实例回收。
        List<Instance> survivors = new ArrayList<>();
        for (Instance inst : new ArrayList<>(w.getInstances())) {
            Node node = snapshot.getNodes().get(inst.nodeId());
            boolean nodeReady = node != null && node.status() == NodeStatus.READY;
            NodeSimulator.InstanceOutcome outcome =
                    simulator.observe(nodeReady, inst.bornTick(), now,
                            inst.nodeId(), w.getImage(), inst.version());
            switch (outcome) {
                case LOST -> recordEvent(w.getId(), "InstanceEvicted",
                        "实例 " + inst.id() + " 随节点 " + inst.nodeId()
                                + " 失联被回收，将在健康节点重建");
                case FAILS_TO_START -> {
                    if (inst.state() != InstanceState.FAILED) {
                        recordEvent(w.getId(), "InstanceFailed",
                                "实例 " + inst.id() + " 启动失败（节点 " + inst.nodeId()
                                        + "，版本 " + inst.version() + "）");
                        survivors.add(inst.withState(InstanceState.FAILED, now));
                    } else {
                        survivors.add(inst);
                    }
                }
                case RUNNING -> {
                    if (inst.state() != InstanceState.RUNNING) {
                        recordEvent(w.getId(), "InstanceReady", "实例 " + inst.id() + " 已就绪");
                    }
                    survivors.add(inst.state() == InstanceState.RUNNING
                            ? inst : inst.withState(InstanceState.RUNNING, now));
                }
                case STARTING -> survivors.add(inst.state() == InstanceState.PENDING
                        ? inst : inst.withState(InstanceState.PENDING, now));
            }
        }
        w.getInstances().clear();
        w.getInstances().addAll(survivors);

        // 3) 计算健康节点已占用槽位（跨所有工作负载，容量是集群硬约束）。
        Map<String, Integer> used = new HashMap<>();
        for (Workload other : snapshot.getWorkloads().values()) {
            for (Instance inst : other.getInstances()) {
                Node n = snapshot.getNodes().get(inst.nodeId());
                if (n != null && n.status() == NodeStatus.READY) {
                    used.merge(inst.nodeId(), 1, Integer::sum);
                }
            }
        }

        if (w.getPhase() == Phase.UPDATING) {
            reconcileRollout(w, now, used);
        } else {
            reconcileSteady(w, now, used);
        }

        w.setObservedGeneration(w.getGeneration());
    }

    /** 稳态调谐：清理失败实例，按期望版本补足 replicas，受容量约束。 */
    private void reconcileSteady(Workload w, long now, Map<String, Integer> used) {
        w.getInstances().removeIf(i -> {
            if (i.state() == InstanceState.FAILED) {
                recordEvent(w.getId(), "InstanceRemoved",
                        "回收失败实例 " + i.id() + " 并将重建");
                return true;
            }
            return false;
        });
        // 重新统计回收失败实例后的槽位占用。
        used.clear();
        for (Workload other : snapshot.getWorkloads().values()) {
            for (Instance inst : other.getInstances()) {
                Node n = snapshot.getNodes().get(inst.nodeId());
                if (n != null && n.status() == NodeStatus.READY) {
                    used.merge(inst.nodeId(), 1, Integer::sum);
                }
            }
        }

        // 缩容：实例多于期望时，按确定性顺序删除（先删 PENDING/FAILED 中的最大 seq，再删 RUNNING）。
        while (w.getInstances().size() > w.getReplicas()) {
            Instance victim = w.getInstances().stream()
                    .max(Comparator.comparingInt((Instance i) ->
                            i.state() == InstanceState.RUNNING ? 1 : 0)
                            .thenComparingLong(Instance::bornTick)
                            .thenComparing(Instance::id))
                    .orElseThrow();
            w.getInstances().remove(victim);
            used.merge(victim.nodeId(), -1, Integer::sum);
            recordEvent(w.getId(), "InstanceScaledDown",
                    "缩容终止实例 " + victim.id() + "（节点 " + victim.nodeId() + "）");
        }

        int missing = w.getReplicas() - w.getInstances().size();
        boolean unschedulable = false;
        for (int i = 0; i < missing; i++) {
            String nodeId = pickNode(w, used);
            if (nodeId == null) {
                unschedulable = true;
                break;
            }
            Instance inst = createInstance(w, nodeId, w.getVersion(), now);
            used.merge(nodeId, 1, Integer::sum);
            recordEvent(w.getId(), "InstanceCreated",
                    "创建实例 " + inst.id() + "（版本 " + w.getVersion() + "）于节点 " + nodeId);
        }

        int ready = (int) w.getInstances().stream()
                .filter(i -> i.state() == InstanceState.RUNNING).count();
        if (w.getInstances().size() >= w.getReplicas() && ready >= w.getReplicas()) {
            w.setPhase(Phase.RUNNING);
            w.setReason("已收敛：" + ready + "/" + w.getReplicas() + " 实例就绪，版本 " + w.getVersion());
        } else if (unschedulable) {
            w.setPhase(Phase.PENDING);
            w.setReason("容量不足：健康节点空闲槽位无法满足 replicas=" + w.getReplicas()
                    + "，等待节点恢复/扩容；当前实例 " + w.getInstances().size()
                    + "，就绪 " + ready + "，可用下限 " + w.getMinAvailable());
        } else {
            w.setPhase(Phase.PENDING);
            w.setReason("启动中：就绪 " + ready + "/" + w.getReplicas());
        }
    }

    /**
     * 滚动更新调谐（maxSurge=0，实例总数恒定为 replicas，确定性）：
     * <ol>
     *   <li>若集群存在空闲槽位，先在空闲槽位上建立新版本（旧实例全部保留），直到新版本就绪数
     *       达到 minAvailable；</li>
     *   <li>此后每轮“先终止一个旧实例腾出槽位（终止后当前可用仍 &gt;= minAvailable），再建新”；</li>
     *   <li>旧版本清零且新版本全部就绪 => 提交版本进入 RUNNING；期间绝不超卖、总数不膨胀；</li>
     *   <li>无任何空闲槽位且全部为旧版本时无法安全起步：保持现状等待容量，总超时告停并回滚；</li>
     *   <li>新版本 FAILED / 启动超时 / 总超时 => 移除新版本、确定性回滚旧版本并给出失败原因。</li>
     * </ol>
     */
    private void reconcileRollout(Workload w, long now, Map<String, Integer> used) {
        String target = w.getTargetVersion();

        // 失败/超时判定（优先级最高，立即告停并回滚）。
        boolean failedInstance = w.getInstances().stream().anyMatch(i ->
                i.version().equals(target) && i.state() == InstanceState.FAILED);
        boolean startTimeout = w.getInstances().stream().anyMatch(i ->
                i.version().equals(target) && i.state() == InstanceState.PENDING
                        && now - i.bornTick() + 1 > w.getStartTimeoutTicks());
        if (failedInstance || startTimeout) {
            failRollout(w, "新版本 " + target + " 实例"
                    + (failedInstance ? "启动失败（镜像/节点故障注入）"
                    : "超过 startTimeoutTicks=" + w.getStartTimeoutTicks() + " 仍未就绪"));
            reconcileSteadyAfterAbort(w, now, used);
            return;
        }
        if (now - w.getRolloutStartTick() + 1 > w.getRolloutTimeoutTicks()) {
            failRollout(w, "滚动更新超过 rolloutTimeoutTicks=" + w.getRolloutTimeoutTicks()
                    + " 仍未完成（可能容量不足导致无法推进）");
            reconcileSteadyAfterAbort(w, now, used);
            return;
        }

        long newRunning = w.getInstances().stream()
                .filter(i -> i.version().equals(target) && i.state() == InstanceState.RUNNING).count();
        long newTotal = w.getInstances().stream()
                .filter(i -> i.version().equals(target)).count();
        List<Instance> oldInstances = w.getInstances().stream()
                .filter(i -> !i.version().equals(target)).toList();

        // 实例总数恒定为 replicas（maxSurge=0）。
        // 本轮允许新建的新版本数量：先利用空闲槽位建立 minAvailable 个新版本（不杀旧），
        // 之后每建一个新版本前先终止一个旧实例腾出槽位。
        int canCreate = (int) Math.min(Integer.MAX_VALUE,
                (long) w.getReplicas() - newTotal);

        // 阶段 A：一次性建立 minAvailable 个新版本实例（仅使用空闲槽位，不杀旧）。
        // 它们就绪前不会终止任何旧实例，因此可用数不会提前跌破旧版本当前水平。
        while (newTotal < w.getMinAvailable() && newTotal < w.getReplicas()) {
            String nodeId = pickNode(w, used);
            if (nodeId == null || w.getInstances().size() >= w.getReplicas()) {
                // 无空闲槽位：仅当终止一个旧实例后当前可用仍 >= minAvailable 时才杀旧腾槽；
                // 否则停滞保持旧版本可用，总超时告停（绝不超卖、绝不跌破可用下限）。
                List<Instance> oldsNow = w.getInstances().stream()
                        .filter(i -> !i.version().equals(target)).toList();
                long availNow = w.getInstances().stream()
                        .filter(i -> i.state() == InstanceState.RUNNING).count();
                if (oldsNow.isEmpty() || availNow - 1 < w.getMinAvailable()) {
                    w.setReason("更新受阻：无法在不跌破可用下限(" + w.getMinAvailable()
                            + ")的前提下腾出槽位建立新版本，保持旧版本可用；"
                            + "超过 rolloutTimeout 将告停");
                    return;
                }
                Instance old = oldsNow.get(0);
                w.getInstances().remove(old);
                used.merge(old.nodeId(), -1, Integer::sum);
                recordEvent(w.getId(), "RolloutKillOld",
                        "容量受限：终止旧实例 " + old.id() + " 为新版本腾槽（可用仍 >= 下限）");
                nodeId = pickNode(w, used);
                if (nodeId == null) {
                    w.setReason("更新受阻：腾槽后仍无可调度节点，等待容量；超过 rolloutTimeout 将告停");
                    return;
                }
            }
            Instance inst = createInstance(w, nodeId, target, now);
            used.merge(nodeId, 1, Integer::sum);
            newTotal++;
            recordEvent(w.getId(), "RolloutCreateNew",
                    "建立新版本最小可用集：创建 " + inst.id() + " 于节点 " + nodeId);
        }

        // 阶段 B：新版本最小可用集已在途/就绪。此后每轮最多做一次交换：
        // 先终止一个旧实例腾出槽位（终止后当前可用仍 >= minAvailable），再创建一个新版本。
        List<Instance> olds = w.getInstances().stream()
                .filter(i -> !i.version().equals(target)).toList();
        long currentAvailable = w.getInstances().stream()
                .filter(i -> i.state() == InstanceState.RUNNING).count();
        if (!olds.isEmpty() && currentAvailable - 1 >= w.getMinAvailable()) {
            Instance victim = olds.get(0);
            w.getInstances().remove(victim);
            used.merge(victim.nodeId(), -1, Integer::sum);
            recordEvent(w.getId(), "RolloutKillOld",
                    "新版本最小可用集已建立，终止旧实例 " + victim.id()
                            + "（节点 " + victim.nodeId() + "）");
            String nodeId = pickNode(w, used);
            if (nodeId == null) {
                w.setReason("更新受阻：终止旧实例后仍无节点可调度新版本，等待容量，总超时将告停");
                return;
            }
            Instance inst = createInstance(w, nodeId, target, now);
            used.merge(nodeId, 1, Integer::sum);
            newTotal++;
            recordEvent(w.getId(), "RolloutCreateNew",
                    "替换创建新版本实例 " + inst.id() + " 于节点 " + nodeId);
        }

        long oldLeft = w.getInstances().stream()
                .filter(i -> !i.version().equals(target)).count();
        long newReady = w.getInstances().stream()
                .filter(i -> i.version().equals(target) && i.state() == InstanceState.RUNNING).count();

        // 阶段 C：旧版本清零且新版本全部就绪 => 提交版本。
        if (oldLeft == 0 && newReady >= w.getReplicas()) {
            w.setVersion(target);
            w.setPhase(Phase.RUNNING);
            w.setRolloutGeneration(-1);
            w.setRolloutStartTick(-1);
            w.setReason("滚动更新完成：已收敛到版本 " + target + "，" + newReady + " 个实例就绪");
            recordEvent(w.getId(), "RolloutSucceeded", w.getReason());
        } else {
            long available = w.getInstances().stream()
                    .filter(i -> i.state() == InstanceState.RUNNING).count();
            w.setReason("滚动更新中：新版本就绪 " + newReady + "/" + w.getReplicas()
                    + "，旧版本剩余 " + oldLeft + "，当前可用 " + available
                    + "（下限 " + w.getMinAvailable() + "），总数恒定 " + w.getReplicas());
        }
    }

    /** 回滚：移除全部新版本实例，目标版本恢复为旧版本，随后按稳态补齐旧版本。 */
    private void failRollout(Workload w, String cause) {
        String target = w.getTargetVersion();
        w.getInstances().removeIf(i -> {
            if (i.version().equals(target)) {
                recordEvent(w.getId(), "RolloutRemoveNew", "回滚：移除新版本实例 " + i.id());
                return true;
            }
            return false;
        });
        w.setTargetVersion(w.getVersion());
        w.setPhase(Phase.UPDATE_FAILED);
        w.setRolloutGeneration(-1);
        w.setRolloutStartTick(-1);
        long oldReady = w.getInstances().stream()
                .filter(i -> i.state() == InstanceState.RUNNING).count();
        w.setReason("滚动更新失败并已告停：" + cause + "；已确定性回滚到旧版本 "
                + w.getVersion() + "（旧实例保留 " + w.getInstances().size()
                + " 个，就绪 " + oldReady + "）。修复新版本后重新提交即可重试");
        recordEvent(w.getId(), "RolloutFailed", w.getReason());
    }

    private void reconcileSteadyAfterAbort(Workload w, long now, Map<String, Integer> used) {
        int missing = w.getReplicas() - w.getInstances().size();
        for (int i = 0; i < missing; i++) {
            String nodeId = pickNode(w, used);
            if (nodeId == null) {
                w.setReason(w.getReason() + "；回滚补旧副本时容量不足，保持缺口等待容量");
                return;
            }
            Instance inst = createInstance(w, nodeId, w.getVersion(), now);
            used.merge(nodeId, 1, Integer::sum);
            recordEvent(w.getId(), "RolloutBackfillOld",
                    "回滚补旧副本：创建 " + inst.id() + " 于节点 " + nodeId);
        }
    }

    /**
     * 确定性节点选择：健康且有空闲槽位的节点中，选“当前负载最低、ID 字典序最小”者。
     * 保证重复调谐/重启后的放置决策可解释、可复现，绝不超卖。
     */
    private String pickNode(Workload w, Map<String, Integer> used) {
        return snapshot.getNodes().values().stream()
                .filter(n -> n.status() == NodeStatus.READY)
                .filter(n -> used.getOrDefault(n.id(), 0) < n.capacity())
                .min(Comparator.comparingInt((Node n) -> used.getOrDefault(n.id(), 0))
                        .thenComparing(Node::id))
                .map(Node::id)
                .orElse(null);
    }

    private Instance createInstance(Workload w, String nodeId, String version, long now) {
        long seq = w.getInstanceSequence() + 1;
        w.setInstanceSequence(seq);
        Instance inst = new Instance(w.getId() + "-" + seq, w.getId(), nodeId,
                version, InstanceState.PENDING, now, -1);
        w.getInstances().add(inst);
        return inst;
    }
}
