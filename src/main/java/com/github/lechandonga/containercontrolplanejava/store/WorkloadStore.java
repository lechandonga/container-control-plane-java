package com.github.lechandonga.containercontrolplanejava.store;

import com.github.lechandonga.containercontrolplanejava.model.InstanceState;
import com.github.lechandonga.containercontrolplanejava.model.InstanceRecord;
import com.github.lechandonga.containercontrolplanejava.model.Revision;
import com.github.lechandonga.containercontrolplanejava.model.RevisionKind;
import com.github.lechandonga.containercontrolplanejava.model.RevisionStatus;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadPhase;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;
import com.github.lechandonga.containercontrolplanejava.time.ClockService;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 控制面的权威状态存储（单机内存实现）。
 *
 * <p>并发模型：
 * <ul>
 *   <li>期望状态的修改走 <b>乐观并发控制</b>：每次变更产生新的 resourceVersion，
 *       提交方必须携带上次读到的版本；版本不匹配抛 {@link ConflictException}，绝不静默覆盖。</li>
 *   <li>调谐循环对单个工作负载的“观察-决策-执行”临界区通过 {@link #lockFor} 串行化，
 *       同一工作负载在任意时刻只有一个调谐事务；不同工作负载互不阻塞。</li>
 * </ul>
 */
@Component
public class WorkloadStore {

    private final ConcurrentHashMap<String, Workload> workloads = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, InstanceRecord> instances = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> workloadLocks = new ConcurrentHashMap<>();
    private final AtomicLong rvSeq = new AtomicLong();
    private final ClockService clock;
    /** 状态落盘钩子：每次权威状态可能变化后由调谐临界区统一触发一次（崩溃恢复用）。 */
    private volatile Runnable stateChangeHook = () -> { };

    public WorkloadStore(ClockService clock) {
        this.clock = clock;
    }

    public void setStateChangeHook(Runnable hook) {
        this.stateChangeHook = hook == null ? () -> { } : hook;
    }

    /** 调谐临界区结束时调用：把本轮权威状态原子持久化到本地磁盘。 */
    public void fireStateChange() {
        stateChangeHook.run();
    }

    private String nextResourceVersion() {
        return "rv-" + rvSeq.incrementAndGet();
    }

    /** 获取单个工作负载的调谐互斥锁（锁对象稳定，调谐器在 finally 中释放）。 */
    public Object lockFor(String workloadUid) {
        return workloadLocks.computeIfAbsent(workloadUid, k -> new Object());
    }

    /**
     * 首次提交（创建）期望状态。重复 create 同 uid 视为冲突/非法，必须走 update。
     */
    public Workload create(String uid, WorkloadSpec spec) {
        spec.validate();
        Workload created = new Workload(uid, spec, 1, nextResourceVersion());
        Workload existing = workloads.putIfAbsent(uid, created);
        if (existing != null) {
            throw new ConflictException(uid, "<absent>", existing.getResourceVersion());
        }
        created.addRevision(Revision.open(1, clock.now(), RevisionKind.CREATE, null, spec, null));
        return created;
    }

    public Optional<Workload> find(String uid) {
        return Optional.ofNullable(workloads.get(uid));
    }

    public Workload get(String uid) {
        return find(uid).orElseThrow(() -> new NotFoundException(uid));
    }

    public List<Workload> list() {
        return new ArrayList<>(workloads.values());
    }

    /**
     * 声明式提交期望状态。
     *
     * <p>规则（确定性）：
     * <ul>
     *   <li>期望规格与当前逐字段相等：幂等空操作，generation 与 resourceVersion 均不变。</li>
     *   <li>规格有变化：必须携带与当前一致的 resourceVersion，否则拒绝并抛 {@link ConflictException}；
     *       接受后 generation+1（滚动更新以 generation 为界）、resourceVersion 换新。</li>
     * </ul>
     *
     * @return 提交结果（含是否发生实际变更）
     */
    public SubmitResult submit(String uid, WorkloadSpec newSpec, String expectedResourceVersion) {
        newSpec.validate();
        synchronized (lockFor(uid)) {
            Workload wl = get(uid);
            if (Objects.equals(newSpec, wl.getSpec())) {
                return new SubmitResult(wl, false);
            }
            if (expectedResourceVersion == null
                    || !expectedResourceVersion.equals(wl.getResourceVersion())) {
                throw new ConflictException(uid, expectedResourceVersion, wl.getResourceVersion());
            }
            WorkloadSpec before = wl.getSpec();
            wl.setSpec(newSpec);
            wl.setGeneration(wl.getGeneration() + 1);
            wl.setResourceVersion(nextResourceVersion());
            // 新提交代表人工介入：解除上一轮终态告停，交给调谐循环重新收敛
            wl.setRolloutAttempts(0);
            wl.setRolloutDeadline(null);
            wl.setUpdatePhase(com.github.lechandonga.containercontrolplanejava.model.UpdatePhase.IDLE);
            wl.clearRollbackTarget();
            wl.setRolloutFailureReason(null);
            // 上一条仍在发布中的修订被新的人工提交取代：终态明确，历史照实留痕。
            supersedeHeadIfOpen(wl, "SupersededByUpdate:r" + wl.getGeneration());
            wl.addRevision(Revision.open(wl.getGeneration(), clock.now(),
                    RevisionKind.UPDATE, before, newSpec, null));
            return new SubmitResult(wl, true);
        }
    }

    /**
     * 显式按修订回滚。
     *
     * <p>规则：
     * <ul>
     *   <li>{@code targetRevision} 为 null 时表示“退回上一个修订”（修订号严格小于当前的最大一条）；</li>
     *   <li>目标修订不存在 → {@link NotFoundException}（404）；</li>
     *   <li>与更新同样的乐观并发保护：版本缺失/不匹配 → {@link ConflictException}，不产生历史；</li>
     *   <li>目标声明与当前期望逐字段相等 → 幂等空操作（重复回滚请求），不写历史、不跳号；</li>
     *   <li>接受后把期望状态换成目标修订声明的副本数与版本，generation+1，
     *       并新增一条 kind=ROLLBACK 的新修订；目标修订之后、被本次回滚撤销的
     *       SUCCEEDED/IN_PROGRESS 修订照实标记为 ROLLED_BACK。</li>
     * </ul>
     */
    public RollbackResult rollback(String uid, Long targetRevision, String expectedResourceVersion) {
        synchronized (lockFor(uid)) {
            Workload wl = get(uid);
            if (expectedResourceVersion == null
                    || !expectedResourceVersion.equals(wl.getResourceVersion())) {
                throw new ConflictException(uid, expectedResourceVersion, wl.getResourceVersion());
            }
            Revision target = targetRevision == null
                    ? wl.previousRevision() : wl.findRevision(targetRevision);
            if (target == null) {
                throw new NotFoundException("工作负载 " + uid + " 的修订 "
                        + (targetRevision == null ? "<上一个>" : targetRevision) + " 不存在", true);
            }
            WorkloadSpec targetSpec = target.getAfter();
            if (Objects.equals(targetSpec, wl.getSpec())) {
                return new RollbackResult(wl, false, target);
            }
            WorkloadSpec before = wl.getSpec();
            wl.setSpec(targetSpec);
            wl.setGeneration(wl.getGeneration() + 1);
            wl.setResourceVersion(nextResourceVersion());
            wl.setRolloutAttempts(0);
            wl.setRolloutDeadline(clock.now().plusSeconds(targetSpec.rolloutDeadlineSeconds()));
            wl.setUpdatePhase(com.github.lechandonga.containercontrolplanejava.model.UpdatePhase.IDLE);
            wl.clearRollbackTarget();
            wl.setRolloutFailureReason(null);
            // 被本次回滚撤销的中间修订（不含目标自身）：成功过/进行中的，如实改写为 ROLLED_BACK；
            // FAILED/SUPERSEDED/ROLLED_BACK 保持其既有终态与原因，不覆盖历史事实。
            String markReason = "RolledBackTo:r" + target.getRevision() + " by r" + wl.getGeneration();
            for (Revision r : wl.getRevisions()) {
                if (r.getRevision() > target.getRevision()) {
                    r.markRolledBack(markReason, clock.now());
                }
            }
            Revision rollbackRevision = Revision.open(wl.getGeneration(), clock.now(),
                    RevisionKind.ROLLBACK, before, targetSpec, target.getRevision());
            wl.addRevision(rollbackRevision);
            return new RollbackResult(wl, true, target);
        }
    }

    private void supersedeHeadIfOpen(Workload wl, String reason) {
        Revision head = wl.headRevision();
        if (head != null) {
            head.complete(RevisionStatus.SUPERSEDED, reason, clock.now());
        }
    }

    /** 调谐器把当前期望对应的修订写入终态（成功/失败）。终态只写一次。 */
    public void completeHeadRevision(Workload wl, RevisionStatus status, String reason) {
        Revision head = wl.headRevision();
        if (head != null) {
            head.complete(status, reason, clock.now());
        }
    }

    /**
     * 删除工作负载。删除同样受乐观锁保护；实际清理由调谐循环完成（DELETING 阶段）。
     */
    public Workload markDeleting(String uid, String expectedResourceVersion) {
        synchronized (lockFor(uid)) {
            Workload wl = get(uid);
            if (expectedResourceVersion != null && !expectedResourceVersion.equals(wl.getResourceVersion())) {
                throw new ConflictException(uid, expectedResourceVersion, wl.getResourceVersion());
            }
            wl.setPhase(WorkloadPhase.DELETING);
            wl.setResourceVersion(nextResourceVersion());
            return wl;
        }
    }

    /** 调谐循环确认实例清空后，物理移除聚合与锁。 */
    public void purge(String uid) {
        workloads.remove(uid);
        workloadLocks.remove(uid);
    }

    public InstanceRecord putInstance(InstanceRecord record) {
        instances.put(record.getId(), record);
        return record;
    }

    public InstanceRecord getInstance(String id) {
        return instances.get(id);
    }

    public void removeInstance(String id) {
 instances.remove(id);
    }

    public List<InstanceRecord> instancesOf(String workloadUid) {
        List<InstanceRecord> list = new ArrayList<>();
        for (InstanceRecord r : instances.values()) {
            if (r.getWorkloadUid().equals(workloadUid)) {
                list.add(r);
            }
        }
        return list;
    }

    /** 节点上现存的全部实例记录（含 LOST 记录，调谐器据此决定搬迁）。 */
    public List<InstanceRecord> instancesOnNode(String nodeUid) {
        List<InstanceRecord> list = new ArrayList<>();
        for (InstanceRecord r : instances.values()) {
            if (r.getNodeUid().equals(nodeUid)) {
                list.add(r);
            }
        }
        return list;
    }

    public Map<String, InstanceRecord> allInstances() {
        return new ConcurrentHashMap<>(instances);
    }

    /** 仅供测试隔离使用：清空全部工作负载、实例与锁。 */
    public void resetForTests() {
        workloads.clear();
        instances.clear();
        workloadLocks.clear();
        rvSeq.set(0);
    }

    /** 调谐器在临界区内修改聚合状态后换发 resourceVersion（观察状态变化同样需要可见性版本）。 */
    public void bumpResourceVersion(Workload workload) {
        workload.setResourceVersion(nextResourceVersion());
    }

    // ===== 进程重启恢复支持（仅供持久化层使用） =====

    /** 当前资源版本自增序列值（重启后继续单调，不复用旧号）。 */
    public long resourceVersionCounter() {
        return rvSeq.get();
    }

    public void restoreResourceVersionCounter(long value) {
        rvSeq.set(value);
    }

    /** 清空内存权威状态并用快照重建：工作负载聚合（含修订历史）、实例记录、锁对象。 */
    public void restoreAll(List<Workload> workloadSnapshots,
                           List<InstanceRecord> instanceSnapshots) {
        workloads.clear();
        instances.clear();
        workloadLocks.clear();
        for (Workload wl : workloadSnapshots) {
            workloads.put(wl.getUid(), wl);
        }
        for (InstanceRecord record : instanceSnapshots) {
            instances.put(record.getId(), record);
        }
    }

    public Collection<Workload> workloadsView() {
        return new ArrayList<>(workloads.values());
    }

    public record SubmitResult(Workload workload, boolean changed) {}

    public record RollbackResult(Workload workload, boolean changed, Revision targetRevision) {}
}
