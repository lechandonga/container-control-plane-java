package com.github.lechandonga.containercontrolplanejava.store;

import com.github.lechandonga.containercontrolplanejava.model.InstanceRecord;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadPhase;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;
import com.github.lechandonga.containercontrolplanejava.revision.Revision;
import com.github.lechandonga.containercontrolplanejava.revision.RevisionLog;
import com.github.lechandonga.containercontrolplanejava.revision.RevisionTrigger;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
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
    private final AtomicLong rvSeq;
    private final RevisionLog revisionLog;

    public WorkloadStore(RevisionLog revisionLog) {
        this.revisionLog = revisionLog;
        this.rvSeq = new AtomicLong();
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
        Revision revision = revisionLog.append(uid, created.getGeneration(),
                RevisionTrigger.CREATE, null, null, spec);
        created.setActiveRevisionNo(revision.getRevision());
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
            WorkloadSpec oldSpec = wl.getSpec();
            wl.setSpec(newSpec);
            wl.setGeneration(wl.getGeneration() + 1);
            wl.setResourceVersion(nextResourceVersion());
            // 新提交代表人工介入：解除上一轮终态告停，交给调谐循环重新收敛
            wl.setRolloutAttempts(0);
            wl.setRolloutDeadline(null);
            wl.setUpdatePhase(com.github.lechandonga.containercontrolplanejava.model.UpdatePhase.IDLE);
            wl.clearRollbackTarget();
            wl.setUserRollback(false);
            wl.setCleanupNonTarget(false);
            wl.setSupersededRevisionNo(null);
            Revision revision = revisionLog.append(uid, wl.getGeneration(),
                    RevisionTrigger.UPDATE, null, oldSpec, newSpec);
            wl.setActiveRevisionNo(revision.getRevision());
            return new SubmitResult(wl, true);
        }
    }

    /**
     * 按修订回滚。并发保护与普通更新完全一致（必须携带当前 resourceVersion），
     * 被拒/重复提交同样不产生历史。回滚本身是一条新的 {@link RevisionTrigger#ROLLBACK} 修订，
     * 目标声明取自历史修订（副本数与版本），不覆盖任何旧历史。
     *
     * @param targetRevisionNo 目标修订号；null 表示“退回上一个修订”
     */
    public SubmitResult rollback(String uid, Long targetRevisionNo, String expectedResourceVersion) {
        synchronized (lockFor(uid)) {
            Workload wl = get(uid);
            List<Revision> history = revisionLog.list(uid);
            if (history.isEmpty()) {
                throw new InvalidRollbackException(
                        "工作负载 " + uid + " 没有任何发布历史，无法回滚");
            }
            Revision target;
            if (targetRevisionNo == null) {
                // “退回上一个修订”：最近一条历史修订之前的那条；若只有一条，则回到其声明。
                target = history.size() >= 2
                        ? history.get(history.size() - 2) : history.get(0);
            } else {
                target = revisionLog.get(uid, targetRevisionNo)
                        .orElseThrow(() -> new InvalidRollbackException(
                                "修订 " + targetRevisionNo + " 不存在于工作负载 " + uid + " 的发布历史"));
            }
            WorkloadSpec targetSpec = target.getAfterSpec();
            if (Objects.equals(targetSpec, wl.getSpec())) {
                // 声明与当前完全一致：幂等空操作，不写历史、不换版本。
                return new SubmitResult(wl, false);
            }
            if (expectedResourceVersion == null
                    || !expectedResourceVersion.equals(wl.getResourceVersion())) {
                throw new ConflictException(uid, expectedResourceVersion, wl.getResourceVersion());
            }
            WorkloadSpec oldSpec = wl.getSpec();
            Long superseded = wl.getActiveRevisionNo();
            wl.setSpec(targetSpec);
            wl.setGeneration(wl.getGeneration() + 1);
            wl.setResourceVersion(nextResourceVersion());
            wl.setRolloutAttempts(0);
            wl.setRolloutDeadline(null);
            wl.setUpdatePhase(com.github.lechandonga.containercontrolplanejava.model.UpdatePhase.IDLE);
            wl.clearRollbackTarget();
            wl.setRolloutFailureReason(null);
            wl.setUserRollback(true);
            wl.setCleanupNonTarget(false);
            wl.setSupersededRevisionNo(superseded);
            Revision revision = revisionLog.append(uid, wl.getGeneration(),
                    RevisionTrigger.ROLLBACK, target.getRevision(), oldSpec, targetSpec);
            wl.setActiveRevisionNo(revision.getRevision());
            return new SubmitResult(wl, true);
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
        revisionLog.purge(uid);
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

    public record SubmitResult(Workload workload, boolean changed) {}

    // ===== 进程重启恢复 =====

    /** 仅供持久层使用：当前 resourceVersion 序列值。 */
    public long rvSequenceForSnapshot() {
        return rvSeq.get();
    }

    /** 进程重启后按磁盘快照整体恢复权威状态（锁对象重新建立）。 */
    public void restoreFromSnapshot(long rvSequence, List<Workload> restoredWorkloads,
                                    List<InstanceRecord> restoredInstances) {
        workloads.clear();
        instances.clear();
        workloadLocks.clear();
        rvSeq.set(rvSequence);
        for (Workload wl : restoredWorkloads) {
            workloads.put(wl.getUid(), wl);
            workloadLocks.put(wl.getUid(), new Object());
        }
        for (InstanceRecord r : restoredInstances) {
            instances.put(r.getId(), r);
        }
    }
}
