package com.github.lechandonga.containercontrolplanejava.revision;

import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;
import com.github.lechandonga.containercontrolplanejava.time.ClockService;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 每个工作负载的发布历史。
 *
 * <p>规则：
 * <ul>
 *   <li>只有“真正改变期望状态”的提交才会 {@link #append} 一条修订；重复提交、冲突被拒不入历史，
       因此修订号严格连续、绝不跳号；</li>
 *   <li>历史只增不改：序号、前后声明、时间永久固定，终态只迁移一次；</li>
 *   <li>工作负载删除（purge）时历史一并移除，这是唯一允许的抹除路径；</li>
 *   <li>没有更新/删除单条修订的接口。</li>
 * </ul>
 */
@Component
public class RevisionLog {

    private final ConcurrentMap<String, List<Revision>> byWorkload = new ConcurrentHashMap<>();
    private final ClockService clock;

    public RevisionLog(ClockService clock) {
        this.clock = clock;
    }

    public Revision append(String workloadUid, long generation, RevisionTrigger trigger,
                           Long targetRevision, WorkloadSpec beforeSpec, WorkloadSpec afterSpec) {
        List<Revision> history = byWorkload.computeIfAbsent(workloadUid, k -> new ArrayList<>());
        Revision revision;
        synchronized (history) {
            long next = history.isEmpty() ? 1 : history.get(history.size() - 1).getRevision() + 1;
            revision = Revision.open(next, generation, trigger, targetRevision,
                    beforeSpec, afterSpec, clock.now());
            history.add(revision);
        }
        return revision;
    }

    public List<Revision> list(String workloadUid) {
        List<Revision> history = byWorkload.get(workloadUid);
        if (history == null) {
            return List.of();
        }
        synchronized (history) {
            return new ArrayList<>(history);
        }
    }

    public Optional<Revision> get(String workloadUid, long revisionNo) {
        return list(workloadUid).stream()
                .filter(r -> r.getRevision() == revisionNo).findFirst();
    }

    /** 最近一条修订（用于“退回上一个修订”）。 */
    public Optional<Revision> latest(String workloadUid) {
        List<Revision> history = byWorkload.get(workloadUid);
        if (history == null) {
            return Optional.empty();
        }
        synchronized (history) {
            return history.isEmpty()
                    ? Optional.empty() : Optional.of(history.get(history.size() - 1));
        }
    }

    public Optional<Revision> byGeneration(String workloadUid, long generation) {
        return list(workloadUid).stream()
                .filter(r -> r.getGeneration() == generation).findFirst();
    }

    public void purge(String workloadUid) {
        byWorkload.remove(workloadUid);
    }

    /** 进程重启后整体替换为持久层读出的历史。 */
    public void replaceAll(Map<String, List<Revision>> restored) {
        byWorkload.clear();
        byWorkload.putAll(restored);
    }

    public Map<String, List<Revision>> snapshot() {
        return new ConcurrentHashMap<>(byWorkload);
    }

    public void resetForTests() {
        byWorkload.clear();
    }
}
