package com.github.lechandonga.containercontrolplanejava.revision;

import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;

import java.time.Instant;

/**
 * 一条不可变（序号/声明内容不可变）的发布修订记录。
 *
 * <p>只有 {@link #status} / {@link #reason} 会随收敛结果从 IN_PROGRESS 迁移到终态一次，
 * 终态后不再改写；工作负载删除时随之一并移除，除此之外任何操作都不能抹掉历史。
 *
 * @param revision       单调递增、连续不跳号的修订序号
 * @param generation     该修订对应的期望 generation
 * @param trigger        修订来源（创建/更新/回滚）
 * @param targetRevision 回滚修订时指向的目标修订序号；非回滚为 null
 * @param beforeSpec     变更前声明（首条修订为 null）
 * @param afterSpec      变更后声明（即本次提交的期望内容）
 * @param createdAt      修订产生时间
 */
public class Revision {
    private final long revision;
    private final long generation;
    private final RevisionTrigger trigger;
    private final Long targetRevision;
    private final WorkloadSpec beforeSpec;
    private final WorkloadSpec afterSpec;
    private final Instant createdAt;
    private volatile RevisionStatus status;
    private volatile String reason;
    private volatile Instant finishedAt;

    public Revision(long revision, long generation, RevisionTrigger trigger, Long targetRevision,
                    WorkloadSpec beforeSpec, WorkloadSpec afterSpec, Instant createdAt,
                    RevisionStatus status, String reason, Instant finishedAt) {
        this.revision = revision;
        this.generation = generation;
        this.trigger = trigger;
        this.targetRevision = targetRevision;
        this.beforeSpec = beforeSpec;
        this.afterSpec = afterSpec;
        this.createdAt = createdAt;
        this.status = status;
        this.reason = reason;
        this.finishedAt = finishedAt;
    }

    public static Revision open(long revision, long generation, RevisionTrigger trigger,
                                Long targetRevision, WorkloadSpec beforeSpec,
                                WorkloadSpec afterSpec, Instant now) {
        return new Revision(revision, generation, trigger, targetRevision, beforeSpec, afterSpec,
                now, RevisionStatus.IN_PROGRESS, null, null);
    }

    /** 终态迁移：只允许从 IN_PROGRESS 迁出一次，杜绝历史被反复改写。 */
    public void finish(RevisionStatus terminal, String reason, Instant now) {
        if (terminal == RevisionStatus.IN_PROGRESS) {
            return;
        }
        // 已成功的修订被一次成功回滚取代时，允许 SUCCEEDED -> ROLLED_BACK 这一次语义迁移；
        // 其余终态（FAILED/ROLLED_BACK）固化，任何操作都不能再改写。
        if (status != RevisionStatus.IN_PROGRESS
                && !(status == RevisionStatus.SUCCEEDED
                        && terminal == RevisionStatus.ROLLED_BACK)) {
            return;
        }
        this.status = terminal;
        this.reason = reason;
        this.finishedAt = now;
    }

    public long getRevision() { return revision; }
    public long getGeneration() { return generation; }
    public RevisionTrigger getTrigger() { return trigger; }
    public Long getTargetRevision() { return targetRevision; }
    public WorkloadSpec getBeforeSpec() { return beforeSpec; }
    public WorkloadSpec getAfterSpec() { return afterSpec; }
    public Instant getCreatedAt() { return createdAt; }
    public RevisionStatus getStatus() { return status; }
    public String getReason() { return reason; }
    public Instant getFinishedAt() { return finishedAt; }
}
