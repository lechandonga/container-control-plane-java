package com.github.lechandonga.containercontrolplanejava.model;

import java.time.Instant;

/**
 * 一条不可变事实 + 可变终态的发布修订记录。
 *
 * <p>修订序号（revision）等于该次提交生效后的 {@code spec.generation}：
 * 只有真正改变期望状态的提交（创建/更新/回滚）才会产生修订并取号，
 * 重复提交、冲突被拒不写历史也不跳号。
 *
 * <p>不可变事实：序号、时间、来源、变更前后声明、触发来源。
 * 可变部分仅 {@link #status} 与 {@link #reason}/{@link #completedAt}：
 * 由调谐循环在该修订走向终态（成功/失败/被回退/被取代）时写入一次，
 * 此后任何操作都不得改写或抹掉（工作负载删除除外）。
 */
public class Revision {
    private final long revision;
    private final Instant createdAt;
    private final RevisionKind kind;
    private final WorkloadSpec before;
    private final WorkloadSpec after;
    /** ROLLBACK 修订：回滚目标修订号；其它来源为 null。 */
    private final Long targetRevision;

    private volatile RevisionStatus status;
    private volatile String reason;
    private volatile Instant completedAt;

    public Revision(long revision, Instant createdAt, RevisionKind kind,
                    WorkloadSpec before, WorkloadSpec after, Long targetRevision,
                    RevisionStatus status, String reason, Instant completedAt) {
        this.revision = revision;
        this.createdAt = createdAt;
        this.kind = kind;
        this.before = before;
        this.after = after;
        this.targetRevision = targetRevision;
        this.status = status;
        this.reason = reason;
        this.completedAt = completedAt;
    }

    public static Revision open(long revision, Instant createdAt, RevisionKind kind,
                                WorkloadSpec before, WorkloadSpec after, Long targetRevision) {
        return new Revision(revision, createdAt, kind, before, after, targetRevision,
                RevisionStatus.IN_PROGRESS, null, null);
    }

    /** 终态写入：仅允许从 IN_PROGRESS 迁移，写入后不可再改。 */
    public void complete(RevisionStatus terminal, String reason, Instant at) {
        if (this.status != RevisionStatus.IN_PROGRESS) {
            return;
        }
        this.status = terminal;
        this.reason = reason;
        this.completedAt = at;
    }

    /**
     * 仅供权威存储在“显式回滚”成立时改写被撤销修订的事实状态：
     * 该修订确实成功过或正在发布，但一次带乐观锁保护的显式回滚把它撤销了，
     * 这是一次新的、更强的历史事实。仅允许 SUCCEEDED / IN_PROGRESS 迁移，
     * FAILED / SUPERSEDED / ROLLED_BACK 的既有事实不允许被覆盖。
     */
    public void markRolledBack(String reason, Instant at) {
        if (this.status != RevisionStatus.SUCCEEDED
                && this.status != RevisionStatus.IN_PROGRESS) {
            return;
        }
        this.status = RevisionStatus.ROLLED_BACK;
        this.reason = reason;
        this.completedAt = at;
    }

    public long getRevision() { return revision; }
    public Instant getCreatedAt() { return createdAt; }
    public RevisionKind getKind() { return kind; }
    public WorkloadSpec getBefore() { return before; }
    public WorkloadSpec getAfter() { return after; }
    public Long getTargetRevision() { return targetRevision; }
    public RevisionStatus getStatus() { return status; }
    public String getReason() { return reason; }
    public Instant getCompletedAt() { return completedAt; }
}
