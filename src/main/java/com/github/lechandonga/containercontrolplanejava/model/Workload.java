package com.github.lechandonga.containercontrolplanejava.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 工作负载聚合根：期望状态（spec）+ 观察到的状态（status）。
 */
public class Workload {
    private final String uid;
    private volatile WorkloadSpec spec;
    private volatile long generation;
    private volatile String resourceVersion;

    private volatile WorkloadPhase phase = WorkloadPhase.PENDING;
    private volatile UpdatePhase updatePhase = UpdatePhase.IDLE;
    private volatile long observedGeneration;
    private volatile int readyReplicas;
    private volatile int updatedReadyReplicas;
    private volatile long nextInstanceSeq;
    private volatile int rolloutAttempts;
    private volatile Instant rolloutDeadline;
    /** 非空表示当前处于“失败回退”阶段：实际状态应收敛回该镜像而非 spec.image。 */
    private volatile String rollbackTargetImage;
    private volatile long rollbackTargetGeneration;
    /** 触发本次失败回退的原因码（启动失败/超时），回退完成时用于终态告停。 */
    private volatile String rolloutFailureReason;
    private final List<Condition> conditions = new ArrayList<>();

    public Workload(String uid, WorkloadSpec spec, long generation, String resourceVersion) {
        this.uid = uid;
        this.spec = spec;
        this.generation = generation;
        this.resourceVersion = resourceVersion;
    }

    public String getUid() { return uid; }
    public WorkloadSpec getSpec() { return spec; }
    public void setSpec(WorkloadSpec spec) { this.spec = spec; }
    public long getGeneration() { return generation; }
    public void setGeneration(long generation) { this.generation = generation; }
    public String getResourceVersion() { return resourceVersion; }
    public void setResourceVersion(String rv) { this.resourceVersion = rv; }

    public WorkloadPhase getPhase() { return phase; }
    public void setPhase(WorkloadPhase phase) { this.phase = phase; }
    public UpdatePhase getUpdatePhase() { return updatePhase; }
    public void setUpdatePhase(UpdatePhase updatePhase) { this.updatePhase = updatePhase; }
    public long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(long g) { this.observedGeneration = g; }
    public int getReadyReplicas() { return readyReplicas; }
    public void setReadyReplicas(int readyReplicas) { this.readyReplicas = readyReplicas; }
    public int getUpdatedReadyReplicas() { return updatedReadyReplicas; }
    public void setUpdatedReadyReplicas(int v) { this.updatedReadyReplicas = v; }
    public long getNextInstanceSeq() { return nextInstanceSeq; }
    public void setNextInstanceSeq(long seq) { this.nextInstanceSeq = seq; }
    public long allocateInstanceSeq() { return ++nextInstanceSeq; }
    public int getRolloutAttempts() { return rolloutAttempts; }
    public void setRolloutAttempts(int rolloutAttempts) { this.rolloutAttempts = rolloutAttempts; }
    public Instant getRolloutDeadline() { return rolloutDeadline; }
    public void setRolloutDeadline(Instant rolloutDeadline) { this.rolloutDeadline = rolloutDeadline; }
    public String getRollbackTargetImage() { return rollbackTargetImage; }
    public void setRollbackTarget(String image, long generation) {
        this.rollbackTargetImage = image;
        this.rollbackTargetGeneration = generation;
    }
    public long getRollbackTargetGeneration() { return rollbackTargetGeneration; }
    public void clearRollbackTarget() { this.rollbackTargetImage = null; }
    public String getRolloutFailureReason() { return rolloutFailureReason; }
    public void setRolloutFailureReason(String reason) { this.rolloutFailureReason = reason; }
    public List<Condition> getConditions() {
        synchronized (conditions) { return new ArrayList<>(conditions); }
    }
    public void addCondition(Condition c) {
        synchronized (conditions) {
            conditions.removeIf(existing -> existing.type().equals(c.type()));
            conditions.add(c);
        }
    }
}
