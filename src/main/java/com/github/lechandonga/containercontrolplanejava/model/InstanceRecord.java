package com.github.lechandonga.containercontrolplanejava.model;

import java.time.Instant;

/**
 * 一个被管控实例的权威记录（控制面存储为准，节点上的容器是其投影）。
 */
public class InstanceRecord {
    private final String id;
    private final String workloadUid;
    private final String nodeUid;
    private final String image;
    private final long generation;
    private volatile InstanceState state;
    private volatile int restartCount;
    private final Instant createdAt;

    public InstanceRecord(String id, String workloadUid, String nodeUid, String image,
                          long generation, InstanceState state) {
        this(id, workloadUid, nodeUid, image, generation, state, 0, Instant.now());
    }

    /** 进程重启恢复时使用：restartCount 与 createdAt 按落盘值还原。 */
    public InstanceRecord(String id, String workloadUid, String nodeUid, String image,
                          long generation, InstanceState state, int restartCount, Instant createdAt) {
        this.id = id;
        this.workloadUid = workloadUid;
        this.nodeUid = nodeUid;
        this.image = image;
        this.generation = generation;
        this.state = state;
        this.restartCount = restartCount;
        this.createdAt = createdAt;
    }

    public String getId() { return id; }
    public String getWorkloadUid() { return workloadUid; }
    public String getNodeUid() { return nodeUid; }
    public String getImage() { return image; }
    public long getGeneration() { return generation; }
    public InstanceState getState() { return state; }
    public void setState(InstanceState state) { this.state = state; }
    public int getRestartCount() { return restartCount; }
    public void incrementRestartCount() { this.restartCount++; }
    public Instant getCreatedAt() { return createdAt; }

    /** 仅在健康节点上 RUNNING 的实例计入“可用”。 */
    public boolean isAvailable(java.util.function.Predicate<String> healthyNode) {
        return state == InstanceState.RUNNING && healthyNode.test(nodeUid);
    }
}
