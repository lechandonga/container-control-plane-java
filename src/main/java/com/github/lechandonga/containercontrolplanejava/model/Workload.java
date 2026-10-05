package com.github.lechandonga.containercontrolplanejava.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 工作负载 = 期望状态（spec）+ 观测状态（status）。
 *
 * <p>并发控制：{@code resourceVersion} 是乐观锁版本号，每次被接受的写入 +1。
 * 携带旧版本号的写入会被拒绝（409 CONFLICT），不携带版本号的写入视为强制覆盖。
 */
public final class Workload {
    private String id;
    private String image;
    private String version;
    private int replicas;
    /** 滚动更新期间允许的最低可用实例数。 */
    private int minAvailable;
    /** 新版本单个实例允许的最长启动时间（模拟刻度/tick），超过即判定更新失败。 */
    private long startTimeoutTicks;
    /** 一次滚动更新的总超时（模拟刻度），超过即告停。 */
    private long rolloutTimeoutTicks;

    private long resourceVersion;
    private Phase phase = Phase.PENDING;
    private String reason = "";
    private long observedGeneration;
    private long generation;
    /** 滚动更新开始时的 generation。 */
    private long rolloutGeneration = -1;
    /** 正在滚动到的目标版本（更新期间与 {@link #version} 不同）。 */
    private String targetVersion;
    private long rolloutStartTick = -1;
    private final List<Instance> instances = new ArrayList<>();
    /** 该工作负载实例 ID 的单调序号。 */
    private long instanceSequence;

    public Workload() {
    }

    public Workload(String id, String image, String version, int replicas,
                    int minAvailable, long startTimeoutTicks, long rolloutTimeoutTicks) {
        this.id = id;
        this.image = image;
        this.version = version;
        this.replicas = replicas;
        this.minAvailable = minAvailable;
        this.startTimeoutTicks = startTimeoutTicks;
        this.rolloutTimeoutTicks = rolloutTimeoutTicks;
        this.generation = 1;
        this.observedGeneration = 0;
        this.targetVersion = version;
        this.resourceVersion = 1;
    }

    /** 应用一次被接受的期望状态写入；内容真正变化时 generation +1。返回是否发生变化。 */
    public boolean applySpec(String newImage, String newVersion, int newReplicas,
                             int newMinAvailable, long newStartTimeout, long newRolloutTimeout) {
        boolean changed = !eq(image, newImage) || !eq(version, newVersion)
                || replicas != newReplicas || minAvailable != newMinAvailable
                || startTimeoutTicks != newStartTimeout || rolloutTimeoutTicks != newRolloutTimeout;
        this.image = newImage;
        this.replicas = newReplicas;
        this.minAvailable = newMinAvailable;
        this.startTimeoutTicks = newStartTimeout;
        this.rolloutTimeoutTicks = newRolloutTimeout;
        // version 是“已生效版本”，只能由滚动更新成功时提交；期望的新版本写入 targetVersion。
        if (!eq(this.version, newVersion)) {
            this.targetVersion = newVersion;
        }
        if (changed) {
            generation++;
            resourceVersion++;
        }
        return changed;
    }

    public void bumpResourceVersion() {
        resourceVersion++;
    }

    private static boolean eq(String a, String b) {
        return a == null ? b == null : a.equals(b);
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getImage() { return image; }
    public void setImage(String image) { this.image = image; }
    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }
    public int getReplicas() { return replicas; }
    public void setReplicas(int replicas) { this.replicas = replicas; }
    public int getMinAvailable() { return minAvailable; }
    public void setMinAvailable(int minAvailable) { this.minAvailable = minAvailable; }
    public long getStartTimeoutTicks() { return startTimeoutTicks; }
    public void setStartTimeoutTicks(long startTimeoutTicks) { this.startTimeoutTicks = startTimeoutTicks; }
    public long getRolloutTimeoutTicks() { return rolloutTimeoutTicks; }
    public void setRolloutTimeoutTicks(long rolloutTimeoutTicks) { this.rolloutTimeoutTicks = rolloutTimeoutTicks; }
    public long getResourceVersion() { return resourceVersion; }
    public void setResourceVersion(long resourceVersion) { this.resourceVersion = resourceVersion; }
    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
    public long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(long observedGeneration) { this.observedGeneration = observedGeneration; }
    public long getGeneration() { return generation; }
    public void setGeneration(long generation) { this.generation = generation; }
    public long getRolloutGeneration() { return rolloutGeneration; }
    public void setRolloutGeneration(long rolloutGeneration) { this.rolloutGeneration = rolloutGeneration; }
    public String getTargetVersion() { return targetVersion; }
    public void setTargetVersion(String targetVersion) { this.targetVersion = targetVersion; }
    public long getRolloutStartTick() { return rolloutStartTick; }
    public void setRolloutStartTick(long rolloutStartTick) { this.rolloutStartTick = rolloutStartTick; }
    public List<Instance> getInstances() { return instances; }
    public long getInstanceSequence() { return instanceSequence; }
    public void setInstanceSequence(long instanceSequence) { this.instanceSequence = instanceSequence; }
}
