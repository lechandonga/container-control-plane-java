package com.github.lechandonga.containercontrolplanejava.persist;

import com.github.lechandonga.containercontrolplanejava.model.Condition;
import com.github.lechandonga.containercontrolplanejava.model.InstanceState;
import com.github.lechandonga.containercontrolplanejava.model.NodeState;
import com.github.lechandonga.containercontrolplanejava.model.RevisionKind;
import com.github.lechandonga.containercontrolplanejava.model.RevisionStatus;
import com.github.lechandonga.containercontrolplanejava.model.UpdatePhase;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadPhase;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * 本地权威状态的磁盘快照（单一 JSON 文件，原子替换）。
 *
 * <p>字段全部以值类型表达，不直接序列化领域聚合，避免磁盘格式与内存实现强耦合；
 * {@link #FORMAT_MARKER}/{@link #FORMAT_VERSION} 用于启动时识别数据归属与兼容范围。
 */
public record StateSnapshot(String formatMarker,
                            int formatVersion,
                            Instant takenAt,
                            long resourceVersionCounter,
                            List<WorkloadSnapshot> workloads,
                            List<InstanceSnapshot> instances,
                            List<NodeSnapshot> nodes) {

    public static final String FORMAT_MARKER = "container-control-plane-local-state";
    public static final int FORMAT_VERSION = 1;

    public record WorkloadSnapshot(String uid,
                                   WorkloadSpec spec,
                                   long generation,
                                   String resourceVersion,
                                   WorkloadPhase phase,
                                   UpdatePhase updatePhase,
                                   long observedGeneration,
                                   int readyReplicas,
                                   int updatedReadyReplicas,
                                   long nextInstanceSeq,
                                   int rolloutAttempts,
                                   Instant rolloutDeadline,
                                   String rollbackTargetImage,
                                   long rollbackTargetGeneration,
                                   String rolloutFailureReason,
                                   List<Condition> conditions,
                                   List<RevisionSnapshot> revisions) {}

    public record RevisionSnapshot(long revision,
                                   Instant createdAt,
                                   RevisionKind kind,
                                   WorkloadSpec before,
                                   WorkloadSpec after,
                                   Long targetRevision,
                                   RevisionStatus status,
                                   String reason,
                                   Instant completedAt) {}

    public record InstanceSnapshot(String id,
                                   String workloadUid,
                                   String nodeUid,
                                   String image,
                                   long generation,
                                   InstanceState state,
                                   int restartCount,
                                   Instant createdAt) {}

    public record NodeSnapshot(String uid,
                               String host,
                               int capacity,
                               NodeState state,
                               Instant lastHeartbeat,
                               Map<String, String> containers) {}
}
