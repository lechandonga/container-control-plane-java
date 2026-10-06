package com.github.lechandonga.containercontrolplanejava.persistence;

import com.github.lechandonga.containercontrolplanejava.model.Condition;
import com.github.lechandonga.containercontrolplanejava.model.InstanceRecord;
import com.github.lechandonga.containercontrolplanejava.model.InstanceState;
import com.github.lechandonga.containercontrolplanejava.model.NodeInfo;
import com.github.lechandonga.containercontrolplanejava.model.NodeState;
import com.github.lechandonga.containercontrolplanejava.model.UpdatePhase;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadPhase;
import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;
import com.github.lechandonga.containercontrolplanejava.revision.Revision;
import com.github.lechandonga.containercontrolplanejava.revision.RevisionStatus;
import com.github.lechandonga.containercontrolplanejava.revision.RevisionTrigger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 持久化状态的纯数据快照（与领域对象解耦，字段名即落盘格式，保持向前可读）。
 */
public class PersistableState {

    public static final String FORMAT_ID = "CCP-STATE";
    public static final int FORMAT_VERSION = 1;

    public String formatId = FORMAT_ID;
    public int formatVersion = FORMAT_VERSION;
    public String savedAt;
    public long rvSeq;
    public List<WorkloadSnapshot> workloads = new ArrayList<>();
    public List<InstanceSnapshot> instances = new ArrayList<>();
    public List<NodeSnapshot> nodes = new ArrayList<>();
    public List<RevisionEntry> revisions = new ArrayList<>();

    public static class WorkloadSnapshot {
        public String uid;
        public int replicas;
        public String image;
        public int maxSurge;
        public int minAvailable;
        public int maxStartRetries;
        public long rolloutDeadlineSeconds;
        public long generation;
        public String resourceVersion;
        public String phase;
        public String updatePhase;
        public long observedGeneration;
        public int readyReplicas;
        public int updatedReadyReplicas;
        public long nextInstanceSeq;
        public int rolloutAttempts;
        public String rolloutDeadline;
        public String rollbackTargetImage;
        public long rollbackTargetGeneration;
        public String rolloutFailureReason;
        public boolean userRollback;
        public Long activeRevisionNo;
        public Long supersededRevisionNo;
        public boolean cleanupNonTarget;
        public List<ConditionSnapshot> conditions = new ArrayList<>();

        public Workload toWorkload() {
            com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec spec =
                    new com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec(
                            replicas, image, maxSurge, minAvailable, maxStartRetries,
                            rolloutDeadlineSeconds);
            Workload wl = new Workload(uid, spec, generation, resourceVersion);
            wl.setPhase(WorkloadPhase.valueOf(phase));
            wl.setUpdatePhase(UpdatePhase.valueOf(updatePhase));
            wl.setObservedGeneration(observedGeneration);
            wl.setReadyReplicas(readyReplicas);
            wl.setUpdatedReadyReplicas(updatedReadyReplicas);
            wl.setNextInstanceSeq(nextInstanceSeq);
            wl.setRolloutAttempts(rolloutAttempts);
            wl.setRolloutDeadline(rolloutDeadline == null ? null : Instant.parse(rolloutDeadline));
            if (rollbackTargetImage != null) {
                wl.setRollbackTarget(rollbackTargetImage, rollbackTargetGeneration);
            }
            wl.setRolloutFailureReason(rolloutFailureReason);
            wl.setUserRollback(userRollback);
            wl.setActiveRevisionNo(activeRevisionNo);
            wl.setSupersededRevisionNo(supersededRevisionNo);
            wl.setCleanupNonTarget(cleanupNonTarget);
            for (ConditionSnapshot c : conditions) {
                wl.restoreCondition(new Condition(c.type, c.status, c.reason, c.message,
                        c.generation,
                        c.lastTransitionTime == null ? null : Instant.parse(c.lastTransitionTime)));
            }
            return wl;
        }
    }

    public static class ConditionSnapshot {
        public String type;
        public boolean status;
        public String reason;
        public String message;
        public long generation;
        public String lastTransitionTime;
    }

    public static class InstanceSnapshot {
        public String id;
        public String workloadUid;
        public String nodeUid;
        public String image;
        public long generation;
        public String state;
        public int restartCount;
        public String createdAt;

        public InstanceRecord toInstance() {
            return new InstanceRecord(id, workloadUid, nodeUid, image, generation,
                    InstanceState.valueOf(state), restartCount,
                    createdAt == null ? null : Instant.parse(createdAt));
        }
    }

    public static class NodeSnapshot {
        public String uid;
        public String host;
        public int capacity;
        public String state;
        public String lastHeartbeat;
        public boolean partitioned;
        public String failingImage;
        public Map<String, String> containers = new LinkedHashMap<>();

        public SimulatedNode toNode() {
            NodeInfo info = new NodeInfo(uid, host, capacity,
                    lastHeartbeat == null ? null : Instant.parse(lastHeartbeat));
            info.setState(NodeState.valueOf(state));
            SimulatedNode node = new SimulatedNode(info);
            node.restoreFromSnapshot(partitioned, failingImage, containers);
            return node;
        }
    }

    public static class RevisionEntry {
        public String workloadUid;
        public long revision;
        public long generation;
        public String trigger;
        public Long targetRevision;
        public WorkloadSpecSnapshot before;
        public WorkloadSpecSnapshot after;
        public String createdAt;
        public String status;
        public String reason;
        public String finishedAt;

        public Revision toRevision() {
            return new Revision(revision, generation, RevisionTrigger.valueOf(trigger),
                    targetRevision,
                    before == null ? null : before.toSpec(), after.toSpec(),
                    createdAt == null ? null : Instant.parse(createdAt),
                    RevisionStatus.valueOf(status), reason,
                    finishedAt == null ? null : Instant.parse(finishedAt));
        }
    }

    public static class WorkloadSpecSnapshot {
        public int replicas;
        public String image;
        public int maxSurge;
        public int minAvailable;
        public int maxStartRetries;
        public long rolloutDeadlineSeconds;

        public com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec toSpec() {
            return new com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec(
                    replicas, image, maxSurge, minAvailable, maxStartRetries,
                    rolloutDeadlineSeconds);
        }
    }

    // ===== 领域对象 -> 快照 =====

    public static WorkloadSnapshot from(Workload wl) {
        WorkloadSnapshot s = new WorkloadSnapshot();
        s.uid = wl.getUid();
        s.replicas = wl.getSpec().replicas();
        s.image = wl.getSpec().image();
        s.maxSurge = wl.getSpec().maxSurge();
        s.minAvailable = wl.getSpec().minAvailable();
        s.maxStartRetries = wl.getSpec().maxStartRetries();
        s.rolloutDeadlineSeconds = wl.getSpec().rolloutDeadlineSeconds();
        s.generation = wl.getGeneration();
        s.resourceVersion = wl.getResourceVersion();
        s.phase = wl.getPhase().name();
        s.updatePhase = wl.getUpdatePhase().name();
        s.observedGeneration = wl.getObservedGeneration();
        s.readyReplicas = wl.getReadyReplicas();
        s.updatedReadyReplicas = wl.getUpdatedReadyReplicas();
        s.nextInstanceSeq = wl.getNextInstanceSeq();
        s.rolloutAttempts = wl.getRolloutAttempts();
        s.rolloutDeadline = wl.getRolloutDeadline() == null ? null : wl.getRolloutDeadline().toString();
        s.rollbackTargetImage = wl.getRollbackTargetImage();
        s.rollbackTargetGeneration = wl.getRollbackTargetGeneration();
        s.rolloutFailureReason = wl.getRolloutFailureReason();
        s.userRollback = wl.isUserRollback();
        s.activeRevisionNo = wl.getActiveRevisionNo();
        s.supersededRevisionNo = wl.getSupersededRevisionNo();
        s.cleanupNonTarget = wl.isCleanupNonTarget();
        for (Condition c : wl.getConditions()) {
            ConditionSnapshot cs = new ConditionSnapshot();
            cs.type = c.type();
            cs.status = c.status();
            cs.reason = c.reason();
            cs.message = c.message();
            cs.generation = c.generation();
            cs.lastTransitionTime = c.lastTransitionTime() == null
                    ? null : c.lastTransitionTime().toString();
            s.conditions.add(cs);
        }
        return s;
    }

    public static InstanceSnapshot from(InstanceRecord r) {
        InstanceSnapshot s = new InstanceSnapshot();
        s.id = r.getId();
        s.workloadUid = r.getWorkloadUid();
        s.nodeUid = r.getNodeUid();
        s.image = r.getImage();
        s.generation = r.getGeneration();
        s.state = r.getState().name();
        s.restartCount = r.getRestartCount();
        s.createdAt = r.getCreatedAt() == null ? null : r.getCreatedAt().toString();
        return s;
    }

    public static NodeSnapshot from(SimulatedNode node) {
        NodeSnapshot s = new NodeSnapshot();
        NodeInfo info = node.getInfo();
        s.uid = info.getUid();
        s.host = info.getHost();
        s.capacity = info.getCapacity();
        s.state = info.getState().name();
        s.lastHeartbeat = info.getLastHeartbeat() == null ? null : info.getLastHeartbeat().toString();
        s.partitioned = node.isPartitioned();
        s.failingImage = node.failingImageForSnapshot();
        s.containers = new LinkedHashMap<>(node.snapshotContainers());
        return s;
    }

    public static RevisionEntry from(String workloadUid, Revision r) {
        RevisionEntry e = new RevisionEntry();
        e.workloadUid = workloadUid;
        e.revision = r.getRevision();
        e.generation = r.getGeneration();
        e.trigger = r.getTrigger().name();
        e.targetRevision = r.getTargetRevision();
        e.before = r.getBeforeSpec() == null ? null : spec(r.getBeforeSpec());
        e.after = spec(r.getAfterSpec());
        e.createdAt = r.getCreatedAt() == null ? null : r.getCreatedAt().toString();
        e.status = r.getStatus().name();
        e.reason = r.getReason();
        e.finishedAt = r.getFinishedAt() == null ? null : r.getFinishedAt().toString();
        return e;
    }

    private static WorkloadSpecSnapshot spec(com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec sp) {
        WorkloadSpecSnapshot s = new WorkloadSpecSnapshot();
        s.replicas = sp.replicas();
        s.image = sp.image();
        s.maxSurge = sp.maxSurge();
        s.minAvailable = sp.minAvailable();
        s.maxStartRetries = sp.maxStartRetries();
        s.rolloutDeadlineSeconds = sp.rolloutDeadlineSeconds();
        return s;
    }

    // ===== 快照 -> 领域对象 =====
}
