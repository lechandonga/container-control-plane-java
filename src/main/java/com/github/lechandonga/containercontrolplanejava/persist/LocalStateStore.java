package com.github.lechandonga.containercontrolplanejava.persist;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.containercontrolplanejava.event.EventLog;
import com.github.lechandonga.containercontrolplanejava.model.InstanceRecord;
import com.github.lechandonga.containercontrolplanejava.model.NodeState;
import com.github.lechandonga.containercontrolplanejava.model.Revision;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.node.NodeRegistry;
import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;
import com.github.lechandonga.containercontrolplanejava.store.WorkloadStore;
import com.github.lechandonga.containercontrolplanejava.time.ClockService;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 本地文件权威状态存储：不依赖任何外部服务。
 *
 * <p>崩溃安全写：{@code state.tmp} 全量写盘（带 SYNC）后原子 {@code ATOMIC_MOVE}
 * 覆盖 {@code state.json}。因此“写到一半被强杀”后重启，磁盘上只可能存在
 * <b>变更前</b>或<b>变更后</b>两份完整快照之一，绝不会出现自相矛盾的半份状态：
 * <ul>
 *   <li>强杀发生在 rename 之前：tmp 是半份、state.json 仍是上一份完整快照 → 恢复到变更前；</li>
 *   <li>强杀发生在 rename 之后：state.json 已是新快照 → 恢复到变更后。</li>
 * </ul>
 *
 * <p>容错策略（启动加载时）：
 * <ul>
 *   <li>无状态文件（首次启动）：空状态正常启动；</li>
 *   <li>格式标识不认识 / 版本不被支持 / 内容损坏无法解析：把问题文件隔离备份为
 *       {@code state.corrupt-<时间戳>.json}，记录 RECOVERY 告警事件，以空状态启动，
 *       绝不带着自相矛盾的数据运行；</li>
 *   <li>残留的半份 tmp 文件启动时直接删除，永不参与恢复。</li>
 * </ul>
 */
@Component
public class LocalStateStore {

    private static final Logger log = LoggerFactory.getLogger(LocalStateStore.class);

    public static final String BUSINESS_RECOVERY = "RECOVERY";

    private final WorkloadStore store;
    private final NodeRegistry nodeRegistry;
    private final ClockService clock;
    private final EventLog eventLog;
    private final ObjectMapper mapper;
    private final Path stateFile;
    private final Path tempFile;

    /** 测试钩子：置位后下一次写入只落半份 tmp 且不执行 rename，模拟强杀发生在写入中途。 */
    private volatile boolean killDuringNextWrite = false;

    public LocalStateStore(WorkloadStore store, NodeRegistry nodeRegistry, ClockService clock,
                           EventLog eventLog,
                           @Value("${controlplane.state-dir:.controlplane-state}") String stateDir) {
        this.store = store;
        this.nodeRegistry = nodeRegistry;
        this.clock = clock;
        this.eventLog = eventLog;
        this.mapper = new ObjectMapper();
        this.mapper.findAndRegisterModules();
        Path dir = Path.of(stateDir);
        this.stateFile = dir.resolve("state.json");
        this.tempFile = dir.resolve("state.tmp");
        store.setStateChangeHook(this::persistSafely);
    }

    @PostConstruct
    public void bootstrap() {
        try {
            Files.createDirectories(stateFile.getParent());
        } catch (IOException e) {
            throw new IllegalStateException("无法创建本地状态目录: " + stateFile.getParent(), e);
        }
        try {
            // 上次强杀可能留下半份临时文件：它永远不会是权威状态，直接清理。
            Files.deleteIfExists(tempFile);
        } catch (IOException ignored) {
            // 删除失败不影响启动；正式写入前会先覆盖该文件。
        }
        restoreFromDisk();
    }

    /** 组装当前内存权威状态的不可变快照（逐工作负载在其调谐锁内取样，保证快照内部一致）。 */
    public StateSnapshot snapshot() {
        List<StateSnapshot.WorkloadSnapshot> workloadSnapshots = new ArrayList<>();
        for (Workload wl : store.workloadsView()) {
            synchronized (store.lockFor(wl.getUid())) {
                workloadSnapshots.add(new StateSnapshot.WorkloadSnapshot(
                        wl.getUid(), wl.getSpec(), wl.getGeneration(), wl.getResourceVersion(),
                        wl.getPhase(), wl.getUpdatePhase(), wl.getObservedGeneration(),
                        wl.getReadyReplicas(), wl.getUpdatedReadyReplicas(),
                        wl.getNextInstanceSeq(), wl.getRolloutAttempts(), wl.getRolloutDeadline(),
                        wl.getRollbackTargetImage(), wl.getRollbackTargetGeneration(),
                        wl.getRolloutFailureReason(), wl.getConditions(),
                        wl.getRevisions().stream().map(this::toRevisionSnapshot).toList()));
            }
        }
        List<StateSnapshot.InstanceSnapshot> instanceSnapshots = new ArrayList<>();
        for (InstanceRecord r : store.allInstances().values()) {
            instanceSnapshots.add(new StateSnapshot.InstanceSnapshot(
                    r.getId(), r.getWorkloadUid(), r.getNodeUid(), r.getImage(),
                    r.getGeneration(), r.getState(), r.getRestartCount(), r.getCreatedAt()));
        }
        List<StateSnapshot.NodeSnapshot> nodeSnapshots = new ArrayList<>();
        for (SimulatedNode node : nodeRegistry.all()) {
            nodeSnapshots.add(new StateSnapshot.NodeSnapshot(
                    node.getInfo().getUid(), node.getInfo().getHost(),
                    node.getInfo().getCapacity(), node.getInfo().getState(),
                    node.getInfo().getLastHeartbeat(), Map.copyOf(node.snapshotContainers())));
        }
        return new StateSnapshot(StateSnapshot.FORMAT_MARKER, StateSnapshot.FORMAT_VERSION,
                clock.now(), store.resourceVersionCounter(),
                workloadSnapshots, instanceSnapshots, nodeSnapshots);
    }

    private StateSnapshot.RevisionSnapshot toRevisionSnapshot(Revision r) {
        return new StateSnapshot.RevisionSnapshot(r.getRevision(), r.getCreatedAt(), r.getKind(),
                r.getBefore(), r.getAfter(), r.getTargetRevision(),
                r.getStatus(), r.getReason(), r.getCompletedAt());
    }

    private void persistSafely() {
        try {
            persist();
        } catch (Exception e) {
            if (e instanceof IllegalStateException illegal
                    && "SIMULATED_KILL_DURING_WRITE".equals(illegal.getMessage())) {
                // 测试钩子：模拟进程在写盘中途被强杀，原样抛出让测试线程感知“进程已死”。
                throw illegal;
            }
            log.error("[RECOVERY] 本地状态落盘失败（下一拍重试，内存仍是权威状态）", e);
        }
    }

    /** 全量写入临时文件后原子替换；任何中途崩溃都不会污染 state.json。 */
    public synchronized void persist() throws IOException {
        byte[] payload = mapper.writeValueAsBytes(snapshot());
        if (killDuringNextWrite) {
            // 模拟“写到一半被强杀”：只落前半截字节，且绝不执行 rename。
            int half = Math.max(1, payload.length / 2);
            Files.write(tempFile, new String(payload, 0, half, StandardCharsets.UTF_8)
                    .getBytes(StandardCharsets.UTF_8));
            killDuringNextWrite = false;
            throw new IllegalStateException("SIMULATED_KILL_DURING_WRITE");
        }
        Files.write(tempFile, payload,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE, StandardOpenOption.SYNC);
        try {
            Files.move(tempFile, stateFile, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException atomicNotSupported) {
            Files.move(tempFile, stateFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /** 从磁盘恢复内存权威状态。文件缺失=首次启动；损坏/不认识=隔离备份后空状态启动。 */
    public synchronized void restoreFromDisk() {
        if (!Files.exists(stateFile)) {
            log.info("[RECOVERY] 未发现本地状态文件，以空状态首次启动: {}", stateFile);
            return;
        }
        StateSnapshot snapshot;
        try {
            byte[] payload = Files.readAllBytes(stateFile);
            snapshot = mapper.readValue(payload, StateSnapshot.class);
            if (snapshot == null
                    || !StateSnapshot.FORMAT_MARKER.equals(snapshot.formatMarker())
                    || snapshot.formatVersion() > StateSnapshot.FORMAT_VERSION) {
                throw new IOException("不认识的格式标识或更高的格式版本: "
                        + (snapshot == null ? "<null>" : snapshot.formatMarker())
                        + " v" + (snapshot == null ? "?" : snapshot.formatVersion()));
            }
        } catch (Exception e) {
            quarantineCorruptState(e);
            return;
        }
        applySnapshot(snapshot);
        log.info("[RECOVERY] 已从本地状态恢复: {} 个工作负载、{} 条实例、{} 个节点、{} 条修订",
                snapshot.workloads().size(), snapshot.instances().size(),
                snapshot.nodes().size(),
                snapshot.workloads().stream().mapToInt(w -> w.revisions().size()).sum());
    }

    /** 模拟“进程重启”：丢弃全部内存状态（含节点容器投影），仅靠磁盘文件重建。 */
    public synchronized void simulateProcessRestart() {
        store.restoreAll(List.of(), List.of());
        nodeRegistry.resetForTests();
        try {
            Files.deleteIfExists(tempFile);
        } catch (IOException ignored) {
            // 继续从 state.json 恢复。
        }
        restoreFromDisk();
    }

    private void applySnapshot(StateSnapshot snapshot) {
        store.restoreResourceVersionCounter(snapshot.resourceVersionCounter());
        List<Workload> workloads = new ArrayList<>();
        for (StateSnapshot.WorkloadSnapshot w : snapshot.workloads()) {
            Workload wl = new Workload(w.uid(), w.spec(), w.generation(), w.resourceVersion());
            wl.setPhase(w.phase());
            wl.setUpdatePhase(w.updatePhase());
            wl.setObservedGeneration(w.observedGeneration());
            wl.setReadyReplicas(w.readyReplicas());
            wl.setUpdatedReadyReplicas(w.updatedReadyReplicas());
            wl.setNextInstanceSeq(w.nextInstanceSeq());
            wl.setRolloutAttempts(w.rolloutAttempts());
            wl.setRolloutDeadline(w.rolloutDeadline());
            if (w.rollbackTargetImage() != null) {
                wl.setRollbackTarget(w.rollbackTargetImage(), w.rollbackTargetGeneration());
            }
            wl.setRolloutFailureReason(w.rolloutFailureReason());
            wl.restoreConditions(w.conditions() == null ? List.of() : w.conditions());
            for (StateSnapshot.RevisionSnapshot r : w.revisions()) {
                wl.restoreRevision(new Revision(r.revision(), r.createdAt(), r.kind(),
                        r.before(), r.after(), r.targetRevision(),
                        r.status(), r.reason(), r.completedAt()));
            }
            workloads.add(wl);
        }
        List<InstanceRecord> instances = new ArrayList<>();
        for (StateSnapshot.InstanceSnapshot i : snapshot.instances()) {
            instances.add(new InstanceRecord(i.id(), i.workloadUid(), i.nodeUid(), i.image(),
                    i.generation(), i.state(), i.restartCount(), i.createdAt()));
        }
        store.restoreAll(workloads, instances);
        for (StateSnapshot.NodeSnapshot n : snapshot.nodes()) {
            nodeRegistry.restoreNode(n.uid(), n.host(), n.capacity(),
                    n.state() == null ? NodeState.HEALTHY : n.state(),
                    n.lastHeartbeat(), n.containers());
        }
    }

    private void quarantineCorruptState(Exception cause) {
        Path backup = stateFile.resolveSibling("state.corrupt-"
                + clock.now().toEpochMilli() + ".json");
        try {
            Files.move(stateFile, backup, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException moveError) {
            log.error("[RECOVERY] 损坏状态文件隔离失败: {}", stateFile, moveError);
        }
        String message = "本地状态文件损坏或格式不被认识，已隔离为 " + backup
                + " 并以空状态启动（拒绝带着自相矛盾的状态运行）。原因: " + cause.getMessage();
        log.error("[RECOVERY] {}", message);
        eventLog.record(BUSINESS_RECOVERY, "-", "CorruptStateQuarantined", message);
    }

    /** 测试用：下一次落盘模拟强杀（半写 + 不 rename）。 */
    public void armKillDuringNextWrite() {
        this.killDuringNextWrite = true;
    }

    /** 测试用：直接把磁盘文件写成无法解析的字节，模拟断电造成的损坏（极端情况）。 */
    public void corruptStateFileForTests(String content) {
        try {
            Files.writeString(stateFile, content, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public Path getStateFile() {
        return stateFile;
    }
}
