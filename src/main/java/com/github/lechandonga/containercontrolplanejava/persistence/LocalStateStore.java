package com.github.lechandonga.containercontrolplanejava.persistence;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.lechandonga.containercontrolplanejava.model.InstanceRecord;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.node.NodeRegistry;
import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;
import com.github.lechandonga.containercontrolplanejava.revision.Revision;
import com.github.lechandonga.containercontrolplanejava.revision.RevisionLog;
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
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 单机本地持久化：把控制面的全部权威状态（工作负载、实例、节点与节点容器、发布历史、
 * resourceVersion 序列）落盘到同一个 JSON 状态文件，进程重启后整体装载。
 *
 * <p>容错范围（见 README“本地数据与容错”）：
 * <ul>
 *   <li><b>原子写</b>：先写同目录临时文件再 {@code rename} 原子替换，强杀只会留下
 *       “变更前”或“变更后”两个完整状态之一，绝不允许半截状态生效；</li>
 *   <li><b>格式标识</b>：文件头携带 {@code formatId}/{@code formatVersion}，
 *       格式标识不认识、版本高于当前支持版本，明确报错拒绝启动，不做猜测性迁移；</li>
 *   <li><b>校验和</b>：状态体外层带 SHA-256；校验失败（文件损坏/被截断/被改写）明确报错，
 *       不带病启动；</li>
 *   <li>文件不存在视为首次启动，以空状态正常启动。</li>
 * </ul>
 */
@Component
public class LocalStateStore {

    private static final Logger log = LoggerFactory.getLogger(LocalStateStore.class);
    static final String STATE_FILE_NAME = "control-plane-state.json";

    private final WorkloadStore workloadStore;
    private final NodeRegistry nodeRegistry;
    private final RevisionLog revisionLog;
    private final ClockService clock;
    private final Path stateDir;
    private final ObjectMapper mapper;

    /** 测试用：置 true 后，下一次写状态在“临时文件写好、替换之前”模拟强杀。 */
    private volatile boolean crashBeforeAtomicSwap = false;

    public LocalStateStore(WorkloadStore workloadStore, NodeRegistry nodeRegistry,
                           RevisionLog revisionLog, ClockService clock,
                           @Value("${controlplane.state-dir:./data/control-plane}") String stateDir) {
        this.workloadStore = workloadStore;
        this.nodeRegistry = nodeRegistry;
        this.revisionLog = revisionLog;
        this.clock = clock;
        this.stateDir = Path.of(stateDir);
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(SerializationFeature.INDENT_OUTPUT);
    }

    @PostConstruct
    public void init() {
        try {
            Files.createDirectories(stateDir);
            cleanupStaleTempFiles();
        } catch (IOException e) {
            throw new StatePersistenceException("无法创建/访问本地状态目录 " + stateDir, e);
        }
    }

    /** 装载落盘状态到内存组件。状态文件非法时抛 {@link StatePersistenceException} 拒绝启动。 */
    public synchronized void loadOnStartup() {
        Path stateFile = stateDir.resolve(STATE_FILE_NAME);
        if (!Files.exists(stateFile)) {
            log.info("[PERSISTENCE] 未发现历史状态文件，按首次启动处理: {}", stateFile);
            return;
        }
        StateEnvelope envelope;
        try {
            byte[] raw = Files.readAllBytes(stateFile);
            envelope = mapper.readValue(raw, StateEnvelope.class);
        } catch (IOException e) {
            throw new StatePersistenceException(
                    "本地状态文件无法解析（可能损坏/被截断）: " + stateFile
                            + "。为避免自相矛盾的状态，拒绝启动，请人工核查该文件", e);
        }
        if (!PersistableState.FORMAT_ID.equals(envelope.formatId)) {
            throw new StatePersistenceException("本地状态文件格式标识不认识: formatId="
                    + envelope.formatId + "，期望 " + PersistableState.FORMAT_ID
                    + "，拒绝猜测性加载: " + stateFile);
        }
        if (envelope.formatVersion > PersistableState.FORMAT_VERSION) {
            throw new StatePersistenceException("本地状态文件版本 " + envelope.formatVersion
                    + " 高于当前支持的最高版本 " + PersistableState.FORMAT_VERSION
                    + "（前向不兼容），拒绝启动: " + stateFile);
        }
        if (envelope.payload == null) {
            throw new StatePersistenceException("本地状态文件缺少 payload，文件可能损坏: " + stateFile);
        }
        String expected = checksum(envelope.payload);
        if (!expected.equals(envelope.checksum)) {
            throw new StatePersistenceException("本地状态文件校验和不匹配（数据损坏/被改写）: 期望 "
                    + expected + " 实际 " + envelope.checksum + "，拒绝带病启动: " + stateFile);
        }

        PersistableState state;
        try {
            state = mapper.readValue(envelope.payload, PersistableState.class);
        } catch (IOException e) {
            throw new StatePersistenceException("本地状态 payload 无法解析: " + stateFile, e);
        }
        hydrate(state);
        log.info("[PERSISTENCE] 已从 {} 恢复: 工作负载 {} 个, 实例 {} 个, 节点 {} 个, 修订历史 {} 组",
                stateFile, state.workloads.size(), state.instances.size(),
                state.nodes.size(), state.revisions.stream().map(r -> r.workloadUid).distinct().count());
    }

    private void hydrate(PersistableState state) {
        List<SimulatedNode> nodes = new ArrayList<>();
        for (PersistableState.NodeSnapshot n : state.nodes) {
            nodes.add(n.toNode());
        }
        nodeRegistry.restoreFromSnapshot(nodes);

        List<Workload> workloads = new ArrayList<>();
        for (PersistableState.WorkloadSnapshot w : state.workloads) {
            workloads.add(w.toWorkload());
        }
        List<InstanceRecord> instances = new ArrayList<>();
        for (PersistableState.InstanceSnapshot i : state.instances) {
            instances.add(i.toInstance());
        }
        workloadStore.restoreFromSnapshot(state.rvSeq, workloads, instances);

        Map<String, List<Revision>> revisions = new LinkedHashMap<>();
        for (PersistableState.RevisionEntry e : state.revisions) {
            revisions.computeIfAbsent(e.workloadUid, k -> new ArrayList<>()).add(e.toRevision());
        }
        revisionLog.replaceAll(revisions);
    }

    /** 原子落盘当前全部权威状态。每个改变状态的调谐/提交动作之后调用。 */
    public synchronized void persist() {
        PersistableState state = snapshot();
        StateEnvelope envelope = new StateEnvelope();
        envelope.formatId = PersistableState.FORMAT_ID;
        envelope.formatVersion = PersistableState.FORMAT_VERSION;
        envelope.savedAt = clock.now().toString();
        try {
            byte[] payload = mapper.writeValueAsBytes(state);
            envelope.payload = new String(payload, StandardCharsets.UTF_8);
            envelope.checksum = checksum(envelope.payload);

            Path tmp = stateDir.resolve(STATE_FILE_NAME + ".tmp-" + ProcessHandle.current().pid());
            Files.writeString(tmp, mapper.writeValueAsString(envelope), StandardCharsets.UTF_8);
            if (crashBeforeAtomicSwap) {
                crashBeforeAtomicSwap = false;
                throw new SimulatedCrashException(
                        "模拟强杀：状态临时文件已写好，但尚未原子替换正式状态文件");
            }
            Files.move(tmp, stateDir.resolve(STATE_FILE_NAME),
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (SimulatedCrashException e) {
            throw e;
        } catch (IOException e) {
            throw new StatePersistenceException("本地状态落盘失败: " + stateDir, e);
        }
    }

    private PersistableState snapshot() {
        PersistableState state = new PersistableState();
        state.savedAt = clock.now().toString();
        state.rvSeq = workloadStore.rvSequenceForSnapshot();
        for (Workload wl : workloadStore.list()) {
            state.workloads.add(PersistableState.from(wl));
        }
        for (InstanceRecord r : workloadStore.allInstances().values()) {
            state.instances.add(PersistableState.from(r));
        }
        for (SimulatedNode node : nodeRegistry.all()) {
            state.nodes.add(PersistableState.from(node));
        }
        for (Map.Entry<String, List<Revision>> e : revisionLog.snapshot().entrySet()) {
            for (Revision r : e.getValue()) {
                state.revisions.add(PersistableState.from(e.getKey(), r));
            }
        }
        return state;
    }

    private void cleanupStaleTempFiles() throws IOException {
        if (!Files.isDirectory(stateDir)) {
            return;
        }
        try (var stream = Files.newDirectoryStream(stateDir, STATE_FILE_NAME + ".tmp-*")) {
            for (Path tmp : stream) {
                Files.deleteIfExists(tmp);
            }
        }
    }

    private static String checksum(String payload) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(payload.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (Exception e) {
            throw new StatePersistenceException("无法计算状态校验和", e);
        }
    }

    /** 测试用：下次 {@link #persist()} 在原子替换前抛“强杀”，临时文件保留、正式文件不变。 */
    public void injectCrashBeforeAtomicSwap() {
        this.crashBeforeAtomicSwap = true;
    }

    /** 测试用：删除状态文件（等价于清空本地磁盘）。 */
    public void deleteStateFileForTests() {
        try {
            Files.deleteIfExists(stateDir.resolve(STATE_FILE_NAME));
            cleanupStaleTempFiles();
        } catch (IOException e) {
            throw new StatePersistenceException("清理测试状态文件失败", e);
        }
    }

    /** 测试用：直接写坏正式状态文件，模拟磁盘数据损坏。 */
    public void corruptStateFileForTests(String garbage) {
        try {
            Files.writeString(stateDir.resolve(STATE_FILE_NAME), garbage, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new StatePersistenceException("写坏状态文件失败", e);
        }
    }

    public Path stateFileForTests() {
        return stateDir.resolve(STATE_FILE_NAME);
    }

    /** 磁盘上的外层信封：格式标识、版本、payload 与其校验和。 */
    public static class StateEnvelope {
        public String formatId;
        public int formatVersion;
        public String savedAt;
        public String checksum;
        public String payload;
    }

    /** 模拟“写到一半被强杀”：下一次落盘在原子替换前抛出。 */
    public static class SimulatedCrashException extends RuntimeException {
        public SimulatedCrashException(String message) {
            super(message);
        }
    }

    /** 状态文件缺失之外的一切不可恢复问题：解析失败、版本不认识、校验和不匹配、IO 错误。 */
    public static class StatePersistenceException extends RuntimeException {
        public StatePersistenceException(String message) {
            super(message);
        }

        public StatePersistenceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
