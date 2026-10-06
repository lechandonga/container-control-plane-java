package com.github.lechandonga.containercontrolplanejava.node;

import com.github.lechandonga.containercontrolplanejava.model.NodeInfo;
import com.github.lechandonga.containercontrolplanejava.model.NodeState;
import com.github.lechandonga.containercontrolplanejava.time.ClockService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 节点登记与心跳/故障检测。纯单机内存实现，不依赖外部服务。
 */
public class NodeRegistry {

    private final ConcurrentHashMap<String, SimulatedNode> nodes = new ConcurrentHashMap<>();
    private final AtomicLong seq = new AtomicLong();
    private final ClockService clock;

    /** 心跳超过该时长即判定失联。 */
    private volatile long heartbeatTimeoutSeconds = 8;

    public NodeRegistry(ClockService clock) {
        this.clock = clock;
    }

    public void setHeartbeatTimeoutSeconds(long seconds) {
        this.heartbeatTimeoutSeconds = seconds;
    }

    public SimulatedNode register(String host, int capacity) {
        String uid = "node-" + seq.incrementAndGet();
        SimulatedNode node = new SimulatedNode(new NodeInfo(uid, host, capacity, clock.now()));
        nodes.put(uid, node);
        return node;
    }

    public Optional<SimulatedNode> get(String uid) {
        return Optional.ofNullable(nodes.get(uid));
    }

    public List<SimulatedNode> all() {
        return new ArrayList<>(nodes.values());
    }

    /** 仅供测试隔离使用：清空全部节点。 */
    public void resetForTests() {
        nodes.clear();
        seq.set(0);
    }

    /** 仅供进程重启恢复：按磁盘上的权威登记信息重建节点（uid 保持稳定）。 */
    public SimulatedNode restoreNode(String uid, String host, int capacity,
                                    NodeState state, java.time.Instant lastHeartbeat,
                                    java.util.Map<String, String> containers) {
        NodeInfo info = new NodeInfo(uid, host, capacity, lastHeartbeat);
        info.setState(state);
        SimulatedNode node = new SimulatedNode(info);
        node.restoreContainers(containers);
        nodes.put(uid, node);
        long suffix = Long.parseLong(uid.substring("node-".length()));
        seq.updateAndGet(current -> Math.max(current, suffix));
        return node;
    }

    /** 节点上报心跳；被分区的节点无法上报。返回 false 表示心跳未被接受。 */
    public boolean heartbeat(String uid) {
        SimulatedNode node = nodes.get(uid);
        if (node == null || node.isPartitioned()) {
            return false;
        }
        node.getInfo().setLastHeartbeat(clock.now());
        node.getInfo().setState(NodeState.HEALTHY);
        return true;
    }

    /**
     * 故障检测：遍历节点，按心跳超时翻转 HEALTHY -> UNREACHABLE。
     * 一旦判定 UNREACHABLE 即完成 fence，状态保持到节点重新发出心跳。
     *
     * @return 本次新判定为失联的节点（用于触发其上实例的重建）
     */
    public List<SimulatedNode> detectFailures() {
        List<SimulatedNode> newlyLost = new ArrayList<>();
        for (SimulatedNode node : nodes.values()) {
            NodeInfo info = node.getInfo();
            boolean partitioned = node.isPartitioned();
            boolean heartbeatStale = clock.now().getEpochSecond()
                    - info.getLastHeartbeat().getEpochSecond() > heartbeatTimeoutSeconds;
            if (partitioned || heartbeatStale) {
                if (info.getState() == NodeState.HEALTHY) {
                    info.setState(NodeState.UNREACHABLE);
                    newlyLost.add(node);
                }
            }
        }
        return newlyLost;
    }

    /** 恢复分区节点：重新心跳后重新成为健康节点（其上残留容器由控制面清理）。 */
    public void recover(String uid) {
        SimulatedNode node = nodes.get(uid);
        if (node != null) {
            node.injectPartition(false);
            node.getInfo().setLastHeartbeat(clock.now());
            node.getInfo().setState(NodeState.HEALTHY);
        }
    }
}
