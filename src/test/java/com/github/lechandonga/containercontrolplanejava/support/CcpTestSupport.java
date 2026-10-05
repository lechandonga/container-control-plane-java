package com.github.lechandonga.containercontrolplanejava.support;

import com.github.lechandonga.containercontrolplanejava.core.ControlPlane;
import com.github.lechandonga.containercontrolplanejava.core.WorkloadRequest;
import com.github.lechandonga.containercontrolplanejava.model.Instance;
import com.github.lechandonga.containercontrolplanejava.model.InstanceState;
import com.github.lechandonga.containercontrolplanejava.model.Node;
import com.github.lechandonga.containercontrolplanejava.model.NodeStatus;
import com.github.lechandonga.containercontrolplanejava.model.Phase;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.sim.NodeSimulator;
import com.github.lechandonga.containercontrolplanejava.sim.SimulationClock;
import com.github.lechandonga.containercontrolplanejava.store.StateStore;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
import java.util.function.Predicate;

/**
 * 测试夹具：在临时目录上构建一套真实的控制面（存储/时钟/模拟器均为真实组件）。
 */
public final class CcpTestSupport {
    public final ControlPlane cp;
    public final StateStore store;
    public final SimulationClock clock;
    public final NodeSimulator simulator;

    private CcpTestSupport(ControlPlane cp, StateStore store, SimulationClock clock, NodeSimulator simulator) {
        this.cp = cp;
        this.store = store;
        this.clock = clock;
        this.simulator = simulator;
    }

    public static CcpTestSupport create() {
        try {
            var dir = Files.createTempDirectory("ccp-test-");
            StateStore store = new StateStore(dir.resolve("state.json").toString());
            store.init();
            SimulationClock clock = new SimulationClock(false);
            NodeSimulator simulator = new NodeSimulator(1);
            ControlPlane cp = new ControlPlane(store, clock, simulator);
            cp.restore();
            return new CcpTestSupport(cp, store, clock, simulator);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void node(String id, int capacity) {
        cp.registerNode(id, capacity, 1);
    }

    public Workload deploy(String id, String image, String version, int replicas, int minAvailable) {
        return cp.submit(new WorkloadRequest(id, image, version, replicas, minAvailable,
                5, 20, null));
    }

    /** 推进时钟直到该工作负载进入 RUNNING（或超过保护 tick 数）。 */
    public Workload runUntilStable(String id, int maxTicks) {
        for (int i = 0; i < maxTicks; i++) {
            cp.tick(1);
            Workload w = cp.snapshotView().getWorkloads().get(id);
            if (w != null && w.getPhase() == Phase.RUNNING) {
                return w;
            }
        }
        throw new AssertionError("工作负载 " + id + " 在 " + maxTicks + " tick 内未收敛");
    }

    public List<Instance> instances(String id) {
        return List.copyOf(cp.snapshotView().getWorkloads().get(id).getInstances());
    }

    public long runningCount(String id) {
        return instances(id).stream().filter(i -> i.state() == InstanceState.RUNNING).count();
    }

    public long runningCount(String id, String version) {
        return instances(id).stream()
                .filter(i -> i.state() == InstanceState.RUNNING && i.version().equals(version)).count();
    }

    public long instancesOn(String workloadId, String nodeId) {
        return instances(workloadId).stream().filter(i -> i.nodeId().equals(nodeId)).count();
    }

    public void assertCapacityNotExceeded() {
        var nodes = cp.snapshotView().getNodes();
        var used = new java.util.HashMap<String, Integer>();
        for (Workload w : cp.snapshotView().getWorkloads().values()) {
            for (Instance i : w.getInstances()) {
                used.merge(i.nodeId(), 1, Integer::sum);
            }
        }
        for (Node n : nodes.values()) {
            if (n.status() == NodeStatus.READY) {
                int u = used.getOrDefault(n.id(), 0);
                if (u > n.capacity()) {
                    throw new AssertionError("节点 " + n.id() + " 超卖: " + u + " > " + n.capacity());
                }
            }
        }
    }

    public List<String> eventTypes(String workloadId) {
        return cp.snapshotView().getEvents().stream()
                .filter(e -> workloadId.equals(e.workloadId()))
                .map(e -> e.type())
                .toList();
    }

    public boolean hasEvent(String workloadId, Predicate<String> typePredicate) {
        return eventTypes(workloadId).stream().anyMatch(typePredicate);
    }
}
