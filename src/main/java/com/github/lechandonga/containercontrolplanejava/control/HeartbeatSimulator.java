package com.github.lechandonga.containercontrolplanejava.control;

import com.github.lechandonga.containercontrolplanejava.node.NodeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 模拟节点周期性心跳。被注入网络分区的节点心跳会被 NodeRegistry 拒绝，
 * 从而在超时后被故障检测器判定失联。
 */
@Component
public class HeartbeatSimulator implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatSimulator.class);

    private final NodeRegistry nodeRegistry;
    private final ScheduledExecutorService executor;
    private final long intervalMs;

    public HeartbeatSimulator(NodeRegistry nodeRegistry,
                              com.github.lechandonga.containercontrolplanejava.config.ControlPlaneProperties props) {
        this.nodeRegistry = nodeRegistry;
        this.intervalMs = props.heartbeatIntervalMs();
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "node-heartbeat");
            t.setDaemon(true);
            return t;
        });
        this.executor.scheduleAtFixedRate(this::beat, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void beat() {
        for (var node : nodeRegistry.all()) {
            nodeRegistry.heartbeat(node.getInfo().getUid());
        }
    }

    @Override
    public void destroy() {
        executor.shutdownNow();
    }
}
