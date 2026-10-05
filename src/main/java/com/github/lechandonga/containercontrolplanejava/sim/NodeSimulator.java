package com.github.lechandonga.containercontrolplanejava.sim;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Component;

/**
 * 本地节点/容器运行时模拟器（不依赖任何外部服务）。
 *
 * <p>维护节点的易失“运行时”状态（重启控制面后节点需要重新注册/置备）：
 * <ul>
 *   <li>每个实例需要经过 {@code startupTicks} 个时钟刻度才 READY；</li>
 *   <li>可按 (node, image, version) 注入“某个镜像版本启动失败”，用于复现坏版本滚动失败；</li>
 *   <li>节点失联（NOT_READY）后其上所有实例视为死亡。</li>
 * </ul>
 */
@Component
public class NodeSimulator {
    /** key = nodeId，value = 该节点上实例启动需要的 tick 数。 */
    private final Map<String, Integer> startupTicksByNode = new HashMap<>();
    /** 在该节点上永远启动失败的 image:version 集合（模拟坏版本）。 */
    private final Map<String, Set<String>> failingRefsByNode = new HashMap<>();
    private final int defaultStartupTicks;

    public NodeSimulator(@org.springframework.beans.factory.annotation.Value("${ccp.startup-ticks:1}") int defaultStartupTicks) {
        this.defaultStartupTicks = defaultStartupTicks;
    }

    public synchronized void registerNode(String nodeId, Integer startupTicks) {
        startupTicksByNode.put(nodeId, startupTicks == null ? defaultStartupTicks : startupTicks);
    }

    public synchronized void removeNode(String nodeId) {
        startupTicksByNode.remove(nodeId);
        failingRefsByNode.remove(nodeId);
    }

    /** 在指定节点上注入某镜像版本的启动失败（模拟坏版本/镜像拉取失败）。 */
    public synchronized void injectImageFailure(String nodeId, String image, String version) {
        failingRefsByNode.computeIfAbsent(nodeId, k -> new HashSet<>()).add(ref(image, version));
    }

    public synchronized void clearImageFailure(String nodeId, String image, String version) {
        Set<String> set = failingRefsByNode.get(nodeId);
        if (set != null) {
            set.remove(ref(image, version));
        }
    }

    private static String ref(String image, String version) {
        return image + ":" + version;
    }

    /**
     * 计算一个已创建实例在给定时刻的运行状态。
     *
     * @param nodeReady      承载节点当前是否健康
     * @param bornTick       实例创建时刻
     * @param now            当前时钟
     * @param nodeId         节点 ID
     * @param image          镜像名
     * @param version        镜像版本（失败注入以 image:version 为键，只让坏版本失败）
     */
    public synchronized InstanceOutcome observe(boolean nodeReady, long bornTick, long now,
                                                String nodeId, String image, String version) {
        if (!nodeReady) {
            return InstanceOutcome.LOST;
        }
        Set<String> failing = failingRefsByNode.get(nodeId);
        if (failing != null && failing.contains(ref(image, version))) {
            return InstanceOutcome.FAILS_TO_START;
        }
        int need = startupTicksByNode.getOrDefault(nodeId, defaultStartupTicks);
        return now - bornTick + 1 >= need ? InstanceOutcome.RUNNING : InstanceOutcome.STARTING;
    }

    public enum InstanceOutcome { STARTING, RUNNING, FAILS_TO_START, LOST }
}
