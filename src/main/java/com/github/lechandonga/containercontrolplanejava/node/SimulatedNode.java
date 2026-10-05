package com.github.lechandonga.containercontrolplanejava.node;

import com.github.lechandonga.containercontrolplanejava.model.NodeInfo;

import java.util.HashMap;
import java.util.Map;

/**
 * 本地模拟节点：真实承载工作负载的“机器”。
 *
 * <p>语义要点：
 * <ul>
 *   <li>{@link #startContainer} 与 {@link #stopContainer} 以实例 id 为键，天然幂等：
 *       控制面重启后重复下发不会产生重复容器，计数不漂移。</li>
 *   <li>节点可被注入 partition（失联）：失联期间所有调用静默失败、心跳停止，
 *       模拟真实网络分区，控制面必须自行 fence 并在别处重建。</li>
 *   <li>可注入镜像启动失败，用于驱动滚动更新的失败/回退分支。</li>
 * </ul>
 */
public class SimulatedNode {

    private final NodeInfo info;
    private final Map<String, String> runningContainers = new HashMap<>();

    private volatile boolean partitioned = false;
    private volatile String failingImage = null;

    public SimulatedNode(NodeInfo info) {
        this.info = info;
    }

    public NodeInfo getInfo() {
        return info;
    }

    /** 注入网络分区：节点“活着”但控制面无法触达。 */
    public void injectPartition(boolean partitioned) {
        this.partitioned = partitioned;
    }

    public boolean isPartitioned() {
        return partitioned;
    }

    /** 指定镜像在本节点（以及所有读取该配置的判定处）启动失败。 */
    public void injectFailingImage(String image) {
        this.failingImage = image;
    }

    public boolean imageFailsOnNode(String image) {
        return partitioned || (failingImage != null && failingImage.equals(image));
    }

    /**
     * 在节点上启动容器。幂等：同一 instanceId 重复调用不会叠加计数。
     *
     * @return true 表示节点上存在该容器且运行成功；false 表示节点不可达或镜像启动失败
     */
    public synchronized boolean startContainer(String instanceId, String image) {
        if (partitioned) {
            return false;
        }
        if (failingImage != null && failingImage.equals(image)) {
            runningContainers.remove(instanceId);
            return false;
        }
        runningContainers.put(instanceId, image);
        return true;
    }

    /** 幂等停止；节点失联时返回 false（控制面不能假设旧容器已停）。 */
    public synchronized boolean stopContainer(String instanceId) {
        if (partitioned) {
            return false;
        }
        runningContainers.remove(instanceId);
        return true;
    }

    public synchronized boolean isContainerRunning(String instanceId) {
        return !partitioned && runningContainers.containsKey(instanceId);
    }

    public synchronized int runningCount() {
        return partitioned ? 0 : runningContainers.size();
    }

    /** 模拟节点恢复：容器在其本地视图中仍然存在（分区没有销毁机器）。 */
    public synchronized Map<String, String> snapshotContainers() {
        return new HashMap<>(runningContainers);
    }

    /** 被 fence 后若节点恢复，控制面会清空其残留容器，避免幽灵实例。 */
    public synchronized void wipeAll() {
        runningContainers.clear();
    }
}
