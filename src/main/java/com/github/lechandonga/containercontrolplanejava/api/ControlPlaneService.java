package com.github.lechandonga.containercontrolplanejava.api;

import com.github.lechandonga.containercontrolplanejava.control.ControlLoop;
import com.github.lechandonga.containercontrolplanejava.event.EventLog;
import com.github.lechandonga.containercontrolplanejava.model.Revision;
import com.github.lechandonga.containercontrolplanejava.model.RevisionStatus;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;
import com.github.lechandonga.containercontrolplanejava.node.NodeRegistry;
import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;
import com.github.lechandonga.containercontrolplanejava.persist.LocalStateStore;
import com.github.lechandonga.containercontrolplanejava.reconcile.Reconciler;
import com.github.lechandonga.containercontrolplanejava.store.ConflictException;
import com.github.lechandonga.containercontrolplanejava.store.WorkloadStore;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 控制面对外服务门面：所有写操作完成后立即同步驱动一次调谐，
 * 即便控制循环尚未到下一拍，期望状态也会被立刻推进。
 */
@Service
public class ControlPlaneService {

    private final WorkloadStore store;
    private final NodeRegistry nodeRegistry;
    private final Reconciler reconciler;
    private final ControlLoop controlLoop;
    private final EventLog eventLog;
    private final LocalStateStore localStateStore;

    public ControlPlaneService(WorkloadStore store, NodeRegistry nodeRegistry,
                               Reconciler reconciler, ControlLoop controlLoop, EventLog eventLog,
                               LocalStateStore localStateStore) {
        this.store = store;
        this.nodeRegistry = nodeRegistry;
        this.reconciler = reconciler;
        this.controlLoop = controlLoop;
        this.eventLog = eventLog;
        this.localStateStore = localStateStore;
    }

    public Workload createWorkload(String uid, WorkloadSpec spec) {
        Workload wl = store.create(uid, spec);
        eventLog.record(EventLog.BUSINESS_LIFECYCLE, uid, "Created",
                "提交期望状态: replicas=" + spec.replicas() + " image=" + spec.image());
        eventLog.record(EventLog.BUSINESS_RELEASE, uid, "RevisionOpened",
                "修订 r1 创建（CREATE）: replicas=" + spec.replicas() + " image=" + spec.image());
        reconciler.reconcile(uid);
        return wl;
    }

    /**
     * 声明式提交。期望未变则幂等无操作；期望变化必须携带匹配的 resourceVersion。
     */
    public Workload submit(String uid, WorkloadSpec spec, String expectedResourceVersion) {
        WorkloadStore.SubmitResult result = store.submit(uid, spec, expectedResourceVersion);
        if (result.changed()) {
            eventLog.record(EventLog.BUSINESS_LIFECYCLE, uid, "SpecUpdated",
                    "期望状态变更，generation=" + result.workload().getGeneration()
                            + " replicas=" + spec.replicas() + " image=" + spec.image());
            Revision head = result.workload().headRevision();
            eventLog.record(EventLog.BUSINESS_RELEASE, uid, "RevisionOpened",
                    "修订 r" + result.workload().getGeneration()
                            + " 创建（UPDATE）: 变更前 replicas=" + head.getBefore().replicas()
                            + " image=" + head.getBefore().image()
                            + " -> 变更后 replicas=" + head.getAfter().replicas()
                            + " image=" + head.getAfter().image());
        } else {
            eventLog.record(EventLog.BUSINESS_LIFECYCLE, uid, "SpecSubmittedNoop",
                    "重复提交：期望状态与当前一致，幂等忽略，generation 不变");
        }
        reconciler.reconcile(uid);
        return result.workload();
    }

    /**
     * 按修订回滚。{@code targetRevision} 为 null 表示“退回上一个修订”。
     * 并发保护、重复请求幂等、修订留痕规则全部与普通更新一致（由存储层保证）。
     */
    public Workload rollback(String uid, Long targetRevision, String expectedResourceVersion) {
        WorkloadStore.RollbackResult result =
                store.rollback(uid, targetRevision, expectedResourceVersion);
        Revision head = result.workload().headRevision();
        if (result.changed()) {
            eventLog.record(EventLog.BUSINESS_RELEASE, uid, "RollbackRequested",
                    "修订 r" + result.workload().getGeneration()
                            + " 创建（ROLLBACK）: 回到 r" + result.targetRevision().getRevision()
                            + " 声明的 replicas=" + head.getAfter().replicas()
                            + " image=" + head.getAfter().image()
                            + "，作为一条新修订走正常滚动过程");
        } else {
            eventLog.record(EventLog.BUSINESS_RELEASE, uid, "RollbackNoop",
                    "重复回滚请求：目标修订 r" + result.targetRevision().getRevision()
                            + " 的声明与当前期望一致，幂等忽略，不产生修订");
        }
        reconciler.reconcile(uid);
        return result.workload();
    }

    public java.util.List<Revision> listRevisions(String uid) {
        return store.get(uid).getRevisions();
    }

    public Revision getRevision(String uid, long revisionNumber) {
        Revision revision = store.get(uid).findRevision(revisionNumber);
        if (revision == null) {
            throw new com.github.lechandonga.containercontrolplanejava.store.NotFoundException(
                    "工作负载 " + uid + " 的修订 " + revisionNumber);
        }
        return revision;
    }

    public Workload get(String uid) {
        return store.get(uid);
    }

    public List<Workload> list() {
        return store.list();
    }

    public void delete(String uid, String expectedResourceVersion) {
        Workload wl = store.markDeleting(uid, expectedResourceVersion);
        eventLog.record(EventLog.BUSINESS_LIFECYCLE, uid, "DeleteRequested",
                "请求删除工作负载 resourceVersion=" + wl.getResourceVersion());
        reconciler.reconcile(uid);
    }

    public SimulatedNode registerNode(String host, int capacity) {
        SimulatedNode node = nodeRegistry.register(host, capacity);
        eventLog.record(EventLog.BUSINESS_LIFECYCLE, "-", "NodeRegistered",
                "节点 " + node.getInfo().getUid() + " 上线，容量=" + capacity);
        return node;
    }

    public void partitionNode(String nodeUid) {
        SimulatedNode node = nodeRegistry.get(nodeUid).orElseThrow();
        node.injectPartition(true);
        eventLog.record(EventLog.BUSINESS_NODE_FAILURE, "-", "PartitionInjected",
                "向节点 " + nodeUid + " 注入网络分区（心跳与指令均不可达）");
    }

    public void recoverNode(String nodeUid) {
        SimulatedNode node = nodeRegistry.get(nodeUid).orElseThrow();
        node.injectPartition(false);
        nodeRegistry.recover(nodeUid);
        // 恢复后清空残留容器，防止与重建实例脑裂；随后全量调谐。
        reconciler.wipeRecoveredNode(nodeUid);
        eventLog.record(EventLog.BUSINESS_NODE_FAILURE, "-", "NodeRecovered",
                "节点 " + nodeUid + " 网络恢复，残留容器已清空并重新加入集群");
        reconciler.reconcileAll();
    }

    public void failImageOnNode(String nodeUid, String image) {
        SimulatedNode node = nodeRegistry.get(nodeUid).orElseThrow();
        node.injectFailingImage(image);
        eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, "-", "ImageFailureInjected",
                "节点 " + nodeUid + " 被配置为镜像 " + image + " 启动失败");
    }

    /** 供测试/演示确定性地推进控制循环。 */
    public void drive() {
        controlLoop.tick();
    }

    /** 供测试模拟控制面重启。 */
    public void restartControlPlane() {
        controlLoop.restartReconciliation();
    }

    /**
     * 模拟真实“进程停掉再拉起来”：内存权威状态全部丢弃，仅从本地磁盘文件恢复，
     * 随后从中断处继续全量调谐。
     */
    public void restartProcessFromDisk() {
        eventLog.record(EventLog.BUSINESS_RECOVERY, "-", "ProcessRestart",
                "模拟进程重启：丢弃全部内存状态，仅从本地文件恢复期望状态、发布历史与进行中进度");
        localStateStore.simulateProcessRestart();
        reconciler.reconcileAll();
    }

    /** 测试用：下一次状态落盘模拟“写到一半被强杀”。 */
    public void armKillDuringNextWrite() {
        localStateStore.armKillDuringNextWrite();
    }
}
