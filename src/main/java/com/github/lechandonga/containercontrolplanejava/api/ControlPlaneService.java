package com.github.lechandonga.containercontrolplanejava.api;

import com.github.lechandonga.containercontrolplanejava.control.ControlLoop;
import com.github.lechandonga.containercontrolplanejava.event.EventLog;
import com.github.lechandonga.containercontrolplanejava.persistence.LocalStateStore;
import com.github.lechandonga.containercontrolplanejava.revision.Revision;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;
import com.github.lechandonga.containercontrolplanejava.node.NodeRegistry;
import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;
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
    private final LocalStateStore persistence;
    private final com.github.lechandonga.containercontrolplanejava.revision.RevisionLog revisionLog;

    public ControlPlaneService(WorkloadStore store, NodeRegistry nodeRegistry,
                               Reconciler reconciler, ControlLoop controlLoop, EventLog eventLog,
                               LocalStateStore persistence,
                               com.github.lechandonga.containercontrolplanejava.revision.RevisionLog revisionLog) {
        this.store = store;
        this.nodeRegistry = nodeRegistry;
        this.reconciler = reconciler;
        this.controlLoop = controlLoop;
        this.eventLog = eventLog;
        this.persistence = persistence;
        this.revisionLog = revisionLog;
    }

    public Workload createWorkload(String uid, WorkloadSpec spec) {
        Workload wl = store.create(uid, spec);
        eventLog.record(EventLog.BUSINESS_LIFECYCLE, uid, "Created",
                "提交期望状态: replicas=" + spec.replicas() + " image=" + spec.image());
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
        } else {
            eventLog.record(EventLog.BUSINESS_LIFECYCLE, uid, "SpecSubmittedNoop",
                    "重复提交：期望状态与当前一致，幂等忽略，generation 不变");
        }
        reconciler.reconcile(uid);
        return result.workload();
    }

    /**
     * 按修订回滚。{@code targetRevisionNo} 为 null 表示“退回上一个修订”。
     * 并发保护、重复提交语义与普通更新完全一致；回滚作为一条新修订进入发布历史。
     */
    public Workload rollback(String uid, Long targetRevisionNo, String expectedResourceVersion) {
        WorkloadStore.SubmitResult result = store.rollback(uid, targetRevisionNo, expectedResourceVersion);
        if (result.changed()) {
            Revision active = revisionLog.latest(uid).orElseThrow();
            eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, uid, "RollbackRequested",
                    "请求回滚到修订 " + (targetRevisionNo == null ? "<上一个>" : targetRevisionNo)
                            + "，生成新修订 " + active.getRevision()
                            + "，目标声明 replicas=" + active.getAfterSpec().replicas()
                            + " image=" + active.getAfterSpec().image());
        } else {
            eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, uid, "RollbackNoop",
                    "回滚目标声明与当前期望一致，幂等忽略，不产生修订");
        }
        reconciler.reconcile(uid);
        return result.workload();
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
        persistence.persist();
        return node;
    }

    public void partitionNode(String nodeUid) {
        SimulatedNode node = nodeRegistry.get(nodeUid).orElseThrow();
        node.injectPartition(true);
        eventLog.record(EventLog.BUSINESS_NODE_FAILURE, "-", "PartitionInjected",
                "向节点 " + nodeUid + " 注入网络分区（心跳与指令均不可达）");
        persistence.persist();
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
        persistence.persist();
    }

    public void failImageOnNode(String nodeUid, String image) {
        SimulatedNode node = nodeRegistry.get(nodeUid).orElseThrow();
        node.injectFailingImage(image);
        eventLog.record(EventLog.BUSINESS_ROLLING_UPDATE, "-", "ImageFailureInjected",
                "节点 " + nodeUid + " 被配置为镜像 " + image + " 启动失败");
        persistence.persist();
    }

    /** 供测试/演示确定性地推进控制循环。 */
    public void drive() {
        controlLoop.tick();
    }

    /**
     * 模拟整个控制面进程重启：先从本地状态文件把权威状态（期望、历史、进行中进度、
     * 节点与容器视图）装回内存，再从中断处继续全量调谐。
     */
    public void restartControlPlane() {
        persistence.loadOnStartup();
        controlLoop.restartReconciliation();
    }
}
