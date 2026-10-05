package com.github.lechandonga.containercontrolplanejava.web.dto;

import com.github.lechandonga.containercontrolplanejava.model.Condition;
import com.github.lechandonga.containercontrolplanejava.model.Workload;

import java.util.List;

/**
 * 工作负载对外视图：聚合期望状态、实际状态、更新阶段与可解释条件。
 */
public record WorkloadView(String uid,
                           long generation,
                           long observedGeneration,
                           String resourceVersion,
                           String phase,
                           String updatePhase,
                           int desiredReplicas,
                           int readyReplicas,
                           int updatedReadyReplicas,
                           String image,
                           int minAvailable,
                           int maxSurge,
                           String rollbackTarget,
                           List<ConditionView> conditions) {

    public static WorkloadView from(Workload wl) {
        List<ConditionView> conditions = wl.getConditions().stream()
                .map(c -> new ConditionView(c.type(), c.status(), c.reason(),
                        c.message(), c.generation(), c.lastTransitionTime().toString()))
                .toList();
        return new WorkloadView(
                wl.getUid(),
                wl.getGeneration(),
                wl.getObservedGeneration(),
                wl.getResourceVersion(),
                wl.getPhase().name(),
                wl.getUpdatePhase().name(),
                wl.getSpec().replicas(),
                wl.getReadyReplicas(),
                wl.getUpdatedReadyReplicas(),
                wl.getSpec().image(),
                wl.getSpec().minAvailable(),
                wl.getSpec().maxSurge(),
                wl.getRollbackTargetImage(),
                conditions);
    }

    public record ConditionView(String type, boolean status, String reason,
                                String message, long generation, String lastTransitionTime) {}
}
