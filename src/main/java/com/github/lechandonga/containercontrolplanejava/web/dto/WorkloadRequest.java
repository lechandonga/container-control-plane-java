package com.github.lechandonga.containercontrolplanejava.web.dto;

import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;

/**
 * 创建/更新工作负载的请求体。
 * resourceVersion：更新时必带的乐观锁版本；创建时忽略。
 */
public record WorkloadRequest(String uid,
                              Integer replicas,
                              String image,
                              Integer maxSurge,
                              Integer minAvailable,
                              Integer maxStartRetries,
                              Long rolloutDeadlineSeconds,
                              String resourceVersion) {

    public WorkloadSpec toSpec(WorkloadSpec current) {
        int targetReplicas = replicas != null ? replicas : current.replicas();
        String targetImage = image != null ? image : current.image();
        return new WorkloadSpec(
                targetReplicas,
                targetImage,
                maxSurge != null ? maxSurge : current.maxSurge(),
                minAvailable != null ? minAvailable : current.minAvailable(),
                maxStartRetries != null ? maxStartRetries : current.maxStartRetries(),
                rolloutDeadlineSeconds != null ? rolloutDeadlineSeconds : current.rolloutDeadlineSeconds());
    }
}
