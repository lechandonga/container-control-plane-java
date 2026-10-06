package com.github.lechandonga.containercontrolplanejava.web.dto;

import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;
import com.github.lechandonga.containercontrolplanejava.revision.Revision;

/**
 * 发布修订视图：序号、时间、变更前后声明、结果（成功/失败/被回退/进行中）与原因。
 */
public record RevisionView(long revision,
                           long generation,
                           String trigger,
                           Long targetRevision,
                           SpecView before,
                           SpecView after,
                           String createdAt,
                           String status,
                           String reason,
                           String finishedAt) {

    public static RevisionView from(Revision r) {
        return new RevisionView(
                r.getRevision(),
                r.getGeneration(),
                r.getTrigger().name(),
                r.getTargetRevision(),
                r.getBeforeSpec() == null ? null : SpecView.from(r.getBeforeSpec()),
                SpecView.from(r.getAfterSpec()),
                r.getCreatedAt() == null ? null : r.getCreatedAt().toString(),
                r.getStatus().name(),
                r.getReason(),
                r.getFinishedAt() == null ? null : r.getFinishedAt().toString());
    }

    public record SpecView(int replicas,
                           String image,
                           int maxSurge,
                           int minAvailable,
                           int maxStartRetries,
                           long rolloutDeadlineSeconds) {
        static SpecView from(WorkloadSpec spec) {
            return new SpecView(spec.replicas(), spec.image(), spec.maxSurge(),
                    spec.minAvailable(), spec.maxStartRetries(), spec.rolloutDeadlineSeconds());
        }
    }
}
