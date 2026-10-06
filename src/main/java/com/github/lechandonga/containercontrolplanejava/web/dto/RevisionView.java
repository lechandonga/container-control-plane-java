package com.github.lechandonga.containercontrolplanejava.web.dto;

import com.github.lechandonga.containercontrolplanejava.model.Revision;

import java.util.List;

/**
 * 发布修订视图：修订序号、时间、变更前后声明、来源、终态与原因。
 */
public record RevisionView(long revision,
                           String createdAt,
                           String kind,
                           Long targetRevision,
                           SpecView before,
                           SpecView after,
                           String status,
                           String reason,
                           String completedAt) {

    public record SpecView(int replicas, String image, int maxSurge, int minAvailable,
                           int maxStartRetries, long rolloutDeadlineSeconds) {
        static SpecView of(com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec spec) {
            return spec == null ? null : new SpecView(spec.replicas(), spec.image(),
                    spec.maxSurge(), spec.minAvailable(), spec.maxStartRetries(),
                    spec.rolloutDeadlineSeconds());
        }
    }

    public static RevisionView from(Revision r) {
        return new RevisionView(r.getRevision(), r.getCreatedAt().toString(), r.getKind().name(),
                r.getTargetRevision(), SpecView.of(r.getBefore()), SpecView.of(r.getAfter()),
                r.getStatus().name(), r.getReason(),
                r.getCompletedAt() == null ? null : r.getCompletedAt().toString());
    }

    public static List<RevisionView> list(java.util.List<Revision> revisions) {
        return revisions.stream().map(RevisionView::from).toList();
    }
}
