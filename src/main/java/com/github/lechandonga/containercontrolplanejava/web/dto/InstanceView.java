package com.github.lechandonga.containercontrolplanejava.web.dto;

import com.github.lechandonga.containercontrolplanejava.model.InstanceRecord;

public record InstanceView(String id,
                           String workloadUid,
                           String nodeUid,
                           String image,
                           long generation,
                           String state,
                           String createdAt) {

    public static InstanceView from(InstanceRecord r) {
        return new InstanceView(r.getId(), r.getWorkloadUid(), r.getNodeUid(),
                r.getImage(), r.getGeneration(), r.getState().name(), r.getCreatedAt().toString());
    }
}
