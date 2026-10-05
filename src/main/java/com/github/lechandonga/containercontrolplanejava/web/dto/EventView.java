package com.github.lechandonga.containercontrolplanejava.web.dto;

import com.github.lechandonga.containercontrolplanejava.event.ControlPlaneEvent;

public record EventView(String time, String business, String workloadUid,
                        String reason, String message) {

    public static EventView from(ControlPlaneEvent e) {
        return new EventView(e.time().toString(), e.business(), e.workloadUid(),
                e.reason(), e.message());
    }
}
