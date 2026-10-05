package com.github.lechandonga.containercontrolplanejava.event;

import java.time.Instant;

/**
 * 控制面对外可解释的事件：每一次创建/驱逐/重建/版本切换/告停都留痕。
 */
public record ControlPlaneEvent(Instant time,
                                String business,
                                String workloadUid,
                                String reason,
                                String message) {

    public static ControlPlaneEvent of(String business, String workloadUid, String reason, String message) {
        return new ControlPlaneEvent(Instant.now(), business, workloadUid, reason, message);
    }
}
