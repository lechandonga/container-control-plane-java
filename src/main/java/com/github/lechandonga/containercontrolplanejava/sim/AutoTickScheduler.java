package com.github.lechandonga.containercontrolplanejava.sim;

import com.github.lechandonga.containercontrolplanejava.core.ControlPlane;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 自动调谐：仅在 ccp.auto-tick=true 时启用，按固定间隔推进虚拟时钟并调谐。
 * 默认关闭，保证单元测试完全确定性。
 */
@Component
@ConditionalOnProperty(name = "ccp.auto-tick", havingValue = "true")
public class AutoTickScheduler {
    private final ControlPlane controlPlane;

    public AutoTickScheduler(ControlPlane controlPlane) {
        this.controlPlane = controlPlane;
    }

    @Scheduled(fixedDelayString = "${ccp.tick-interval-ms:1000}")
    public void tick() {
        controlPlane.tick(1);
    }
}
