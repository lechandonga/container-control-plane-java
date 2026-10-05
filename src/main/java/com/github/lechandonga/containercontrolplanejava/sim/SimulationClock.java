package com.github.lechandonga.containercontrolplanejava.sim;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 全局模拟时钟。
 *
 * <p>默认 {@code ccp.auto-tick=false}：时间只在显式调用 {@code POST /api/sim/tick} 时前进，
 * 因此所有测试（包括超时、故障注入）都是确定性的，不依赖真实等待。
 * 生产演示时可打开 auto-tick，由后台调度器按固定间隔推动调谐。
 */
@Component
public class SimulationClock {
    private long tick;
    private final boolean autoTick;

    public SimulationClock(@Value("${ccp.auto-tick:false}") boolean autoTick) {
        this.autoTick = autoTick;
    }

    public synchronized long now() {
        return tick;
    }

    public synchronized long advance(long steps) {
        tick += steps;
        return tick;
    }

    public boolean isAutoTick() {
        return autoTick;
    }
}
