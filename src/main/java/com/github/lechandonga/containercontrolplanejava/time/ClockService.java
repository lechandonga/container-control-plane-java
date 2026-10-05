package com.github.lechandonga.containercontrolplanejava.time;

import java.time.Clock;
import java.time.Instant;

/**
 * 统一时钟，便于在测试中精确控制“节点失联超时”“滚动更新超时”等时序。
 */
public class ClockService {

    private volatile Clock clock = Clock.systemUTC();

    public void setClock(Clock clock) {
        this.clock = clock;
    }

    public Instant now() {
        return Instant.now(clock);
    }
}
