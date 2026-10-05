package com.github.lechandonga.containercontrolplanejava.event;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * 内存事件总线/审计日志。容量受限、仅保留最近事件，测试与 REST 查询都依赖它解释“发生了什么”。
 */
@Component
public class EventLog {

    public static final String BUSINESS_RECONCILE = "RECONCILE";
    public static final String BUSINESS_NODE_FAILURE = "NODE_FAILURE";
    public static final String BUSINESS_ROLLING_UPDATE = "ROLLING_UPDATE";
    public static final String BUSINESS_CONFLICT = "CONCURRENT_CONFLICT";
    public static final String BUSINESS_CAPACITY = "CAPACITY";
    public static final String BUSINESS_LIFECYCLE = "LIFECYCLE";

    private static final int MAX_EVENTS = 2000;

    private final ConcurrentLinkedDeque<ControlPlaneEvent> events = new ConcurrentLinkedDeque<>();

    public void record(ControlPlaneEvent event) {
        events.addLast(event);
        while (events.size() > MAX_EVENTS) {
            events.pollFirst();
        }
    }

    public void record(String business, String workloadUid, String reason, String message) {
        record(ControlPlaneEvent.of(business, workloadUid, reason, message));
    }

    public List<ControlPlaneEvent> all() {
        return new ArrayList<>(events);
    }

    public List<ControlPlaneEvent> byWorkload(String uid) {
        return events.stream().filter(e -> uid.equals(e.workloadUid())).toList();
    }

    /** 仅供测试隔离使用。 */
    public void resetForTests() {
        events.clear();
    }
}
