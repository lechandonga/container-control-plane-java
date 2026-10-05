package com.github.lechandonga.containercontrolplanejava.store;

import com.github.lechandonga.containercontrolplanejava.model.Event;
import com.github.lechandonga.containercontrolplanejava.model.Node;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 控制面的完整可持久化状态快照。
 */
public class Snapshot {
    private Map<String, Workload> workloads = new LinkedHashMap<>();
    private Map<String, Node> nodes = new LinkedHashMap<>();
    private List<Event> events = new ArrayList<>();
    private long tick;

    public Map<String, Workload> getWorkloads() { return workloads; }
    public void setWorkloads(Map<String, Workload> workloads) { this.workloads = workloads; }
    public Map<String, Node> getNodes() { return nodes; }
 public void setNodes(Map<String, Node> nodes) { this.nodes = nodes; }
    public List<Event> getEvents() { return events; }
    public void setEvents(List<Event> events) { this.events = events; }
    public long getTick() { return tick; }
    public void setTick(long tick) { this.tick = tick; }
}
