package com.github.lechandonga.containercontrolplanejava.model;

import java.time.Instant;

/**
 * 节点在控制面登记的元数据。
 */
public class NodeInfo {
    private final String uid;
    private final String host;
    private final int capacity;
    private volatile NodeState state = NodeState.HEALTHY;
    private volatile Instant lastHeartbeat;

    public NodeInfo(String uid, String host, int capacity, Instant now) {
        this.uid = uid;
        this.host = host;
        this.capacity = capacity;
        this.lastHeartbeat = now;
    }

    public String getUid() { return uid; }
    public String getHost() { return host; }
    public int getCapacity() { return capacity; }
    public NodeState getState() { return state; }
    public void setState(NodeState state) { this.state = state; }
    public Instant getLastHeartbeat() { return lastHeartbeat; }
    public void setLastHeartbeat(Instant t) { this.lastHeartbeat = t; }
}
