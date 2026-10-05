package com.github.lechandonga.containercontrolplanejava.model;

/**
 * 模拟节点的健康状态。节点失联（NOT_READY）后其上实例视为已丢失，需要在其他健康节点重建。
 */
public enum NodeStatus {
    READY,
    NOT_READY
}
