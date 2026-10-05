package com.github.lechandonga.containercontrolplanejava.model;

public enum NodeState {
    HEALTHY,
    /** 心跳超时，已被控制面隔离（fence），其上实例视为 LOST。 */
    UNREACHABLE
}
