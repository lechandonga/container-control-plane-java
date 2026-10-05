package com.github.lechandonga.containercontrolplanejava.model;

/**
 * 模拟实例的运行状态。
 */
public enum InstanceState {
    /** 已调度到节点、容器进程启动中（尚未就绪，不计入可用数）。 */
    PENDING,
    /** 就绪并对外提供服务，计入可用数。 */
    RUNNING,
    /** 启动失败（模拟节点故障/失败注入），需要被回收重建。 */
    FAILED
}
