package com.github.lechandonga.containercontrolplanejava.model;

public enum InstanceState {
    /** 实例在健康节点上正常运行。 */
    RUNNING,
    /** 所属节点失联/被隔离，或节点上容器已消失，等待重建。 */
    LOST,
    /** 节点接受调度但启动失败，等待按预算重试。 */
    START_FAILED
}
