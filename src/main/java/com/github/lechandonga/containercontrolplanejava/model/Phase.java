package com.github.lechandonga.containercontrolplanejava.model;

/**
 * 工作负载（Deployment）在控制面中的生命周期相位。
 */
public enum Phase {
    /** 已提交期望状态，尚未完成首次调谐。 */
    PENDING,
    /** 实际状态已收敛到期望状态（副本数与版本均满足）。 */
    RUNNING,
    /** 正在进行滚动更新（替换版本）。 */
    UPDATING,
    /** 滚动更新失败并已告停：新版本不可用，流量仍由旧版本承载（或容量不足）。 */
    UPDATE_FAILED,
    /** 正在删除：终止全部实例后从存储移除。删除优先级最高。 */
    TERMINATING
}
