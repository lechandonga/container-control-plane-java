package com.github.lechandonga.containercontrolplanejava.model;

/**
 * 工作负载在控制面中的聚合阶段。
 */
public enum WorkloadPhase {
    /** 已提交期望状态，尚未完成首轮调谐。 */
    PENDING,
    /** 实际状态已收敛到当前期望状态。 */
    RUNNING,
    /** 滚动更新 / 修复动作进行中。 */
    PROGRESSING,
    /** 期望无法达成（容量不足、启动失败且无旧版本可回退等），控制面告停。 */
    FAILED,
    /** 正在删除，等待调谐循环清空实例。 */
    DELETING
}
