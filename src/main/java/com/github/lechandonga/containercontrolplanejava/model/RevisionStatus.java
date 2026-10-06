package com.github.lechandonga.containercontrolplanejava.model;

/**
 * 发布修订的最终状态。
 */
public enum RevisionStatus {
    /** 修订已产生，其声明的滚动/扩缩容仍在进行中。 */
    IN_PROGRESS,
    /** 该修订声明的期望状态已成功收敛。 */
    SUCCEEDED,
    /** 该修订的发布失败并已告停（可能伴随自动回退到旧版本）。 */
    FAILED,
    /** 一次显式回滚以该修订为目标：该修订所声明的版本正在/已经重新成为实际状态。 */
    ROLLED_BACK,
    /** 该修订发布未完成就被更新的人工提交取代，不再作为期望被追求。 */
    SUPERSEDED
}
